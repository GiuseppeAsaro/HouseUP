import html
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from bson import ObjectId

from app.core.database import mongo_db


_USERS_COLLECTION = mongo_db["users"]
_HOUSES_COLLECTION = mongo_db["houses"]
_BOOKING_RESULTS_COLLECTION = mongo_db["booking_results"]
_FEEDBACKS_COLLECTION = mongo_db["feedbacks"]

_USER_LIST_LIMITS = {
    "for_sale_houses": 2,
    "sold_houses": 2,
    "bought_houses": 2,
    "booking_results": 2,
    "feedbacks": 2,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


_JSON_KEY_RE = re.compile(r"(&quot;[^&]*?&quot;)(\s*:)")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _trim_user_document(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(doc, dict):
        return None

    trimmed = dict(doc)
    trimmed.pop("password_hash", None)
    for field_name, limit in _USER_LIST_LIMITS.items():
        raw = trimmed.get(field_name)
        if isinstance(raw, list):
            sliced = raw[:limit]
            if sliced:
                trimmed[field_name] = sliced
            else:
                trimmed.pop(field_name, None)
    # Keep snapshot clean: if a field is None, hide it in the example output.
    for key in list(trimmed.keys()):
        if trimmed[key] is None:
            trimmed.pop(key, None)
    return trimmed


def _find_house_for_booking(booking_doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(booking_doc, dict):
        return None

    house_doc = booking_doc.get("house")
    if not isinstance(house_doc, dict):
        return None

    house_id = str(house_doc.get("id") or "").strip()
    if not house_id:
        return None

    try:
        return _HOUSES_COLLECTION.find_one({"_id": ObjectId(house_id)})
    except Exception:
        return _HOUSES_COLLECTION.find_one({"_id": house_id})


def get_examples_snapshot() -> Dict[str, Any]:
    seller_with_for_sale = _USERS_COLLECTION.find_one(
        {
            "role": "seller",
            "for_sale_houses.0": {"$exists": True},
        },
        sort=[("_id", 1)],
    )

    seller_with_sold = _USERS_COLLECTION.find_one(
        {
            "role": "seller",
            "sold_houses.0": {"$exists": True},
        },
        sort=[("_id", 1)],
    )

    seller_doc = seller_with_for_sale or seller_with_sold or _USERS_COLLECTION.find_one(
        {"role": "seller"},
        sort=[("_id", 1)],
    )

    buyer_with_bought = _USERS_COLLECTION.find_one(
        {
            "role": "buyer",
            "bought_houses.0": {"$exists": True},
        },
        sort=[("_id", 1)],
    )

    buyer_with_feedback = _USERS_COLLECTION.find_one(
        {
            "role": "buyer",
            "feedbacks.0": {"$exists": True},
        },
        sort=[("_id", 1)],
    )

    feedback_doc: Optional[Dict[str, Any]] = None
    if isinstance(buyer_with_feedback, dict):
        feedback_doc = _FEEDBACKS_COLLECTION.find_one(
            {"user.id": str(buyer_with_feedback.get("_id"))},
            sort=[("feedback_date", 1), ("_id", 1)],
        )
    if feedback_doc is None:
        feedback_doc = _FEEDBACKS_COLLECTION.find_one({}, sort=[("feedback_date", 1), ("_id", 1)])

    booking_result_bought_true_doc = _BOOKING_RESULTS_COLLECTION.find_one(
        {"house_bought": True},
        sort=[("_id", 1)],
    )
    booking_result_bought_false_doc = _BOOKING_RESULTS_COLLECTION.find_one(
        {"house_bought": False},
        sort=[("_id", 1)],
    )
    booking_result_doc = (
        booking_result_bought_true_doc
        or booking_result_bought_false_doc
        or _BOOKING_RESULTS_COLLECTION.find_one({}, sort=[("_id", 1)])
    )

    house_doc = _find_house_for_booking(booking_result_bought_true_doc)
    if house_doc is None:
        house_doc = _find_house_for_booking(booking_result_bought_false_doc)
    if house_doc is None:
        house_doc = _find_house_for_booking(booking_result_doc)
    if house_doc is None:
        house_doc = _HOUSES_COLLECTION.find_one({}, sort=[("_id", 1)])

    house_for_sale_doc = _HOUSES_COLLECTION.find_one(
        {"is_sold": {"$ne": True}},
        sort=[("_id", 1)],
    )
    house_sold_doc = _HOUSES_COLLECTION.find_one({"is_sold": True}, sort=[("_id", 1)])

    snapshot = {
        "generated_at": _utc_now_iso(),
        "seller_example": _trim_user_document(seller_doc),
        "seller_with_for_sale_example": _trim_user_document(seller_with_for_sale),
        "seller_with_sold_example": _trim_user_document(seller_with_sold),
        "buyer_with_bought_example": _trim_user_document(buyer_with_bought),
        "buyer_with_feedback_example": _trim_user_document(buyer_with_feedback),
        "feedback_example": feedback_doc,
        "booking_result_bought_true_example": booking_result_bought_true_doc,
        "booking_result_bought_false_example": booking_result_bought_false_doc,
        "booking_result_example": booking_result_doc,
        "house_for_sale_example": house_for_sale_doc,
        "house_sold_example": house_sold_doc,
        "house_example": house_doc,
    }
    return _json_safe(snapshot)


def _section_html(title: str, payload: Any) -> str:
    if payload is None:
        pretty = html.escape("No document found.")
        payload_class = "empty"
    else:
        pretty = _pretty_json_html(payload)
        payload_class = ""
    return (
        f"<section class='card'>"
        f"<h2>{html.escape(title)}</h2>"
        f"<pre class='{payload_class}'>{pretty}</pre>"
        f"</section>"
    )


def _pretty_json_html(payload: Any) -> str:
    pretty = json.dumps(payload, indent=2, ensure_ascii=False)
    lines = pretty.splitlines()
    out_lines = []
    for line in lines:
        escaped = html.escape(line)
        escaped = _JSON_KEY_RE.sub(
            r"<strong class='json-key'>\1</strong><strong class='json-sep'>\2</strong>",
            escaped,
        )
        escaped = (
            escaped.replace("{", "<strong class='json-sep'>{</strong>")
            .replace("}", "<strong class='json-sep'>}</strong>")
            .replace("[", "<strong class='json-sep'>[</strong>")
            .replace("]", "<strong class='json-sep'>]</strong>")
        )
        out_lines.append(escaped)
    return "\n".join(out_lines)


def _status_pill(label: str, is_found: bool) -> str:
    status_class = "ok" if is_found else "missing"
    status_text = "Found" if is_found else "Missing"
    return (
        f"<div class='pill {status_class}'>"
        f"<span class='pill-label'>{html.escape(label)}</span>"
        f"<span class='pill-value'>{status_text}</span>"
        f"</div>"
    )


def render_examples_html(snapshot: Dict[str, Any]) -> str:
    sections = [
        ("Seller Example", snapshot.get("seller_example")),
        ("Seller With For-Sale Houses", snapshot.get("seller_with_for_sale_example")),
        ("Seller With Sold Houses", snapshot.get("seller_with_sold_example")),
        ("Buyer With Bought Houses", snapshot.get("buyer_with_bought_example")),
        ("Buyer With Feedback", snapshot.get("buyer_with_feedback_example")),
        ("Feedback Document", snapshot.get("feedback_example")),
        (
            "Booking Result Document (house_bought=true)",
            snapshot.get("booking_result_bought_true_example"),
        ),
        (
            "Booking Result Document (house_bought=false)",
            snapshot.get("booking_result_bought_false_example"),
        ),
        ("House Document (for-sale)", snapshot.get("house_for_sale_example")),
        ("House Document (sold)", snapshot.get("house_sold_example")),
        ("House Document", snapshot.get("house_example")),
    ]
    body = "".join(_section_html(title, payload) for title, payload in sections)
    generated_at = str(snapshot.get("generated_at") or "")
    summary = "".join(
        [
            _status_pill("Seller", snapshot.get("seller_example") is not None),
            _status_pill(
                "Seller (For-sale)",
                snapshot.get("seller_with_for_sale_example") is not None,
            ),
            _status_pill(
                "Seller (Sold)",
                snapshot.get("seller_with_sold_example") is not None,
            ),
            _status_pill("Buyer (Bought)", snapshot.get("buyer_with_bought_example") is not None),
            _status_pill("Buyer (Feedback)", snapshot.get("buyer_with_feedback_example") is not None),
            _status_pill(
                "Booking Bought=True",
                snapshot.get("booking_result_bought_true_example") is not None,
            ),
            _status_pill(
                "Booking Bought=False",
                snapshot.get("booking_result_bought_false_example") is not None,
            ),
            _status_pill("House (For-sale)", snapshot.get("house_for_sale_example") is not None),
            _status_pill("House (Sold)", snapshot.get("house_sold_example") is not None),
            _status_pill("House", snapshot.get("house_example") is not None),
        ]
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>HouseUp Mongo Examples</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    :root {{
      --blue-1: #6dd5fa;
      --blue-2: #2980ff;
      --ink: #f4f9ff;
      --ink-muted: rgba(244, 249, 255, 0.84);
      --glass: rgba(255, 255, 255, 0.16);
      --glass-strong: rgba(255, 255, 255, 0.22);
      --line: rgba(255, 255, 255, 0.22);
      --top-glass: rgba(7, 33, 73, 0.28);
      --top-glass-strong: rgba(7, 33, 73, 0.38);
      --top-line: rgba(255, 255, 255, 0.38);
      --ok: #4ae39a;
      --warn: #ffd166;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      background: linear-gradient(135deg, var(--blue-1), var(--blue-2));
      color: var(--ink);
      font-family: "Poppins", "Segoe UI", sans-serif;
    }}
    .wrap {{
      max-width: 1240px;
      margin: 0 auto;
      padding: 24px 16px 46px;
    }}
    .header {{
      text-align: center;
      margin-bottom: 14px;
      color: #fff;
      background: var(--top-glass);
      border: 1px solid var(--top-line);
      border-radius: 18px;
      padding: 14px 12px;
      backdrop-filter: blur(10px);
    }}
    h1 {{
      margin: 0;
      font-size: clamp(30px, 4vw, 40px);
      line-height: 1.08;
      letter-spacing: 0.3px;
      font-weight: 700;
      text-shadow: 0 6px 18px rgba(0, 0, 0, 0.28);
    }}
    .meta {{
      margin: 8px 0 0;
      color: rgba(255, 255, 255, 0.96);
      font-size: 14px;
      font-weight: 500;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      justify-content: center;
      gap: 10px;
      margin: 10px 0 16px;
    }}
    .btn {{
      display: inline-block;
      padding: 10px 14px;
      border: 1px solid var(--top-line);
      border-radius: 12px;
      text-decoration: none;
      color: #fff;
      font-weight: 600;
      font-size: 14px;
      background: var(--top-glass);
      backdrop-filter: blur(10px);
      box-shadow: 0 6px 16px rgba(0, 0, 0, 0.2);
      transition: transform .15s ease, background .15s ease, border-color .15s ease;
    }}
    .btn:hover {{
      transform: translateY(-1px);
      background: var(--top-glass-strong);
      border-color: rgba(255, 255, 255, 0.52);
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 10px;
      margin-bottom: 16px;
    }}
    .pill {{
      border-radius: 14px;
      padding: 11px 12px;
      background: var(--top-glass);
      border: 1px solid var(--top-line);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      backdrop-filter: blur(10px);
    }}
    .pill-label {{
      font-size: 12px;
      font-weight: 600;
      color: rgba(255, 255, 255, 0.98);
      opacity: 1;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .pill-value {{
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .2px;
      padding: 3px 8px;
      border-radius: 999px;
      background: rgba(255, 255, 255, .18);
    }}
    .pill.ok .pill-value {{
      color: #093d27;
      background: var(--ok);
    }}
    .pill.missing .pill-value {{
      color: #4a3200;
      background: var(--warn);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      gap: 16px;
    }}
    .card {{
      background: var(--glass);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      color: #fff;
      box-shadow: 0 10px 26px rgba(0, 0, 0, 0.22);
      backdrop-filter: blur(12px);
      animation: rise .35s ease both;
    }}
    .card:nth-child(2) {{ animation-delay: .04s; }}
    .card:nth-child(3) {{ animation-delay: .08s; }}
    .card:nth-child(4) {{ animation-delay: .12s; }}
    .card:nth-child(5) {{ animation-delay: .16s; }}
    .card:nth-child(6) {{ animation-delay: .20s; }}
    .card:nth-child(7) {{ animation-delay: .24s; }}
    .card h2 {{
      margin: 0 0 10px;
      font-size: 16px;
      line-height: 1.3;
      font-weight: 600;
      border-bottom: 1px solid rgba(255, 255, 255, 0.24);
      padding-bottom: 9px;
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "JetBrains Mono", "SFMono-Regular", Menlo, monospace;
      font-size: 12px;
      line-height: 1.5;
      color: #ecf5ff;
      background: rgba(7, 27, 63, 0.24);
      border: 1px solid rgba(255, 255, 255, 0.15);
      border-radius: 12px;
      padding: 10px;
      max-height: none;
      overflow: visible;
    }}
    .json-key {{
      font-weight: 700;
      color: #ffffff;
    }}
    .json-sep {{
      font-weight: 700;
      color: #cfe6ff;
    }}
    pre.empty {{
      color: rgba(236, 245, 255, 0.82);
      font-style: italic;
    }}
    @keyframes rise {{
      from {{ opacity: 0; transform: translateY(8px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    @media (max-width: 640px) {{
      .wrap {{
        padding: 18px 12px 24px;
      }}
      .grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <h1>HouseUp Mongo Examples</h1>
      <p class="meta">Live snapshot from MongoDB. Generated at: {html.escape(generated_at)}</p>
    </div>
    <div class="toolbar">
      <a class="btn" href="/examples/live">Refresh</a>
      <a class="btn" href="/api/v1/examples/snapshot">JSON snapshot</a>
    </div>
    <div class="summary">{summary}</div>
    <div class="grid">{body}</div>
  </div>
</body>
</html>
"""
