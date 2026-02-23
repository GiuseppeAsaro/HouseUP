from typing import Any, Dict, List

from app.models.house import House


def parse_house_list(value: Any) -> List[House]:
    if not isinstance(value, list):
        return []

    houses: List[House] = []
    seen_ids = set()
    for item in value:
        if isinstance(item, str):
            house = House.from_json({"_id": item})
        elif isinstance(item, dict):
            house = House.from_json(item)
        else:
            continue

        house_id = str(house.id).strip()
        if not house_id or house_id in seen_ids:
            continue

        seen_ids.add(house_id)
        houses.append(house)

    return houses


def _parse_user_snapshot(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}

    raw_id = value.get("id") or value.get("_id")
    if raw_id is None:
        return {}
    user_id = str(raw_id).strip()
    if not user_id:
        return {}

    snapshot: Dict[str, Any] = {"id": user_id}

    full_name = value.get("full_name")
    if full_name is None:
        full_name = value.get("name")
    if full_name is not None:
        full_name_str = str(full_name).strip()
        if full_name_str:
            snapshot["full_name"] = full_name_str

    for field in ("email", "phone"):
        raw_field = value.get(field)
        if raw_field is None:
            continue
        field_str = str(raw_field).strip()
        if field_str:
            snapshot[field] = field_str

    return snapshot


def parse_feedback_embed_list(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []

    feedbacks: List[Dict[str, Any]] = []
    seen_ids = set()
    for item in value:
        if not isinstance(item, dict):
            continue

        raw_feedback_id = item.get("id") or item.get("_id")
        if raw_feedback_id is None:
            continue

        feedback_id = str(raw_feedback_id).strip()
        if not feedback_id or feedback_id in seen_ids:
            continue
        seen_ids.add(feedback_id)

        embed: Dict[str, Any] = {"id": feedback_id}

        feedback_date = item.get("feedback_date")
        if feedback_date is not None:
            feedback_date_str = str(feedback_date).strip()
            if feedback_date_str:
                embed["feedback_date"] = feedback_date_str

        raw_rating = item.get("rating")
        if raw_rating is not None and raw_rating != "":
            try:
                embed["rating"] = int(raw_rating)
            except (TypeError, ValueError):
                pass

        comment = item.get("comment")
        if comment is not None:
            comment_str = str(comment).strip()
            if comment_str:
                embed["comment"] = comment_str

        call_datetime = item.get("call_datetime")
        if call_datetime is not None:
            call_datetime_str = str(call_datetime).strip()
            if call_datetime_str:
                embed["call_datetime"] = call_datetime_str

        seller_snapshot = _parse_user_snapshot(item.get("seller"))
        if seller_snapshot:
            embed["seller"] = seller_snapshot

        feedbacks.append(embed)

    return feedbacks


def parse_string_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []

    seen = set()
    values: List[str] = []
    for item in value:
        if item is None:
            continue

        item_str = str(item).strip()
        if not item_str or item_str in seen:
            continue

        seen.add(item_str)
        values.append(item_str)

    return values


def extract_embed_ids(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []

    seen = set()
    ids: List[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue

        raw_id = item.get("id") or item.get("_id")
        if raw_id is None:
            continue

        raw_id_str = str(raw_id).strip()
        if not raw_id_str or raw_id_str in seen:
            continue

        seen.add(raw_id_str)
        ids.append(raw_id_str)

    return ids


def exclude_overlapping_ids(ids: List[str], excluded_ids: List[str]) -> List[str]:
    excluded = {str(item).strip() for item in excluded_ids if str(item).strip()}
    filtered: List[str] = []
    seen = set()
    for item in ids:
        marker = str(item).strip()
        if not marker or marker in excluded or marker in seen:
            continue
        seen.add(marker)
        filtered.append(marker)
    return filtered
