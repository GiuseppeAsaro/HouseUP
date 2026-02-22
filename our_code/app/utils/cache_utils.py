import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


def cache_get(redis_client, key: str) -> Optional[List[Dict[str, Any]]]:
    if redis_client is None:
        return None
    try:
        raw = redis_client.get(key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


def cache_set(redis_client, key: str, value: List[Dict[str, Any]], ttl: int) -> None:
    if redis_client is None:
        return
    try:
        redis_client.setex(key, ttl, json.dumps(value))
    except Exception:
        pass


def cache_note(
    historical_hit: bool,
    current_hit: bool,
    *,
    node_role: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    suffix = ""
    if node_role and node_role.lower() != "unknown":
        suffix = f" ({node_role})"

    if historical_hit and current_hit:
        return {"note": f"full served by redis{suffix}"}
    if historical_hit and not current_hit:
        return {"note": f"partially served by redis{suffix}"}
    return None


def compute_ttl_seconds(
    normalized_datetime: str,
    *,
    ttl_buffer_after: int,
    ttl_min_seconds: int,
) -> int:
    try:
        dt = datetime.fromisoformat(normalized_datetime.replace("Z", "+00:00"))
    except Exception as exc:
        raise ValueError("Invalid booking datetime.") from exc

    now = datetime.now(timezone.utc)
    expires_at = dt + timedelta(seconds=ttl_buffer_after)
    ttl = int((expires_at - now).total_seconds())
    if ttl <= 0:
        raise ValueError("Booking datetime is in the past.")
    return max(ttl, ttl_min_seconds)
