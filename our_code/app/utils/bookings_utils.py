import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

from app.utils.normalize_utils import normalize_optional_text


def normalize_date_token(raw_date: Any) -> str:
    text = normalize_optional_text(raw_date)
    if text is None:
        raise ValueError("'date' is required.")

    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("'date' must be in YYYY-MM-DD format.") from exc
    return parsed.strftime("%Y%m%d")


def normalize_time_token(raw_time: Any) -> str:
    text = normalize_optional_text(raw_time)
    if text is None:
        raise ValueError("'time' is required.")

    try:
        parsed = datetime.strptime(text, "%H:%M")
    except ValueError as exc:
        raise ValueError("'time' must be in HH:MM format.") from exc
    return parsed.strftime("%H%M")


def datetime_to_tokens(normalized_datetime: str) -> tuple[str, str]:
    text = normalize_optional_text(normalized_datetime)
    if text is None:
        raise ValueError("Invalid datetime format.")

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid datetime format.") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed_utc = parsed.astimezone(timezone.utc)
    return parsed_utc.strftime("%Y%m%d"), parsed_utc.strftime("%H%M")


def decode_json(raw_value: Any) -> Dict[str, Any]:
    text = raw_value
    if isinstance(text, (bytes, bytearray)):
        text = text.decode("utf-8")
    return json.loads(text)


def build_user_booking_key(
    user_id: str,
    date_token: str,
    time_token: str,
    with_user_id: str,
    house_id: str,
) -> str:
    return (
        f"booking:user:{user_id}:date:{date_token}:time:{time_token}:"
        f"with_user:{with_user_id}:house:{house_id}"
    )


def build_user_booking_day_pattern(user_id: str, date_token: str) -> str:
    return f"booking:user:{user_id}:date:{date_token}:*"


def build_user_booking_pattern(user_id: str) -> str:
    return f"booking:user:{user_id}:date:*"


def write_booking_by_key(
    redis_client: Any,
    key: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    *,
    nx: bool = False,
) -> bool:
    encoded = json.dumps(payload)
    if nx:
        return bool(redis_client.set(key, encoded, ex=ttl_seconds, nx=True))
    redis_client.set(key, encoded, ex=ttl_seconds)
    return True


def read_booking_by_key(
    redis_client: Any,
    key: str,
) -> Optional[Dict[str, Any]]:
    raw_value = redis_client.get(key)
    if raw_value is None:
        return None
    return decode_json(raw_value)


def iter_user_bookings_by_date(
    redis_client: Any,
    user_id: str,
    date_token: str,
) -> Iterator[Dict[str, Any]]:
    pattern = build_user_booking_day_pattern(user_id, date_token)

    for key in redis_client.scan_iter(match=pattern):
        raw_value = redis_client.get(key)
        if raw_value is None:
            continue
        yield decode_json(raw_value)


def iter_user_bookings(
    redis_client: Any,
    user_id: str,
) -> Iterator[Dict[str, Any]]:
    pattern = build_user_booking_pattern(user_id)

    for key in redis_client.scan_iter(match=pattern):
        raw_value = redis_client.get(key)
        if raw_value is None:
            continue
        yield decode_json(raw_value)


def delete_booking_by_key(
    redis_client: Any,
    key: str,
) -> int:
    return int(redis_client.delete(key))
