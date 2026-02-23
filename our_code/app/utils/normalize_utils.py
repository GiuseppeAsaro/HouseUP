from datetime import datetime, timezone
from typing import Any, Optional


def normalize_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def normalize_required_text(value: Any, message: str) -> str:
    text = normalize_optional_text(value)
    if text is None:
        raise ValueError(message)
    return text


def normalize_bool_strict(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    raise ValueError(f"'{field_name}' must be a boolean.")


def normalize_positive_float(value: Any, message: str) -> float:
    if value is None or value == "":
        raise ValueError(message)
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(message) from exc
    if numeric <= 0:
        raise ValueError(message)
    return numeric


def normalize_int_range(value: Any, min_value: int, max_value: int, message: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(message) from exc
    if number < min_value or number > max_value:
        raise ValueError(message)
    return number


def normalize_datetime_string(raw: Any) -> str:
    if isinstance(raw, datetime):
        dt = raw
    elif isinstance(raw, (int, float)):
        dt = datetime.fromtimestamp(raw, tz=timezone.utc)
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            raise ValueError("Datetime is empty.")

        iso_candidate = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(iso_candidate)
        except Exception:
            dt = None

        if dt is None:
            patterns_with_time = [
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M",
                "%d/%m/%Y %H:%M",
                "%d-%m-%Y %H:%M",
            ]
            for fmt in patterns_with_time:
                try:
                    dt = datetime.strptime(text.replace("T", " "), fmt)
                    break
                except Exception:
                    continue
            if dt is None:
                patterns_date_only = [
                    "%Y-%m-%d",
                    "%d/%m/%Y",
                    "%d-%m-%Y",
                    "%m/%d/%Y",
                    "%m-%d-%Y",
                ]
                for fmt in patterns_date_only:
                    try:
                        dt = datetime.strptime(text, fmt)
                        break
                    except Exception:
                        continue
                if dt is not None:
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            if dt is None:
                raise ValueError("Invalid datetime format.")
    else:
        raise ValueError("Invalid datetime format.")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    dt_utc = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return dt_utc.isoformat(timespec="minutes").replace("+00:00", "Z")
