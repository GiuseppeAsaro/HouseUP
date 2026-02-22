import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional

from redis.exceptions import RedisError

from app.core.database import redis_client
from app.models import Callslot, User
from app.utils.cache_utils import compute_ttl_seconds
from app.utils.callslots_utils import (
    build_user_callslot_key,
    datetime_to_tokens,
    delete_callslot_by_key,
    iter_user_callslots,
    iter_user_callslots_by_date,
    normalize_date_token,
    normalize_optional_text,
    normalize_time_token,
    read_callslot_by_key,
    write_callslot_by_key,
    write_callslot_pair_by_keys,
)
from app.utils.normalize_utils import normalize_datetime_string
from app.utils.permissions_utils import (
    can_view_callslot,
    ensure_can_create_callslot,
    ensure_can_modify_callslot,
)

_TTL_BUFFER_AFTER_CALLSLOT = timedelta(hours=6)
_TTL_MIN_SECONDS = 60 * 60

_ALLOWED_CALLSLOT_STATUSES = {"requested", "called", "cancelled"}
CALLSLOT_PERSISTENCE_ERROR_MSG = "Callslot system is temporarily unavailable."


@dataclass
class _CallslotSelector:
    actor_id: str
    other_id: str
    date_token: str
    time_token: str
    actor_key: str
    other_key: str


def _build_selector(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
) -> _CallslotSelector:
    actor_id = str(current_user.id)
    other_id = normalize_optional_text(with_user_id)
    if other_id is None:
        raise ValueError("'with_user_id' is required.")

    date_token = normalize_date_token(date)
    time_token = normalize_time_token(time)
    actor_key = build_user_callslot_key(actor_id, date_token, time_token, other_id)
    other_key = build_user_callslot_key(other_id, date_token, time_token, actor_id)
    return _CallslotSelector(
        actor_id=actor_id,
        other_id=other_id,
        date_token=date_token,
        time_token=time_token,
        actor_key=actor_key,
        other_key=other_key,
    )


def _read_callslot_for_selector(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
) -> tuple[Optional[Callslot], _CallslotSelector]:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    selector = _build_selector(current_user, date, time, with_user_id)
    payload = read_callslot_by_key(redis_client, selector.actor_key)
    if payload is None:
        return None, selector

    callslot = Callslot.from_json(payload)
    if not can_view_callslot(current_user, callslot.to_json()):
        raise PermissionError("Not allowed.")
    return callslot, selector


def _write_pair_with_nx(
    first_key: str,
    second_key: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    *,
    conflict_message: str,
) -> None:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    first_created = write_callslot_by_key(
        redis_client,
        first_key,
        payload,
        ttl_seconds,
        nx=True,
    )
    if not first_created:
        raise ValueError(conflict_message)

    second_created = write_callslot_by_key(
        redis_client,
        second_key,
        payload,
        ttl_seconds,
        nx=True,
    )
    if second_created:
        return

    delete_callslot_by_key(redis_client, first_key)
    raise ValueError(conflict_message)


def create_callslot(current_user: User, payload: Dict[str, Any]) -> Callslot:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    ensure_can_create_callslot(current_user)

    role = str(current_user.role).strip().lower()
    datetime_raw = payload.get("datetime")
    if datetime_raw is None:
        raise ValueError("'datetime' is required.")

    call_datetime = normalize_datetime_string(datetime_raw)
    date_token, time_token = datetime_to_tokens(call_datetime)
    phone_from_payload = normalize_optional_text(payload.get("phone"))
    seller_id_from_payload = normalize_optional_text(payload.get("seller_id"))
    buyer_id_from_payload = normalize_optional_text(payload.get("buyer_id"))

    buyer_name = normalize_optional_text(payload.get("buyer_name"))
    buyer_email = normalize_optional_text(payload.get("buyer_email"))
    seller_name = normalize_optional_text(payload.get("seller_name"))
    seller_email = normalize_optional_text(payload.get("seller_email"))

    if role == "buyer":
        buyer_id = str(current_user.id)
        seller_id = seller_id_from_payload
        if seller_id is None:
            raise ValueError("'seller_id' is required for buyer.")

        phone = phone_from_payload or normalize_optional_text(current_user.phone)
        if phone is None:
            raise ValueError("Phone is required.")

        buyer_name = buyer_name or normalize_optional_text(current_user.full_name)
        buyer_email = buyer_email or normalize_optional_text(current_user.email)

    elif role == "seller":
        seller_id = str(current_user.id)
        buyer_id = buyer_id_from_payload
        if buyer_id is None:
            raise ValueError("'buyer_id' is required for seller.")

        if phone_from_payload is None:
            raise ValueError("'phone' is required for seller.")

        phone = phone_from_payload
        seller_name = seller_name or normalize_optional_text(current_user.full_name)
        seller_email = seller_email or normalize_optional_text(current_user.email)

    elif role == "admin":
        buyer_id = buyer_id_from_payload
        seller_id = seller_id_from_payload
        if buyer_id is None:
            raise ValueError("'buyer_id' is required for admin.")
        if seller_id is None:
            raise ValueError("'seller_id' is required for admin.")
        if phone_from_payload is None:
            raise ValueError("'phone' is required for admin.")
        phone = phone_from_payload

    else:
        raise PermissionError("Not allowed to create callslots.")

    if buyer_id == seller_id:
        raise ValueError("'buyer_id' and 'seller_id' must be different.")

    ttl_seconds = compute_ttl_seconds(
        call_datetime,
        ttl_buffer_after=int(_TTL_BUFFER_AFTER_CALLSLOT.total_seconds()),
        ttl_min_seconds=_TTL_MIN_SECONDS,
    )

    callslot = Callslot(
        callslot_id=uuid.uuid4().hex,
        call_datetime=call_datetime,
        status="requested",
        phone=phone,
        buyer_id=buyer_id,
        buyer_name=buyer_name,
        buyer_email=buyer_email,
        seller_id=seller_id,
        seller_name=seller_name,
        seller_email=seller_email,
    )

    buyer_key = build_user_callslot_key(buyer_id, date_token, time_token, seller_id)
    seller_key = build_user_callslot_key(seller_id, date_token, time_token, buyer_id)

    try:
        _write_pair_with_nx(
            buyer_key,
            seller_key,
            callslot.to_json(),
            ttl_seconds,
            conflict_message="Callslot slot already booked.",
        )
    except RedisError:
        raise RuntimeError(CALLSLOT_PERSISTENCE_ERROR_MSG)

    return callslot


def list_callslots(current_user: User, date: Optional[str] = None) -> List[Callslot]:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    user_id = str(current_user.id)
    results: List[Callslot] = []

    try:
        if date is None:
            payload_iter = iter_user_callslots(redis_client, user_id)
        else:
            payload_iter = iter_user_callslots_by_date(redis_client, user_id, normalize_date_token(date))

        for payload in payload_iter:
            callslot = Callslot.from_json(payload)
            if not can_view_callslot(current_user, callslot.to_json()):
                continue
            results.append(callslot)
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError("Unable to read callslots from Redis.") from exc

    return sorted(results, key=lambda c: c.call_datetime)


def update_callslot(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
    new_datetime: Optional[str] = None,
    phone: Optional[str] = None,
) -> Callslot:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")
    if new_datetime is None and phone is None:
        raise ValueError("At least one field between 'new_datetime' and 'phone' is required.")

    callslot, selector = _read_callslot_for_selector(
        current_user=current_user,
        date=date,
        time=time,
        with_user_id=with_user_id,
    )
    if callslot is None:
        raise KeyError("Callslot not found.")

    ensure_can_modify_callslot(current_user, callslot.to_json())

    if new_datetime is not None:
        callslot.call_datetime = normalize_datetime_string(new_datetime)
        new_date_token, new_time_token = datetime_to_tokens(callslot.call_datetime)
    else:
        new_date_token = selector.date_token
        new_time_token = selector.time_token

    if phone is not None:
        normalized_phone = normalize_optional_text(phone)
        if normalized_phone is None:
            raise ValueError("'phone' cannot be empty.")
        callslot.phone = normalized_phone

    ttl_seconds = compute_ttl_seconds(
        callslot.call_datetime,
        ttl_buffer_after=int(_TTL_BUFFER_AFTER_CALLSLOT.total_seconds()),
        ttl_min_seconds=_TTL_MIN_SECONDS,
    )

    updated_payload = callslot.to_json()
    new_actor_key = build_user_callslot_key(
        selector.actor_id,
        new_date_token,
        new_time_token,
        selector.other_id,
    )
    new_other_key = build_user_callslot_key(
        selector.other_id,
        new_date_token,
        new_time_token,
        selector.actor_id,
    )

    try:
        if new_actor_key == selector.actor_key and new_other_key == selector.other_key:
            wrote = write_callslot_pair_by_keys(
                redis_client,
                selector.actor_key,
                selector.other_key,
                updated_payload,
                ttl_seconds,
                require_existing=True,
            )
            if not wrote:
                raise KeyError("Callslot not found.")
            return callslot

        _write_pair_with_nx(
            new_actor_key,
            new_other_key,
            updated_payload,
            ttl_seconds,
            conflict_message="Callslot slot already booked.",
        )
        delete_callslot_by_key(redis_client, selector.actor_key)
        delete_callslot_by_key(redis_client, selector.other_key)
    except ValueError:
        raise
    except KeyError:
        raise
    except RedisError:
        raise RuntimeError(CALLSLOT_PERSISTENCE_ERROR_MSG)
    except Exception as exc:
        raise RuntimeError("Unable to update callslot in Redis.") from exc

    return callslot


def update_callslot_status(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
    status: str,
) -> Callslot:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    callslot, selector = _read_callslot_for_selector(
        current_user=current_user,
        date=date,
        time=time,
        with_user_id=with_user_id,
    )
    if callslot is None:
        raise KeyError("Callslot not found.")

    ensure_can_modify_callslot(current_user, callslot.to_json())

    safe_status = normalize_optional_text(status)
    if safe_status is None:
        raise ValueError("'status' is required.")
    safe_status = safe_status.lower()
    if safe_status not in _ALLOWED_CALLSLOT_STATUSES:
        raise ValueError("Invalid callslot status transition.")
    if callslot.status != "requested" and safe_status != callslot.status:
        raise ValueError("Only requested callslots can change status.")

    callslot.status = safe_status
    ttl_seconds = compute_ttl_seconds(
        callslot.call_datetime,
        ttl_buffer_after=int(_TTL_BUFFER_AFTER_CALLSLOT.total_seconds()),
        ttl_min_seconds=_TTL_MIN_SECONDS,
    )

    try:
        wrote = write_callslot_pair_by_keys(
            redis_client,
            selector.actor_key,
            selector.other_key,
            callslot.to_json(),
            ttl_seconds,
            require_existing=True,
        )
        if not wrote:
            raise KeyError("Callslot not found.")
    except KeyError:
        raise
    except RedisError:
        raise RuntimeError(CALLSLOT_PERSISTENCE_ERROR_MSG)
    except Exception as exc:
        raise RuntimeError("Unable to update callslot in Redis.") from exc

    return callslot


def delete_callslot(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
) -> bool:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    selector = _build_selector(
        current_user=current_user,
        date=date,
        time=time,
        with_user_id=with_user_id,
    )

    try:
        actor_payload = read_callslot_by_key(redis_client, selector.actor_key)
        other_payload = read_callslot_by_key(redis_client, selector.other_key)
    except Exception as exc:
        raise RuntimeError("Unable to read callslot from Redis.") from exc

    existing_payload = actor_payload or other_payload
    if existing_payload is not None:
        callslot = Callslot.from_json(existing_payload)
        if not can_view_callslot(current_user, callslot.to_json()):
            raise PermissionError("Not allowed.")
        ensure_can_modify_callslot(current_user, callslot.to_json())

    try:
        deleted_actor = delete_callslot_by_key(redis_client, selector.actor_key)
        deleted_other = delete_callslot_by_key(redis_client, selector.other_key)
    except Exception as exc:
        raise RuntimeError("Unable to delete callslot from Redis.") from exc

    return bool(deleted_actor or deleted_other)
