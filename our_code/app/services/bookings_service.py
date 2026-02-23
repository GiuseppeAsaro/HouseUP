import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional

from redis.exceptions import RedisError

from app.core.database import redis_client
from app.models import Booking, User
from app.utils.bookings_utils import (
    build_user_booking_key,
    datetime_to_tokens,
    delete_booking_by_key,
    iter_user_bookings,
    iter_user_bookings_by_date,
    normalize_date_token,
    normalize_optional_text,
    normalize_time_token,
    read_booking_by_key,
    write_booking_by_key,
)
from app.utils.cache_utils import compute_ttl_seconds
from app.utils.normalize_utils import normalize_datetime_string
from app.utils.permissions_utils import (
    can_view_booking,
    ensure_can_create_booking,
    ensure_can_modify_booking,
)


_TTL_BUFFER_AFTER_BOOKING = timedelta(hours=6)
_TTL_MIN_SECONDS = 60 * 60
BOOKING_PERSISTENCE_ERROR_MSG = "Booking system is temporarily unavailable."


@dataclass
class _BookingSelector:
    actor_id: str
    other_id: str
    house_id: str
    date_token: str
    time_token: str
    actor_key: str
    other_key: str


def _normalize_house_price(raw_value: Any) -> float:
    if raw_value is None:
        raise ValueError("'house_price' is required.")
    try:
        return float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("'house_price' must be a valid number.") from exc


def _build_selector(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
    house_id: str,
) -> _BookingSelector:
    actor_id = str(current_user.id)
    other_id = normalize_optional_text(with_user_id)
    if other_id is None:
        raise ValueError("'with_user_id' is required.")

    safe_house_id = normalize_optional_text(house_id)
    if safe_house_id is None:
        raise ValueError("'house_id' is required.")

    date_token = normalize_date_token(date)
    time_token = normalize_time_token(time)
    actor_key = build_user_booking_key(actor_id, date_token, time_token, other_id, safe_house_id)
    other_key = build_user_booking_key(other_id, date_token, time_token, actor_id, safe_house_id)
    return _BookingSelector(
        actor_id=actor_id,
        other_id=other_id,
        house_id=safe_house_id,
        date_token=date_token,
        time_token=time_token,
        actor_key=actor_key,
        other_key=other_key,
    )


def _read_booking_for_selector(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
    house_id: str,
) -> tuple[Optional[Booking], _BookingSelector]:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    selector = _build_selector(current_user, date, time, with_user_id, house_id)
    payload = read_booking_by_key(redis_client, selector.actor_key)
    if payload is None:
        return None, selector

    booking_model = Booking.from_json(payload)
    if not can_view_booking(current_user, booking_model.to_json()):
        raise PermissionError("Not allowed to view this booking.")
    return booking_model, selector


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

    first_created = write_booking_by_key(
        redis_client,
        first_key,
        payload,
        ttl_seconds,
        nx=True,
    )
    if not first_created:
        raise ValueError(conflict_message)

    second_created = write_booking_by_key(
        redis_client,
        second_key,
        payload,
        ttl_seconds,
        nx=True,
    )
    if second_created:
        return

    delete_booking_by_key(redis_client, first_key)
    raise ValueError(conflict_message)


def create_booking(current_user: User, payload: Dict[str, Any]) -> Booking:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    ensure_can_create_booking(current_user)
    role = str(current_user.role).strip().lower()

    datetime_raw = payload.get("datetime")
    if datetime_raw is None:
        raise ValueError("'datetime' is required.")
    normalized_datetime = normalize_datetime_string(datetime_raw)
    date_token, time_token = datetime_to_tokens(normalized_datetime)

    house_id = normalize_optional_text(payload.get("house_id"))
    if house_id is None:
        raise ValueError("'house_id' is required.")

    house_city = normalize_optional_text(payload.get("house_city"))
    if house_city is None:
        raise ValueError("'house_city' is required.")

    house_price = _normalize_house_price(payload.get("house_price"))

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

        buyer_name = buyer_name or normalize_optional_text(current_user.full_name)
        buyer_email = buyer_email or normalize_optional_text(current_user.email)

    elif role == "seller":
        seller_id = str(current_user.id)
        buyer_id = buyer_id_from_payload
        if buyer_id is None:
            raise ValueError("'buyer_id' is required for seller.")

        seller_name = seller_name or normalize_optional_text(current_user.full_name)
        seller_email = seller_email or normalize_optional_text(current_user.email)

    elif role == "admin":
        buyer_id = buyer_id_from_payload
        seller_id = seller_id_from_payload

        if buyer_id is None:
            raise ValueError("'buyer_id' is required for admin.")
        if seller_id is None:
            raise ValueError("'seller_id' is required for admin.")
    else:
        raise PermissionError("Only buyers, sellers, and admins can create bookings.")

    if buyer_id == seller_id:
        raise ValueError("'buyer_id' and 'seller_id' must be different.")

    ttl_seconds = compute_ttl_seconds(
        normalized_datetime,
        ttl_buffer_after=int(_TTL_BUFFER_AFTER_BOOKING.total_seconds()),
        ttl_min_seconds=_TTL_MIN_SECONDS,
    )

    booking = Booking(
        booking_id=uuid.uuid4().hex,
        booking_datetime=normalized_datetime,
        house_id=house_id,
        house_city=house_city,
        house_price=house_price,
        seller_id=seller_id,
        seller_name=seller_name,
        seller_email=seller_email,
        buyer_id=buyer_id,
        buyer_name=buyer_name,
        buyer_email=buyer_email,
    )

    buyer_key = build_user_booking_key(buyer_id, date_token, time_token, seller_id, house_id)
    seller_key = build_user_booking_key(seller_id, date_token, time_token, buyer_id, house_id)

    try:
        _write_pair_with_nx(
            buyer_key,
            seller_key,
            booking.to_json(),
            ttl_seconds,
            conflict_message="Booking slot already booked.",
        )
    except RedisError:
        raise RuntimeError(BOOKING_PERSISTENCE_ERROR_MSG)

    return booking


def list_bookings(current_user: User, date: Optional[str] = None) -> List[Booking]:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    user_id = str(current_user.id)
    results: List[Booking] = []

    try:
        if date is None:
            payload_iter = iter_user_bookings(redis_client, user_id)
        else:
            payload_iter = iter_user_bookings_by_date(redis_client, user_id, normalize_date_token(date))

        for payload in payload_iter:
            booking_model = Booking.from_json(payload)
            if not can_view_booking(current_user, booking_model.to_json()):
                continue
            results.append(booking_model)
    except Exception as exc:
        raise RuntimeError("Unable to read bookings from Redis.") from exc

    return sorted(results, key=lambda item: (item.booking_datetime, item.booking_id))


def update_booking(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
    house_id: str,
    new_datetime: str,
) -> Booking:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")
    if not new_datetime:
        raise ValueError("New datetime is required.")

    booking_model, selector = _read_booking_for_selector(
        current_user=current_user,
        date=date,
        time=time,
        with_user_id=with_user_id,
        house_id=house_id,
    )
    if booking_model is None:
        raise KeyError("Booking not found.")

    ensure_can_modify_booking(current_user, booking_model.to_json())

    normalized_new_datetime = normalize_datetime_string(new_datetime)
    new_date_token, new_time_token = datetime_to_tokens(normalized_new_datetime)
    ttl_seconds = compute_ttl_seconds(
        normalized_new_datetime,
        ttl_buffer_after=int(_TTL_BUFFER_AFTER_BOOKING.total_seconds()),
        ttl_min_seconds=_TTL_MIN_SECONDS,
    )

    booking_model.booking_datetime = normalized_new_datetime
    updated_payload = booking_model.to_json()
    new_actor_key = build_user_booking_key(
        selector.actor_id,
        new_date_token,
        new_time_token,
        selector.other_id,
        selector.house_id,
    )
    new_other_key = build_user_booking_key(
        selector.other_id,
        new_date_token,
        new_time_token,
        selector.actor_id,
        selector.house_id,
    )

    try:
        if new_actor_key == selector.actor_key and new_other_key == selector.other_key:
            write_booking_by_key(redis_client, selector.actor_key, updated_payload, ttl_seconds, nx=False)
            write_booking_by_key(redis_client, selector.other_key, updated_payload, ttl_seconds, nx=False)
            return booking_model

        _write_pair_with_nx(
            new_actor_key,
            new_other_key,
            updated_payload,
            ttl_seconds,
            conflict_message="Booking slot already booked.",
        )
        delete_booking_by_key(redis_client, selector.actor_key)
        delete_booking_by_key(redis_client, selector.other_key)
    except ValueError:
        raise
    except RedisError:
        raise RuntimeError(BOOKING_PERSISTENCE_ERROR_MSG)
    except Exception as exc:
        raise RuntimeError("Unable to update booking in Redis.") from exc

    return booking_model


def delete_booking(
    current_user: User,
    date: str,
    time: str,
    with_user_id: str,
    house_id: str,
) -> bool:
    if redis_client is None:
        raise RuntimeError("Redis client is not configured.")

    selector = _build_selector(
        current_user=current_user,
        date=date,
        time=time,
        with_user_id=with_user_id,
        house_id=house_id,
    )

    try:
        actor_payload = read_booking_by_key(redis_client, selector.actor_key)
        other_payload = read_booking_by_key(redis_client, selector.other_key)
    except Exception as exc:
        raise RuntimeError("Unable to read booking from Redis.") from exc

    existing_payload = actor_payload or other_payload
    if existing_payload is not None:
        booking_model = Booking.from_json(existing_payload)
        if not can_view_booking(current_user, booking_model.to_json()):
            raise PermissionError("Not allowed to view this booking.")
        ensure_can_modify_booking(current_user, booking_model.to_json())

    try:
        deleted_actor = delete_booking_by_key(redis_client, selector.actor_key)
        deleted_other = delete_booking_by_key(redis_client, selector.other_key)
    except Exception as exc:
        raise RuntimeError("Unable to delete booking from Redis.") from exc

    return bool(deleted_actor or deleted_other)
