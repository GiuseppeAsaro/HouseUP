from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo.errors import DuplicateKeyError

from app.core.database import mongo_db
from app.models import BookingResult, User
from app.utils.db_utils import build_id_filter, id_match_values
from app.utils.normalize_utils import (
    normalize_bool_strict,
    normalize_datetime_string,
    normalize_optional_text,
    normalize_positive_float,
    normalize_required_text,
)
from app.utils.permissions_utils import ensure_roles


_BOOKING_RESULTS_COLLECTION = mongo_db["booking_results"]
_HOUSES_COLLECTION = mongo_db["houses"]
_USERS_COLLECTION = mongo_db["users"]


def _build_user_snapshot(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = {
        "id": normalize_required_text(
            user_doc.get("_id") or user_doc.get("id"),
            "Seller id is required.",
        ),
        "email": normalize_optional_text(user_doc.get("email")),
        "full_name": normalize_optional_text(user_doc.get("full_name") or user_doc.get("name")),
    }
    return {key: value for key, value in snapshot.items() if value is not None}


def _load_user_snapshot_by_id(user_id: str) -> Dict[str, Any]:
    try:
        user_doc = _USERS_COLLECTION.find_one(
            build_id_filter(user_id),
            {"_id": 1, "email": 1, "full_name": 1},
        )
    except Exception:  # noqa: BLE001
        user_doc = None

    if isinstance(user_doc, dict) and user_doc.get("_id") is not None:
        try:
            return _build_user_snapshot(user_doc)
        except ValueError:
            pass

    return {"id": user_id}


def _seller_snapshot_from_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_seller = payload.get("seller")
    if raw_seller is None:
        return None

    if not isinstance(raw_seller, dict):
        raise ValueError("'seller' must be an object.")

    seller_id = normalize_required_text(
        raw_seller.get("id") or raw_seller.get("_id"),
        "'seller.id' is required.",
    )
    snapshot: Dict[str, Any] = {"id": seller_id}

    full_name = normalize_optional_text(raw_seller.get("full_name") or raw_seller.get("name"))
    email = normalize_optional_text(raw_seller.get("email"))
    if full_name is not None:
        snapshot["full_name"] = full_name
    if email is not None:
        snapshot["email"] = email

    return snapshot


def _merge_seller_snapshot_with_directory(raw_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    seller_id = normalize_required_text(raw_snapshot.get("id"), "Seller snapshot id is required.")
    merged_snapshot = _load_user_snapshot_by_id(seller_id)
    merged_snapshot["id"] = seller_id

    for field_name in ("full_name", "email"):
        value = normalize_optional_text(raw_snapshot.get(field_name))
        if value is not None:
            merged_snapshot[field_name] = value

    return merged_snapshot


def _seller_id_from_booking_result_doc(booking_result_doc: Dict[str, Any]) -> Optional[str]:
    seller_doc = booking_result_doc.get("seller")
    if isinstance(seller_doc, dict):
        seller_id = normalize_optional_text(seller_doc.get("id") or seller_doc.get("_id"))
        if seller_id is not None:
            return seller_id
    return None


def _seller_snapshot_from_booking_result_doc(booking_result_doc: Dict[str, Any]) -> Dict[str, Any]:
    seller_doc = booking_result_doc.get("seller")
    if isinstance(seller_doc, dict):
        seller_id = normalize_optional_text(seller_doc.get("id") or seller_doc.get("_id"))
        if seller_id is not None:
            snapshot: Dict[str, Any] = {"id": seller_id}
            full_name = normalize_optional_text(seller_doc.get("full_name") or seller_doc.get("name"))
            email = normalize_optional_text(seller_doc.get("email"))
            if full_name is not None:
                snapshot["full_name"] = full_name
            if email is not None:
                snapshot["email"] = email
            return snapshot

    raise ValueError("Seller snapshot is missing.")


def _seller_snapshot_for_create(current_user: User, payload: Dict[str, Any]) -> Dict[str, Any]:
    role = str(current_user.role).strip().lower()

    if role == "seller":
        payload_snapshot = _seller_snapshot_from_payload(payload)
        if payload_snapshot is not None:
            payload_seller_id = normalize_required_text(
                payload_snapshot.get("id"),
                "'seller.id' is required.",
            )
            if payload_seller_id != str(current_user.id):
                raise PermissionError("Sellers can only assign themselves as booking-result seller.")

        return _build_user_snapshot(
            {
                "id": str(current_user.id),
                "email": current_user.email,
                "full_name": current_user.full_name,
            }
        )

    payload_snapshot = _seller_snapshot_from_payload(payload)
    if payload_snapshot is None:
        raise ValueError("'seller.id' is required for admin.")
    return _merge_seller_snapshot_with_directory(payload_snapshot)


def _seller_snapshot_for_update(
    current_user: User,
    payload: Dict[str, Any],
    existing_doc: Dict[str, Any],
) -> Dict[str, Any]:
    role = str(current_user.role).strip().lower()

    if role == "seller":
        if "seller" in payload:
            payload_snapshot = _seller_snapshot_from_payload(payload)
            if payload_snapshot is None:
                raise ValueError("'seller.id' is required.")
            payload_seller_id = normalize_required_text(
                payload_snapshot.get("id"),
                "'seller.id' is required.",
            )
            if payload_seller_id != str(current_user.id):
                raise PermissionError("Sellers can only assign themselves as booking-result seller.")

        return _build_user_snapshot(
            {
                "id": str(current_user.id),
                "email": current_user.email,
                "full_name": current_user.full_name,
            }
        )

    if "seller" in payload:
        payload_snapshot = _seller_snapshot_from_payload(payload)
        if payload_snapshot is None:
            raise ValueError("'seller.id' is required for admin.")
        return _merge_seller_snapshot_with_directory(payload_snapshot)

    return _seller_snapshot_from_booking_result_doc(existing_doc)


def _sync_purchase_state_best_effort(
    *,
    house_id: str,
    house_city: str,
    house_zip_code: str,
    seller_id: str,
) -> None:
    house_embed = {
        "id": house_id,
        "city": house_city,
        "zip_code": house_zip_code,
    }
    match_values = id_match_values(house_id)

    try:
        _HOUSES_COLLECTION.update_one(
            build_id_filter(house_id),
            {"$set": {"is_sold": True}},
            upsert=False,
        )
    except Exception:  # noqa: BLE001
        pass

    try:
        _USERS_COLLECTION.update_many(
            {"for_sale_houses.id": {"$in": match_values}},
            {
                "$pull": {
                    "for_sale_houses": {"id": {"$in": match_values}},
                }
            },
        )
    except Exception:  # noqa: BLE001
        pass

    push_payload = {
        "$each": [house_embed],
        "$position": 0,
    }

    try:
        _USERS_COLLECTION.update_one(
            build_id_filter(seller_id),
            {"$push": {"sold_houses": push_payload}},
            upsert=False,
        )
    except Exception:  # noqa: BLE001
        pass


def _build_booking_result_embed(booking_result_doc: Dict[str, Any]) -> Dict[str, Any]:
    booking_result_id = normalize_required_text(
        booking_result_doc.get("booking_result_id") or booking_result_doc.get("_id"),
        "Booking result id is missing.",
    )
    booking_date_dt = _normalize_booking_datetime_to_utc(booking_result_doc.get("booking_date"))
    booking_date_str = booking_date_dt.isoformat().replace("+00:00", "Z")
    house_bought = normalize_bool_strict(booking_result_doc.get("house_bought"), "house_bought")

    house_data = _house_from_doc(booking_result_doc)
    house_id = normalize_required_text(house_data.get("id"), "Booking result house id is missing.")
    house_listing_price = normalize_positive_float(
        house_data.get("listing_price"),
        "Booking result house listing_price is missing or invalid.",
    )
    house_city = normalize_required_text(house_data.get("city"), "Booking result house city is missing.")
    house_zip_code = normalize_required_text(
        house_data.get("zip_code"),
        "Booking result house zip_code is missing.",
    )

    embed: Dict[str, Any] = {
        "id": booking_result_id,
        "booking_date": booking_date_str,
        "house_bought": house_bought,
        "house": {
            "id": house_id,
            "listing_price": house_listing_price,
            "city": house_city,
            "zip_code": house_zip_code,
        },
    }

    if house_bought:
        raw_final_price = booking_result_doc.get("final_price")
        if raw_final_price not in (None, ""):
            try:
                embed["final_price"] = normalize_positive_float(
                    raw_final_price,
                    "Booking result final_price is invalid.",
                )
            except ValueError:
                pass

    return embed


def _sync_booking_result_embed_best_effort(seller_id: str, booking_result_doc: Dict[str, Any]) -> None:
    booking_result_id = normalize_required_text(
        booking_result_doc.get("booking_result_id") or booking_result_doc.get("_id"),
        "Booking result id is missing.",
    )
    booking_result_embed = _build_booking_result_embed(booking_result_doc)

    try:
        _USERS_COLLECTION.update_one(
            build_id_filter(seller_id),
            {"$pull": {"booking_results": {"id": booking_result_id}}},
            upsert=False,
        )
        _USERS_COLLECTION.update_one(
            build_id_filter(seller_id),
            {
                "$push": {
                    "booking_results": {
                        "$each": [booking_result_embed],
                        "$position": 0,
                    }
                }
            },
            upsert=False,
        )
    except Exception:  # noqa: BLE001
        pass


def _drop_booking_result_from_seller_best_effort(seller_id: str, booking_result_id: str) -> None:
    try:
        _USERS_COLLECTION.update_one(
            build_id_filter(seller_id),
            {"$pull": {"booking_results": {"id": booking_result_id}}},
            upsert=False,
        )
    except Exception:  # noqa: BLE001
        pass


def list_booking_results_from_user_list(current_user: User) -> List[Dict[str, Any]]:
    ensure_roles(
        current_user,
        {"seller", "admin"},
        message="Only sellers and admins can list booking results from user profile.",
    )

    user_doc = _USERS_COLLECTION.find_one(
        build_id_filter(str(current_user.id)),
        {"booking_results": 1},
    )
    if not user_doc:
        return []

    booking_results = user_doc.get("booking_results")
    if not isinstance(booking_results, list):
        return []

    return [item for item in booking_results if isinstance(item, dict)]


def _normalize_booking_datetime_to_utc(value: Any) -> datetime:
    booking_datetime = normalize_datetime_string(value)
    booking_date_dt = datetime.fromisoformat(booking_datetime.replace("Z", "+00:00"))
    if booking_date_dt.tzinfo is None:
        booking_date_dt = booking_date_dt.replace(tzinfo=timezone.utc)
    return booking_date_dt.astimezone(timezone.utc)


def _house_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    house = doc.get("house")
    if isinstance(house, dict):
        return dict(house)
    return {}


def _ensure_can_manage_booking_result(current_user: User, booking_result_doc: Dict[str, Any]) -> None:
    role = str(current_user.role).strip().lower()
    if role == "admin":
        return

    seller_id = _seller_id_from_booking_result_doc(booking_result_doc) or ""

    if role == "seller" and seller_id and seller_id == str(current_user.id):
        return

    raise PermissionError("Not allowed to modify this booking result.")


def create_booking_result(current_user: User, payload: Dict[str, Any]) -> BookingResult:
    ensure_roles(
        current_user,
        {"seller", "admin"},
        message="Only sellers and admins can create booking results.",
    )

    booking_id = normalize_required_text(payload.get("booking_id"), "'booking_id' is required.")
    raw_booking_datetime = payload.get("booking_datetime")
    if raw_booking_datetime is None:
        raise ValueError("'booking_datetime' is required.")
    booking_date_dt = _normalize_booking_datetime_to_utc(raw_booking_datetime)

    house_bought = normalize_bool_strict(payload.get("house_bought"), "house_bought")

    house_id = normalize_required_text(payload.get("house_id"), "'house_id' is required.")
    house_listing_price = normalize_positive_float(
        payload.get("house_listing_price"),
        "'house_listing_price' must be a positive number.",
    )
    house_city = normalize_required_text(payload.get("house_city"), "'house_city' is required.")
    house_zip_code = normalize_required_text(payload.get("house_zip_code"), "'house_zip_code' is required.")

    seller_snapshot = _seller_snapshot_for_create(current_user, payload)
    seller_id = normalize_required_text(seller_snapshot.get("id"), "Seller snapshot id is required.")

    final_price: Optional[float] = None
    if house_bought:
        final_price = normalize_positive_float(
            payload.get("final_price"),
            "'final_price' must be a positive number when 'house_bought' is true.",
        )

    booking_result_doc: Dict[str, Any] = {
        "_id": booking_id,
        "booking_date": booking_date_dt,
        "house_bought": house_bought,
        "house": {
            "id": house_id,
            "listing_price": house_listing_price,
            "city": house_city,
            "zip_code": house_zip_code,
        },
        "seller": seller_snapshot,
    }
    if final_price is not None:
        booking_result_doc["final_price"] = final_price

    try:
        _BOOKING_RESULTS_COLLECTION.insert_one(booking_result_doc)
    except DuplicateKeyError as exc:
        raise ValueError("Booking result already exists for this booking.") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to create booking result.") from exc

    if house_bought:
        _sync_purchase_state_best_effort(
            house_id=house_id,
            house_city=house_city,
            house_zip_code=house_zip_code,
            seller_id=seller_id,
        )
    _sync_booking_result_embed_best_effort(seller_id, booking_result_doc)

    return BookingResult.from_json(booking_result_doc)


def update_booking_result(
    current_user: User,
    booking_result_id: str,
    payload: Dict[str, Any],
) -> Optional[BookingResult]:
    ensure_roles(
        current_user,
        {"seller", "admin"},
        message="Only sellers and admins can update booking results.",
    )

    existing_doc = _BOOKING_RESULTS_COLLECTION.find_one(build_id_filter(booking_result_id))
    if not existing_doc:
        return None

    _ensure_can_manage_booking_result(current_user, existing_doc)
    old_seller_id = _seller_id_from_booking_result_doc(existing_doc)

    merged_doc: Dict[str, Any] = dict(existing_doc)
    merged_house = _house_from_doc(existing_doc)

    if "booking_datetime" in payload:
        merged_doc["booking_date"] = _normalize_booking_datetime_to_utc(payload.get("booking_datetime"))

    if "house_bought" in payload:
        merged_doc["house_bought"] = normalize_bool_strict(payload.get("house_bought"), "house_bought")

    if "house_id" in payload:
        merged_house["id"] = normalize_required_text(payload.get("house_id"), "'house_id' is required.")
    if "house_listing_price" in payload:
        merged_house["listing_price"] = normalize_positive_float(
            payload.get("house_listing_price"),
            "'house_listing_price' must be a positive number.",
        )
    if "house_city" in payload:
        merged_house["city"] = normalize_required_text(payload.get("house_city"), "'house_city' is required.")
    if "house_zip_code" in payload:
        merged_house["zip_code"] = normalize_required_text(payload.get("house_zip_code"), "'house_zip_code' is required.")
    merged_doc["house"] = merged_house

    seller_snapshot = _seller_snapshot_for_update(current_user, payload, existing_doc)
    seller_id = normalize_required_text(seller_snapshot.get("id"), "Seller snapshot id is required.")

    booking_date_dt = _normalize_booking_datetime_to_utc(merged_doc.get("booking_date"))
    house_bought = normalize_bool_strict(merged_doc.get("house_bought"), "house_bought")

    normalized_house = _house_from_doc(merged_doc)
    house_id = normalize_required_text(normalized_house.get("id"), "'house_id' is required.")
    house_listing_price = normalize_positive_float(
        normalized_house.get("listing_price"),
        "'house_listing_price' must be a positive number.",
    )
    house_city = normalize_required_text(normalized_house.get("city"), "'house_city' is required.")
    house_zip_code = normalize_required_text(normalized_house.get("zip_code"), "'house_zip_code' is required.")
    final_price: Optional[float] = None
    if house_bought:
        if "final_price" in payload:
            final_price = normalize_positive_float(
                payload.get("final_price"),
                "'final_price' must be a positive number when 'house_bought' is true.",
            )
        else:
            final_price = normalize_positive_float(
                merged_doc.get("final_price"),
                "'final_price' must be a positive number when 'house_bought' is true.",
            )

    normalized_doc: Dict[str, Any] = {
        "_id": existing_doc["_id"],
        "booking_date": booking_date_dt,
        "house_bought": house_bought,
        "house": {
            "id": house_id,
            "listing_price": house_listing_price,
            "city": house_city,
            "zip_code": house_zip_code,
        },
        "seller": seller_snapshot,
    }
    if final_price is not None:
        normalized_doc["final_price"] = final_price

    try:
        _BOOKING_RESULTS_COLLECTION.replace_one({"_id": existing_doc["_id"]}, normalized_doc)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to update booking result.") from exc

    if house_bought:
        _sync_purchase_state_best_effort(
            house_id=house_id,
            house_city=house_city,
            house_zip_code=house_zip_code,
            seller_id=seller_id,
        )
    booking_result_id_str = normalize_required_text(
        existing_doc.get("booking_result_id") or existing_doc.get("_id"),
        "Booking result id is missing.",
    )
    if old_seller_id is not None and old_seller_id != seller_id:
        _drop_booking_result_from_seller_best_effort(old_seller_id, booking_result_id_str)
    _sync_booking_result_embed_best_effort(seller_id, normalized_doc)

    return BookingResult.from_json(normalized_doc)


def delete_booking_result(current_user: User, booking_result_id: str) -> bool:
    ensure_roles(
        current_user,
        {"seller", "admin"},
        message="Only sellers and admins can delete booking results.",
    )

    existing_doc = _BOOKING_RESULTS_COLLECTION.find_one(build_id_filter(booking_result_id))
    if not existing_doc:
        return False

    _ensure_can_manage_booking_result(current_user, existing_doc)
    seller_id = _seller_id_from_booking_result_doc(existing_doc)
    booking_result_id_str = normalize_required_text(
        existing_doc.get("booking_result_id") or existing_doc.get("_id"),
        "Booking result id is missing.",
    )

    try:
        result = _BOOKING_RESULTS_COLLECTION.delete_one({"_id": existing_doc["_id"]})
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to delete booking result.") from exc

    deleted = bool(result.deleted_count > 0)
    if deleted and seller_id is not None:
        _drop_booking_result_from_seller_best_effort(seller_id, booking_result_id_str)

    return deleted
