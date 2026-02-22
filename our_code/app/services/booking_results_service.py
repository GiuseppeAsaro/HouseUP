from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pymongo.errors import DuplicateKeyError

from app.core.database import mongo_db
from app.models import BookingResult, User
from app.utils.db_utils import build_id_filter, id_match_values
from app.utils.normalize_utils import (
    normalize_bool_strict,
    normalize_datetime_string,
    normalize_positive_float,
    normalize_required_text,
)
from app.utils.permissions_utils import ensure_roles


_BOOKING_RESULTS_COLLECTION = mongo_db["booking_results"]
_HOUSES_COLLECTION = mongo_db["houses"]
_USERS_COLLECTION = mongo_db["users"]


def _sync_purchase_state_best_effort(
    *,
    house_id: str,
    house_city: str,
    house_zip_code: str,
    seller_id: str,
    buyer_id: str,
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

    try:
        _USERS_COLLECTION.update_one(
            build_id_filter(buyer_id),
            {"$push": {"bought_houses": push_payload}},
            upsert=False,
        )
    except Exception:  # noqa: BLE001
        pass


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

    seller_id = str(booking_result_doc.get("seller_id") or "").strip()
    if role == "seller" and seller_id and seller_id == str(current_user.id):
        return

    raise PermissionError("Not allowed to modify this booking result.")


def create_booking_result(current_user: User, payload: Dict[str, Any]) -> BookingResult:
    ensure_roles(
        current_user,
        {"seller", "admin"},
        message="Only sellers and admins can create booking results.",
    )

    role = str(current_user.role).strip().lower()

    booking_id = normalize_required_text(payload.get("booking_id"), "'booking_id' is required.")
    raw_booking_datetime = payload.get("booking_datetime")
    if raw_booking_datetime is None:
        raise ValueError("'booking_datetime' is required.")
    booking_date_dt = _normalize_booking_datetime_to_utc(raw_booking_datetime)

    buyer_id = normalize_required_text(payload.get("buyer_id"), "'buyer_id' is required.")
    house_bought = normalize_bool_strict(payload.get("house_bought"), "house_bought")

    house_id = normalize_required_text(payload.get("house_id"), "'house_id' is required.")
    house_listing_price = normalize_positive_float(
        payload.get("house_listing_price"),
        "'house_listing_price' must be a positive number.",
    )
    house_city = normalize_required_text(payload.get("house_city"), "'house_city' is required.")
    house_zip_code = normalize_required_text(payload.get("house_zip_code"), "'house_zip_code' is required.")

    if role == "seller":
        seller_id = str(current_user.id)
    else:
        seller_id = normalize_required_text(payload.get("seller_id"), "'seller_id' is required for admin.")

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
        "buyer_id": buyer_id,
        "seller_id": seller_id,
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
            buyer_id=buyer_id,
        )

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

    role = str(current_user.role).strip().lower()
    merged_doc: Dict[str, Any] = dict(existing_doc)
    merged_house = _house_from_doc(existing_doc)

    if "booking_datetime" in payload:
        merged_doc["booking_date"] = _normalize_booking_datetime_to_utc(payload.get("booking_datetime"))

    if "buyer_id" in payload:
        merged_doc["buyer_id"] = normalize_required_text(payload.get("buyer_id"), "'buyer_id' is required.")

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

    if "seller_id" in payload:
        if role != "admin":
            raise PermissionError("Only admin can change 'seller_id'.")
        merged_doc["seller_id"] = normalize_required_text(payload.get("seller_id"), "'seller_id' is required.")

    booking_date_dt = _normalize_booking_datetime_to_utc(merged_doc.get("booking_date"))
    buyer_id = normalize_required_text(merged_doc.get("buyer_id"), "'buyer_id' is required.")
    house_bought = normalize_bool_strict(merged_doc.get("house_bought"), "house_bought")

    normalized_house = _house_from_doc(merged_doc)
    house_id = normalize_required_text(normalized_house.get("id"), "'house_id' is required.")
    house_listing_price = normalize_positive_float(
        normalized_house.get("listing_price"),
        "'house_listing_price' must be a positive number.",
    )
    house_city = normalize_required_text(normalized_house.get("city"), "'house_city' is required.")
    house_zip_code = normalize_required_text(normalized_house.get("zip_code"), "'house_zip_code' is required.")
    seller_id = normalize_required_text(merged_doc.get("seller_id"), "'seller_id' is required.")

    if role == "seller" and seller_id != str(current_user.id):
        raise PermissionError("Sellers can only assign themselves as 'seller_id'.")

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
        "buyer_id": buyer_id,
        "seller_id": seller_id,
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
            buyer_id=buyer_id,
        )

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

    try:
        result = _BOOKING_RESULTS_COLLECTION.delete_one({"_id": existing_doc["_id"]})
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to delete booking result.") from exc

    return bool(result.deleted_count > 0)
