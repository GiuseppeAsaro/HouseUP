from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo.errors import DuplicateKeyError

from app.core.database import mongo_db
from app.models import Feedback, User
from app.utils.db_utils import build_id_filter
from app.utils.normalize_utils import (
    normalize_datetime_string,
    normalize_int_range,
    normalize_optional_text,
    normalize_required_text,
)
from app.utils.permissions_utils import ensure_roles


_FEEDBACKS_COLLECTION = mongo_db["feedbacks"]
_USERS_COLLECTION = mongo_db["users"]


def _normalize_feedback_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _feedback_owner_id(feedback_doc: Dict[str, Any]) -> str:
    user_data = feedback_doc.get("user")
    if not isinstance(user_data, dict):
        user_data = {}
    return normalize_required_text(user_data.get("id"), "Feedback user id is missing.")


def _feedback_user_snapshot(feedback_doc: Dict[str, Any]) -> Dict[str, str]:
    user_data = feedback_doc.get("user")
    if not isinstance(user_data, dict):
        user_data = {}

    return {
        "id": normalize_required_text(user_data.get("id"), "Feedback user id is missing."),
        "full_name": normalize_required_text(user_data.get("full_name"), "Feedback user full_name is missing."),
        "email": normalize_required_text(user_data.get("email"), "Feedback user email is missing."),
        "phone": normalize_required_text(user_data.get("phone"), "Feedback user phone is missing."),
    }


def _feedback_seller_snapshot(feedback_doc: Dict[str, Any]) -> Dict[str, str]:
    seller_data = feedback_doc.get("seller")
    if not isinstance(seller_data, dict):
        seller_data = {}

    return {
        "id": normalize_required_text(seller_data.get("id"), "Feedback seller id is missing."),
        "full_name": normalize_required_text(seller_data.get("full_name"), "Feedback seller full_name is missing."),
        "email": normalize_required_text(seller_data.get("email"), "Feedback seller email is missing."),
        "phone": normalize_required_text(seller_data.get("phone"), "Feedback seller phone is missing."),
    }


def _build_feedback_embed(feedback_doc: Dict[str, Any]) -> Dict[str, Any]:
    feedback_id = normalize_required_text(feedback_doc.get("callslot_id"), "Feedback callslot_id is missing.")
    feedback_date = _normalize_feedback_date(feedback_doc.get("feedback_date"))
    feedback_date_str = feedback_date.isoformat().replace("+00:00", "Z")
    rating = normalize_int_range(
        feedback_doc.get("rating"),
        min_value=1,
        max_value=5,
        message="'rating' must be an integer between 1 and 5.",
    )
    call_datetime = normalize_datetime_string(feedback_doc.get("call_datetime"))
    comment = normalize_optional_text(feedback_doc.get("comment"))

    embed: Dict[str, Any] = {
        "id": feedback_id,
        "feedback_date": feedback_date_str,
        "callslot_id": feedback_id,
        "call_datetime": call_datetime,
        "rating": rating,
        "seller": _feedback_seller_snapshot(feedback_doc),
    }
    if comment is not None:
        embed["comment"] = comment
    return embed


def _sync_feedback_embed_best_effort(user_id: str, feedback_doc: Dict[str, Any]) -> None:
    feedback_id = normalize_required_text(feedback_doc.get("callslot_id"), "Feedback callslot_id is missing.")
    feedback_embed = _build_feedback_embed(feedback_doc)

    try:
        _USERS_COLLECTION.update_one(
            build_id_filter(user_id),
            {"$pull": {"feedbacks": {"id": feedback_id}}},
            upsert=False,
        )
        _USERS_COLLECTION.update_one(
            build_id_filter(user_id),
            {
                "$push": {
                    "feedbacks": {
                        "$each": [feedback_embed],
                        "$position": 0,
                    }
                }
            },
            upsert=False,
        )
    except Exception:  # noqa: BLE001
        pass


def _drop_feedback_from_user_best_effort(user_id: str, feedback_id: str) -> None:
    try:
        _USERS_COLLECTION.update_one(
            build_id_filter(user_id),
            {
                "$pull": {
                    "feedbacks": {"id": feedback_id},
                }
            },
            upsert=False,
        )
    except Exception:  # noqa: BLE001
        pass


def list_feedbacks_from_user_list(current_user: User) -> List[Dict[str, Any]]:
    ensure_roles(
        current_user,
        {"buyer", "seller", "admin"},
        message="Only authenticated users can list feedbacks from user profile.",
    )

    user_doc = _USERS_COLLECTION.find_one(
        build_id_filter(str(current_user.id)),
        {"feedbacks": 1},
    )
    if not user_doc:
        return []

    feedbacks = user_doc.get("feedbacks")
    if not isinstance(feedbacks, list):
        return []

    return [item for item in feedbacks if isinstance(item, dict)]


def _ensure_can_manage_feedback(current_user: User, feedback_doc: Dict[str, Any]) -> None:
    role = str(current_user.role).strip().lower()
    if role == "admin":
        return

    owner_id = _feedback_owner_id(feedback_doc)
    if role == "buyer" and owner_id == str(current_user.id):
        return

    raise PermissionError("Not allowed to modify this feedback.")


def create_feedback(current_user: User, payload: Dict[str, Any]) -> Feedback:
    ensure_roles(current_user, {"buyer"}, message="Only buyers can create feedback.")

    callslot_id = normalize_required_text(payload.get("callslot_id"), "'callslot_id' is required.")
    raw_call_datetime = payload.get("call_datetime")
    if raw_call_datetime is None:
        raise ValueError("'call_datetime' is required.")
    call_datetime = normalize_datetime_string(raw_call_datetime)

    rating = normalize_int_range(
        payload.get("rating"),
        min_value=1,
        max_value=5,
        message="'rating' must be an integer between 1 and 5.",
    )
    comment = normalize_optional_text(payload.get("comment"))

    buyer_full_name = normalize_required_text(
        payload.get("buyer_full_name"),
        "'buyer_full_name' is required.",
    )
    buyer_email = normalize_required_text(payload.get("buyer_email"), "'buyer_email' is required.")
    buyer_phone = normalize_required_text(payload.get("buyer_phone"), "'buyer_phone' is required.")

    seller_id = normalize_required_text(payload.get("seller_id"), "'seller_id' is required.")
    seller_full_name = normalize_required_text(
        payload.get("seller_full_name"),
        "'seller_full_name' is required.",
    )
    seller_email = normalize_required_text(payload.get("seller_email"), "'seller_email' is required.")
    seller_phone = normalize_required_text(payload.get("seller_phone"), "'seller_phone' is required.")

    feedback_date_dt = _normalize_feedback_date(None)

    feedback_doc: Dict[str, Any] = {
        "_id": callslot_id,
        "feedback_date": feedback_date_dt,
        "callslot_id": callslot_id,
        "call_datetime": call_datetime,
        "rating": rating,
        "user": {
            "id": str(current_user.id),
            "full_name": buyer_full_name,
            "email": buyer_email,
            "phone": buyer_phone,
        },
        "seller": {
            "id": seller_id,
            "full_name": seller_full_name,
            "email": seller_email,
            "phone": seller_phone,
        },
    }
    if comment is not None:
        feedback_doc["comment"] = comment

    try:
        _FEEDBACKS_COLLECTION.insert_one(feedback_doc)
    except DuplicateKeyError as exc:
        raise ValueError("Feedback already exists for this callslot.") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to create feedback.") from exc

    _sync_feedback_embed_best_effort(str(current_user.id), feedback_doc)

    return Feedback.from_json(feedback_doc)


def update_feedback(
    current_user: User,
    feedback_id: str,
    payload: Dict[str, Any],
) -> Optional[Feedback]:
    ensure_roles(
        current_user,
        {"buyer", "admin"},
        message="Only buyers and admins can update feedback.",
    )

    existing_doc = _FEEDBACKS_COLLECTION.find_one(build_id_filter(feedback_id))
    if not existing_doc:
        return None

    _ensure_can_manage_feedback(current_user, existing_doc)

    role = str(current_user.role).strip().lower()
    old_owner_id = _feedback_owner_id(existing_doc)
    merged_doc: Dict[str, Any] = dict(existing_doc)
    user_data = dict(existing_doc.get("user") if isinstance(existing_doc.get("user"), dict) else {})
    seller_data = dict(existing_doc.get("seller") if isinstance(existing_doc.get("seller"), dict) else {})

    if "call_datetime" in payload:
        merged_doc["call_datetime"] = normalize_datetime_string(payload.get("call_datetime"))
    if "rating" in payload:
        merged_doc["rating"] = normalize_int_range(
            payload.get("rating"),
            min_value=1,
            max_value=5,
            message="'rating' must be an integer between 1 and 5.",
        )
    if "comment" in payload:
        comment = normalize_optional_text(payload.get("comment"))
        if comment is None:
            merged_doc.pop("comment", None)
        else:
            merged_doc["comment"] = comment

    if "buyer_full_name" in payload:
        user_data["full_name"] = normalize_required_text(payload.get("buyer_full_name"), "'buyer_full_name' is required.")
    if "buyer_email" in payload:
        user_data["email"] = normalize_required_text(payload.get("buyer_email"), "'buyer_email' is required.")
    if "buyer_phone" in payload:
        user_data["phone"] = normalize_required_text(payload.get("buyer_phone"), "'buyer_phone' is required.")
    if role == "admin" and "buyer_id" in payload:
        user_data["id"] = normalize_required_text(payload.get("buyer_id"), "'buyer_id' is required.")

    if "seller_id" in payload:
        seller_data["id"] = normalize_required_text(payload.get("seller_id"), "'seller_id' is required.")
    if "seller_full_name" in payload:
        seller_data["full_name"] = normalize_required_text(payload.get("seller_full_name"), "'seller_full_name' is required.")
    if "seller_email" in payload:
        seller_data["email"] = normalize_required_text(payload.get("seller_email"), "'seller_email' is required.")
    if "seller_phone" in payload:
        seller_data["phone"] = normalize_required_text(payload.get("seller_phone"), "'seller_phone' is required.")

    merged_doc["user"] = user_data
    merged_doc["seller"] = seller_data

    owner_snapshot = _feedback_user_snapshot(merged_doc)
    seller_snapshot = _feedback_seller_snapshot(merged_doc)
    if role == "buyer" and owner_snapshot["id"] != str(current_user.id):
        raise PermissionError("Buyers can only assign themselves as feedback owner.")

    callslot_id = normalize_required_text(
        merged_doc.get("callslot_id") or existing_doc.get("_id"),
        "'callslot_id' is required.",
    )
    call_datetime = normalize_datetime_string(merged_doc.get("call_datetime"))
    rating = normalize_int_range(
        merged_doc.get("rating"),
        min_value=1,
        max_value=5,
        message="'rating' must be an integer between 1 and 5.",
    )
    feedback_date_dt = _normalize_feedback_date(existing_doc.get("feedback_date"))
    comment = normalize_optional_text(merged_doc.get("comment"))

    normalized_doc: Dict[str, Any] = {
        "_id": existing_doc["_id"],
        "feedback_date": feedback_date_dt,
        "callslot_id": callslot_id,
        "call_datetime": call_datetime,
        "rating": rating,
        "user": owner_snapshot,
        "seller": seller_snapshot,
    }
    if comment is not None:
        normalized_doc["comment"] = comment

    try:
        _FEEDBACKS_COLLECTION.replace_one({"_id": existing_doc["_id"]}, normalized_doc)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to update feedback.") from exc

    if owner_snapshot["id"] != old_owner_id:
        _drop_feedback_from_user_best_effort(old_owner_id, callslot_id)
    _sync_feedback_embed_best_effort(owner_snapshot["id"], normalized_doc)

    return Feedback.from_json(normalized_doc)


def delete_feedback(current_user: User, feedback_id: str) -> bool:
    ensure_roles(
        current_user,
        {"buyer", "admin"},
        message="Only buyers and admins can delete feedback.",
    )

    existing_doc = _FEEDBACKS_COLLECTION.find_one(build_id_filter(feedback_id))
    if not existing_doc:
        return False

    _ensure_can_manage_feedback(current_user, existing_doc)
    owner_id = _feedback_owner_id(existing_doc)
    callslot_id = normalize_required_text(
        existing_doc.get("callslot_id") or existing_doc.get("_id"),
        "'callslot_id' is required.",
    )

    try:
        result = _FEEDBACKS_COLLECTION.delete_one({"_id": existing_doc["_id"]})
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to delete feedback.") from exc

    if result.deleted_count > 0:
        _drop_feedback_from_user_best_effort(owner_id, callslot_id)
        return True
    return False
