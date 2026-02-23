from typing import Any, Dict, List, Optional

from bson import ObjectId

from app.core.database import mongo_db
from app.models.house import House
from app.models.user import User
from app.utils import find_by_id
from app.utils.houses_utils import (
    build_house_filters,
    find_buyer_ids_for_house,
    pull_house_from_all_user_lists,
    read_user_house_list,
    require_user,
    resolve_seller_embed,
    seller_embed_from_user_doc,
    sync_house_lists,
)
from app.utils.permissions_utils import ensure_can_create_house, ensure_can_manage_house
from app.utils.permissions_utils import ensure_roles

_HOUSES_COLLECTION = mongo_db["houses"]
_USERS_COLLECTION = mongo_db["users"]


def get_houses(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    skip: int = 0,
) -> List[House]:
    mongo_filters = build_house_filters(filters or {})
    safe_limit = max(limit, 0) or 100
    safe_skip = max(skip, 0)

    cursor = (
        _HOUSES_COLLECTION.find(mongo_filters)
        .sort("_id", 1)
        .skip(safe_skip)
        .limit(safe_limit)
    )
    return [House.from_json(doc) for doc in cursor]


def get_house_detail(house_id: str) -> Optional[House]:
    doc = find_by_id(_HOUSES_COLLECTION, house_id)
    if not doc:
        return None
    return House.from_json(doc)


def create_house(current_user: User, payload: Dict[str, Any]) -> House:
    ensure_can_create_house(current_user)

    role = str(current_user.role).strip().lower()
    data = dict(payload)
    data.pop("_id", None)
    data.pop("id", None)

    if role == "seller":
        seller_doc = require_user(_USERS_COLLECTION, current_user.id, expected_role="seller")
        seller_embed = seller_embed_from_user_doc(seller_doc)
    elif role == "admin":
        seller_embed = resolve_seller_embed(data.get("for_sale_by"), _USERS_COLLECTION)
    else:
        raise PermissionError("Not allowed to create houses.")

    house_id = ObjectId()
    payload_house: Dict[str, Any] = dict(data)
    payload_house["_id"] = house_id
    payload_house["for_sale_by"] = seller_embed

    house_model = House.from_json(payload_house)
    house_doc = house_model.to_json()
    house_doc["_id"] = house_id
    _HOUSES_COLLECTION.insert_one(house_doc)

    sync_house_lists(_USERS_COLLECTION, house_doc, buyer_ids=[])
    return House.from_json(house_doc)


def update_house(
    current_user: User,
    house_id: str,
    payload: Dict[str, Any],
) -> Optional[House]:
    doc = find_by_id(_HOUSES_COLLECTION, house_id)
    if not doc:
        return None

    ensure_can_manage_house(current_user, doc)

    role = str(current_user.role).strip().lower()
    data = dict(payload)
    data.pop("_id", None)
    data.pop("id", None)

    if "for_sale_by" in data:
        if role != "admin":
            raise PermissionError("Only admin can change 'for_sale_by'.")
        data["for_sale_by"] = resolve_seller_embed(data.get("for_sale_by"), _USERS_COLLECTION)

    merged_doc = dict(doc)
    merged_doc.update(data)
    merged_doc["_id"] = doc["_id"]

    normalized_doc = House.from_json(merged_doc).to_json()
    normalized_doc["_id"] = doc["_id"]

    current_normalized = House.from_json(doc).to_json()
    current_normalized["_id"] = doc["_id"]

    if normalized_doc == current_normalized:
        return House.from_json(doc)

    _HOUSES_COLLECTION.replace_one({"_id": doc["_id"]}, normalized_doc)
    updated_doc = _HOUSES_COLLECTION.find_one({"_id": doc["_id"]})
    if not updated_doc:
        return None

    existing_buyer_ids = find_buyer_ids_for_house(_USERS_COLLECTION, updated_doc["_id"])
    if not bool(updated_doc.get("is_sold")):
        target_buyer_ids: List[Any] = []
    else:
        target_buyer_ids = existing_buyer_ids

    sync_house_lists(_USERS_COLLECTION, updated_doc, buyer_ids=target_buyer_ids)
    return House.from_json(updated_doc)


def delete_house(current_user: User, house_id: str) -> bool:
    doc = find_by_id(_HOUSES_COLLECTION, house_id)
    if not doc:
        return False

    ensure_can_manage_house(current_user, doc)
    result = _HOUSES_COLLECTION.delete_one({"_id": doc["_id"]})
    if result.deleted_count <= 0:
        return False

    pull_house_from_all_user_lists(_USERS_COLLECTION, doc["_id"])
    return True


def get_my_bought_houses(current_user: User) -> List[House]:
    ensure_roles(
        current_user,
        {"buyer", "seller", "admin"},
        message="Only buyers, sellers, and admins can view bought houses.",
    )
    return read_user_house_list(
        _USERS_COLLECTION,
        current_user,
        "bought_houses",
    )


def get_my_for_sale_houses(current_user: User) -> List[House]:
    ensure_roles(current_user, {"seller", "admin"}, message="Only sellers and admins can view for-sale houses.")
    return read_user_house_list(
        _USERS_COLLECTION,
        current_user,
        "for_sale_houses",
    )


def get_my_sold_houses(current_user: User) -> List[House]:
    ensure_roles(current_user, {"seller", "admin"}, message="Only sellers and admins can view sold houses.")
    return read_user_house_list(
        _USERS_COLLECTION,
        current_user,
        "sold_houses",
    )
