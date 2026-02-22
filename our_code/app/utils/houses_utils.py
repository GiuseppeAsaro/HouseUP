from typing import Any, Dict, List, Optional, Sequence

from bson import ObjectId

from app.models.house import House
from app.models.user import User
from app.utils.db_utils import find_by_id
from app.utils.house_model_utils import parse_optional_float, parse_optional_int
from app.utils.parse_utils import parse_bool

USER_HOUSE_LIST_FIELDS = (
    "for_sale_houses",
    "sold_houses",
    "bought_houses",
)

_NUMERIC_FILTERS = {
    "price",
    "bed",
    "bath",
    "house_size",
}
_OBJECT_ID_FILTERS = {
    "_id",
    "for_sale_by.id",
    "for_sale_by._id",
}


def _cast_numeric_filter_value(field_name: str, raw_value: Any) -> Optional[Any]:
    if field_name in {"bed", "bath"}:
        return parse_optional_int(raw_value)
    return parse_optional_float(raw_value)


def _merge_range_filter(mongo_filters: Dict[str, Any], field_name: str, operator: str, value: Any) -> None:
    existing_value = mongo_filters.get(field_name)
    if isinstance(existing_value, dict):
        existing_value[operator] = value
        return
    if existing_value is None:
        mongo_filters[field_name] = {operator: value}
        return
    mongo_filters[field_name] = {"$eq": existing_value, operator: value}


def to_object_id(value: Any) -> Optional[ObjectId]:
    if isinstance(value, ObjectId):
        return value
    if value is None:
        return None
    try:
        return ObjectId(str(value))
    except Exception:
        return None


def id_match_values(raw_id: Any) -> List[Any]:
    values: List[Any] = []
    oid = to_object_id(raw_id)
    if raw_id is not None:
        values.append(raw_id)
    if oid is not None and oid not in values:
        values.append(oid)
    if raw_id is not None:
        raw_id_str = str(raw_id)
        if raw_id_str not in values:
            values.append(raw_id_str)
    return values


def compact_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


def extract_embed_id(embed: Any) -> Optional[Any]:
    if not isinstance(embed, dict):
        return None
    if embed.get("id") is not None:
        return embed.get("id")
    if embed.get("_id") is not None:
        return embed.get("_id")
    return None


def seller_id_from_house_doc(house_doc: Dict[str, Any]) -> Optional[Any]:
    return extract_embed_id(house_doc.get("for_sale_by"))


def require_user(
    users_collection: Any,
    raw_user_id: Any,
    expected_role: Optional[str] = None,
) -> Dict[str, Any]:
    user_doc = find_by_id(users_collection, raw_user_id)
    if not user_doc:
        raise KeyError("User not found.")

    if expected_role is not None:
        role = str(user_doc.get("role", "")).strip().lower()
        if role != expected_role:
            raise ValueError(f"User must have role '{expected_role}'.")
    return user_doc


def seller_embed_from_user_doc(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    return compact_dict(
        {
            "id": user_doc.get("_id"),
            "full_name": user_doc.get("full_name"),
            "email": user_doc.get("email"),
            "role": user_doc.get("role"),
            "phone": user_doc.get("phone"),
            "city": user_doc.get("city"),
        }
    )


def resolve_seller_embed(payload_for_sale_by: Any, users_collection: Any) -> Dict[str, Any]:
    if not isinstance(payload_for_sale_by, dict):
        raise ValueError("Field 'for_sale_by' must be an object.")

    seller_id = payload_for_sale_by.get("id") or payload_for_sale_by.get("_id")
    if seller_id is None:
        raise ValueError("Field 'for_sale_by.id' is required.")

    seller_doc = require_user(users_collection, seller_id, expected_role="seller")
    return seller_embed_from_user_doc(seller_doc)


def house_embed_from_doc(house_doc: Dict[str, Any]) -> Dict[str, Any]:
    house = House.from_json(house_doc)
    embed = house.to_embed_json()
    embed["id"] = house_doc.get("_id")
    return compact_dict(embed)


def house_to_public_json(house_doc: Dict[str, Any]) -> Dict[str, Any]:
    return House.from_json(house_doc).to_json()


def build_house_filters(raw_filters: Dict[str, Any]) -> Dict[str, Any]:
    mongo_filters: Dict[str, Any] = {}

    for key, value in raw_filters.items():
        if value is None:
            continue

        normalized_key = "_id" if key == "id" else key
        if normalized_key == "zipcode":
            normalized_key = "zip_code"
        normalized_value = value

        if isinstance(normalized_value, str):
            normalized_value = normalized_value.strip()
            if normalized_value == "":
                continue

        if isinstance(normalized_value, dict):
            mongo_filters[normalized_key] = normalized_value
            continue

        if isinstance(normalized_key, str) and normalized_key.startswith("min_"):
            range_field = normalized_key[4:]
            if range_field in _NUMERIC_FILTERS:
                parsed_range_value = _cast_numeric_filter_value(range_field, normalized_value)
                if parsed_range_value is None:
                    return {"_id": {"$exists": False}}
                _merge_range_filter(mongo_filters, range_field, "$gte", parsed_range_value)
                continue
        if isinstance(normalized_key, str) and normalized_key.startswith("max_"):
            range_field = normalized_key[4:]
            if range_field in _NUMERIC_FILTERS:
                parsed_range_value = _cast_numeric_filter_value(range_field, normalized_value)
                if parsed_range_value is None:
                    return {"_id": {"$exists": False}}
                _merge_range_filter(mongo_filters, range_field, "$lte", parsed_range_value)
                continue

        if normalized_key in _OBJECT_ID_FILTERS:
            oid = to_object_id(normalized_value)
            if oid is None:
                return {"_id": {"$exists": False}}
            normalized_value = oid
        elif normalized_key == "is_sold":
            normalized_value = parse_bool(normalized_value)
        elif normalized_key in _NUMERIC_FILTERS:
            parsed_value = _cast_numeric_filter_value(normalized_key, normalized_value)
            if parsed_value is None:
                return {"_id": {"$exists": False}}
            existing_value = mongo_filters.get(normalized_key)
            if isinstance(existing_value, dict):
                existing_value["$eq"] = parsed_value
                continue
            normalized_value = parsed_value

        mongo_filters[normalized_key] = normalized_value

    return mongo_filters


def pull_house_from_all_user_lists(users_collection: Any, house_id: Any) -> None:
    match_values = id_match_values(house_id)
    for field in USER_HOUSE_LIST_FIELDS:
        users_collection.update_many(
            {f"{field}.id": {"$in": match_values}},
            {"$pull": {field: {"id": {"$in": match_values}}}},
        )


def find_buyer_ids_for_house(users_collection: Any, house_id: Any) -> List[Any]:
    match_values = id_match_values(house_id)
    buyer_ids: List[Any] = []
    cursor = users_collection.find(
        {"bought_houses.id": {"$in": match_values}},
        {"_id": 1},
    )
    for doc in cursor:
        buyer_ids.append(doc["_id"])
    return buyer_ids


def find_user_ids_for_house(users_collection: Any, house_id: Any) -> List[Any]:
    match_values = id_match_values(house_id)
    if not match_values:
        return []
    query = {
        "$or": [
            {"for_sale_houses.id": {"$in": match_values}},
            {"sold_houses.id": {"$in": match_values}},
            {"bought_houses.id": {"$in": match_values}},
        ]
    }
    return [doc["_id"] for doc in users_collection.find(query, {"_id": 1})]


def trim_user_house_lists_for_ids(
    users_collection: Any,
    user_ids: Sequence[Any],
) -> None:
    del users_collection
    del user_ids


def sync_house_lists(
    users_collection: Any,
    house_doc: Dict[str, Any],
    buyer_ids: Optional[Sequence[Any]] = None,
) -> None:
    house_id = house_doc.get("_id")
    if house_id is None:
        raise ValueError("House document must include '_id'.")

    seller_id = seller_id_from_house_doc(house_doc)
    if seller_id is None:
        raise ValueError("House document must include 'for_sale_by.id'.")

    seller_doc = require_user(users_collection, seller_id, expected_role="seller")
    pull_house_from_all_user_lists(users_collection, house_id)

    house_embed = house_embed_from_doc(house_doc)
    seller_list = "sold_houses" if bool(house_doc.get("is_sold")) else "for_sale_houses"
    users_collection.update_one(
        {"_id": seller_doc["_id"]},
        {"$push": {seller_list: {"$each": [house_embed], "$position": 0}}},
    )

    if not bool(house_doc.get("is_sold")):
        return

    for buyer_id in buyer_ids or []:
        buyer_doc = require_user(users_collection, buyer_id, expected_role="buyer")
        users_collection.update_one(
            {"_id": buyer_doc["_id"]},
            {"$push": {"bought_houses": {"$each": [house_embed], "$position": 0}}},
        )


def normalize_user_house_list(raw_list: Any) -> List[House]:
    if not isinstance(raw_list, list):
        return []

    houses: List[House] = []
    for item in raw_list:
        if isinstance(item, dict):
            houses.append(House.from_json(item))
        else:
            houses.append(House.from_json({"_id": item}))
    return houses


def read_user_house_list(
    users_collection: Any,
    current_user: User,
    list_name: str,
    limit: Optional[int] = None,
) -> List[House]:
    user_doc = find_by_id(users_collection, current_user.id)
    if not user_doc:
        return []
    normalized = normalize_user_house_list(user_doc.get(list_name))
    if limit is None:
        return normalized
    safe_limit = max(int(limit), 0)
    return normalized[:safe_limit]
