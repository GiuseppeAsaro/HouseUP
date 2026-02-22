from typing import Any, Dict, List, Optional

from bson import ObjectId


def find_by_id(collection, raw_id: Any) -> Optional[Dict[str, Any]]:
    if raw_id is None:
        return None
    doc = collection.find_one({"_id": raw_id})
    if doc is not None:
        return doc
    try:
        oid = ObjectId(str(raw_id))
    except Exception:
        return None
    return collection.find_one({"_id": oid})


def id_match_values(raw_id: Any) -> List[Any]:
    values: List[Any] = []
    if raw_id is not None:
        values.append(raw_id)
    try:
        oid = ObjectId(str(raw_id))
    except Exception:
        oid = None
    if oid is not None and oid not in values:
        values.append(oid)
    if raw_id is not None:
        raw_str = str(raw_id)
        if raw_str not in values:
            values.append(raw_str)
    return values


def build_id_filter(raw_id: Any) -> Dict[str, Any]:
    values = id_match_values(raw_id)
    if len(values) <= 1:
        return {"_id": values[0] if values else raw_id}
    return {"_id": {"$in": values}}
