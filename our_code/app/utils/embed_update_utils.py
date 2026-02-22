from typing import Any, Dict, Iterable, List

from bson import ObjectId


def build_id_candidates(raw_id: Any) -> List[Any]:
    if raw_id is None:
        return []
    raw_str = str(raw_id)
    candidates: List[Any] = [raw_str]
    try:
        oid = ObjectId(raw_str)
    except Exception:
        return candidates
    if oid not in candidates:
        candidates.append(oid)
    return candidates


def build_embed_id_filter(prefix: str, candidates: List[Any], field_names: Iterable[str]) -> Dict[str, Any]:
    if not candidates:
        return {}
    return {
        "$or": [
            {f"{prefix}.{field_name}": {"$in": candidates}}
            for field_name in field_names
        ]
    }


def build_embed_updates(prefix: str, update_doc: Dict[str, Any], allowed_fields: Iterable[str]) -> Dict[str, Any]:
    return {
        f"{prefix}.{field_name}": value
        for field_name, value in update_doc.items()
        if field_name in allowed_fields
    }
