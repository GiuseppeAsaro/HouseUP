from dataclasses import dataclass
from typing import Any, Dict, Optional

from bson import ObjectId

from app.utils.house_model_utils import (
    parse_optional_bool,
    parse_optional_float,
    parse_optional_int,
)


@dataclass
class House:
    id: str
    is_sold: bool = False
    price: Optional[float] = None
    bed: Optional[int] = None
    bath: Optional[int] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    house_size: Optional[float] = None
    prev_sold_date: Optional[str] = None
    for_sale_by: Optional[Dict[str, Any]] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "House":
        raw_id = data.get("_id") or data.get("id") or ""
        raw_zip_code = data.get("zip_code") or data.get("zipcode")

        parsed_is_sold = parse_optional_bool(data.get("is_sold"))
        if parsed_is_sold is None:
            status_raw = str(data.get("status", "")).strip().lower()
            parsed_is_sold = status_raw == "sold"

        for_sale_by = data.get("for_sale_by")
        if not isinstance(for_sale_by, dict):
            for_sale_by = None
        else:
            for_sale_by = _normalize_bson_values(for_sale_by)

        return cls(
            id=str(raw_id),
            is_sold=bool(parsed_is_sold),
            price=parse_optional_float(data.get("price")),
            bed=parse_optional_int(data.get("bed")),
            bath=parse_optional_int(data.get("bath")),
            city=data.get("city"),
            state=data.get("state"),
            zip_code=raw_zip_code,
            house_size=parse_optional_float(data.get("house_size")),
            prev_sold_date=data.get("prev_sold_date"),
            for_sale_by=for_sale_by,
        )

    def to_json(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "_id": self.id,
            "is_sold": self.is_sold,
            "price": self.price,
            "bed": self.bed,
            "bath": self.bath,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "house_size": self.house_size,
            "prev_sold_date": self.prev_sold_date,
            "for_sale_by": self.for_sale_by,
        }
        return {key: value for key, value in data.items() if value is not None}

    def to_embed_json(self) -> Dict[str, Any]:
        data = {
            "id": self.id,
            "city": self.city,
            "zip_code": self.zip_code,
        }
        return {key: value for key, value in data.items() if value is not None}


def _normalize_bson_values(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict):
        return {key: _normalize_bson_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_bson_values(item) for item in value]
    return value
