from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.utils.user_model_utils import (
    parse_feedback_embed_list,
    parse_house_list,
)

from .house import House


_ALLOWED_ROLES = {"buyer", "seller", "admin"}


@dataclass
class User:
    id: str
    email: str
    role: str = "buyer"
    full_name: Optional[str] = None
    city: Optional[str] = None
    phone: Optional[str] = None
    for_sale_houses: List[House] = field(default_factory=list)
    sold_houses: List[House] = field(default_factory=list)
    bought_houses: List[House] = field(default_factory=list)
    feedbacks: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "User":
        user_id = data.get("_id") or data.get("id") or ""

        role = str(data.get("role", "buyer")).strip().lower()
        if role not in _ALLOWED_ROLES:
            role = "buyer"

        for_sale_houses = parse_house_list(data.get("for_sale_houses"))
        sold_houses = parse_house_list(data.get("sold_houses"))
        bought_houses = parse_house_list(data.get("bought_houses"))
        feedbacks = parse_feedback_embed_list(data.get("feedbacks"))

        return cls(
            id=str(user_id),
            email=str(data.get("email", "")),
            role=role,
            full_name=data.get("full_name", data.get("name")),
            city=data.get("city"),
            phone=data.get("phone"),
            for_sale_houses=for_sale_houses,
            sold_houses=sold_houses,
            bought_houses=bought_houses,
            feedbacks=feedbacks,
        )

    def to_json(self) -> Dict[str, Any]:
        role = str(self.role).strip().lower()
        if role not in _ALLOWED_ROLES:
            role = "buyer"

        for_sale_houses = [house.to_embed_json() for house in self.for_sale_houses]
        sold_houses = [house.to_embed_json() for house in self.sold_houses]
        bought_houses = [house.to_embed_json() for house in self.bought_houses]
        feedbacks = parse_feedback_embed_list(self.feedbacks)

        data = {
            "_id": self.id if self.id else None,
            "email": self.email,
            "full_name": self.full_name,
            "city": self.city,
            "phone": self.phone,
            "role": role,
            "for_sale_houses": for_sale_houses if for_sale_houses else None,
            "sold_houses": sold_houses if sold_houses else None,
            "bought_houses": bought_houses if bought_houses else None,
            "feedbacks": feedbacks if feedbacks else None,
        }
        return {key: value for key, value in data.items() if value is not None}

    def to_embed_json(self) -> Dict[str, Any]:
        data = {
            "id": self.id,
            "full_name": self.full_name,
            "email": self.email,
            "role": self.role,
            "phone": self.phone,
            "city": self.city,
        }
        return {key: value for key, value in data.items() if value is not None}
