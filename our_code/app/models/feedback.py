from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from .user import User


def _to_user_snapshot(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, User):
        user_id = value.id
        full_name = value.full_name
        email = value.email
        phone = value.phone
    elif isinstance(value, dict):
        user_id = value.get("id") or value.get("_id")
        full_name = value.get("full_name") or value.get("name")
        email = value.get("email")
        phone = value.get("phone")
    else:
        return None

    if user_id is None:
        return None
    user_id_str = str(user_id).strip()
    if not user_id_str:
        return None

    snapshot: Dict[str, Any] = {
        "id": user_id_str,
    }
    if full_name is not None:
        full_name_str = str(full_name).strip()
        if full_name_str:
            snapshot["full_name"] = full_name_str
    if email is not None:
        email_str = str(email).strip()
        if email_str:
            snapshot["email"] = email_str
    if phone is not None:
        phone_str = str(phone).strip()
        if phone_str:
            snapshot["phone"] = phone_str
    return snapshot


@dataclass
class Feedback:
    id: str
    feedback_date: datetime
    user: User
    seller: Optional[User] = None
    rating: Optional[int] = None
    comment: Optional[str] = None
    callslot_id: Optional[str] = None
    call_datetime: Optional[str] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Feedback":
        feedback_id = data.get("id") or data.get("_id") or ""

        raw_feedback_date = data.get("feedback_date")
        if isinstance(raw_feedback_date, datetime):
            feedback_date = raw_feedback_date
        elif isinstance(raw_feedback_date, str):
            try:
                feedback_date = datetime.fromisoformat(raw_feedback_date.replace("Z", "+00:00"))
            except ValueError:
                feedback_date = datetime.utcnow()
        else:
            feedback_date = datetime.utcnow()

        raw_rating = data.get("rating")
        rating: Optional[int]
        if raw_rating is None or raw_rating == "":
            rating = None
        else:
            try:
                rating = int(raw_rating)
            except (TypeError, ValueError):
                rating = None

        user_data = data.get("user")
        if not isinstance(user_data, dict):
            user_data = {}

        seller_data = data.get("seller")
        if not isinstance(seller_data, dict):
            seller_data = {}
        seller = User.from_json(seller_data) if seller_data else None

        return cls(
            id=str(feedback_id),
            feedback_date=feedback_date,
            user=User.from_json(user_data),
            seller=seller,
            rating=rating,
            comment=data.get("comment"),
            callslot_id=data.get("callslot_id"),
            call_datetime=data.get("call_datetime"),
        )

    def to_json(self) -> Dict[str, Any]:
        user_snapshot = _to_user_snapshot(self.user)
        seller_snapshot = _to_user_snapshot(self.seller)
        data = {
            "id": self.id,
            "feedback_date": self.feedback_date.isoformat(),
            "user": user_snapshot,
            "seller": seller_snapshot,
            "rating": self.rating,
            "comment": self.comment,
            "callslot_id": self.callslot_id,
            "call_datetime": self.call_datetime,
        }
        return {key: value for key, value in data.items() if value is not None}
