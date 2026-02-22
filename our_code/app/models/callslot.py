from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


_ALLOWED_CALLSLOT_STATUSES = {"requested", "called", "cancelled"}


@dataclass
class Callslot:
    callslot_id: str
    call_datetime: str
    status: str = "requested"
    phone: Optional[str] = None
    buyer_id: Optional[str] = None
    buyer_name: Optional[str] = None
    buyer_email: Optional[str] = None
    seller_id: Optional[str] = None
    seller_name: Optional[str] = None
    seller_email: Optional[str] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Callslot":
        callslot_id = data.get("callslot_id") or data.get("id") or ""
        raw_datetime = data.get("call_datetime") or data.get("datetime") or ""
        if isinstance(raw_datetime, datetime):
            call_datetime = raw_datetime.isoformat()
        else:
            call_datetime = str(raw_datetime) if raw_datetime is not None else ""

        raw_status = str(data.get("status", "requested")).strip().lower()
        status = raw_status if raw_status in _ALLOWED_CALLSLOT_STATUSES else "requested"

        buyer_data = data.get("buyer") if isinstance(data.get("buyer"), dict) else {}
        seller_data = data.get("seller") if isinstance(data.get("seller"), dict) else {}

        return cls(
            callslot_id=str(callslot_id),
            call_datetime=call_datetime,
            status=status,
            phone=data.get("phone"),
            buyer_id=data.get("buyer_id") or buyer_data.get("id") or buyer_data.get("_id"),
            buyer_name=data.get("buyer_name")
            or buyer_data.get("full_name")
            or buyer_data.get("name"),
            buyer_email=data.get("buyer_email") or buyer_data.get("email"),
            seller_id=data.get("seller_id") or seller_data.get("id") or seller_data.get("_id"),
            seller_name=data.get("seller_name")
            or seller_data.get("full_name")
            or seller_data.get("name"),
            seller_email=data.get("seller_email") or seller_data.get("email"),
        )

    def to_json(self) -> Dict[str, Any]:
        data = {
            "callslot_id": self.callslot_id,
            "call_datetime": self.call_datetime,
            "status": self.status,
            "phone": self.phone,
            "buyer_id": self.buyer_id,
            "buyer_name": self.buyer_name,
            "buyer_email": self.buyer_email,
            "seller_id": self.seller_id,
            "seller_name": self.seller_name,
            "seller_email": self.seller_email,
        }
        return {key: value for key, value in data.items() if value is not None}

    def to_embed_json(self) -> Dict[str, Any]:
        buyer = {
            "id": self.buyer_id,
            "full_name": self.buyer_name,
            "email": self.buyer_email,
        }
        seller = {
            "id": self.seller_id,
            "full_name": self.seller_name,
            "email": self.seller_email,
        }

        buyer = {key: value for key, value in buyer.items() if value is not None} or None
        seller = {key: value for key, value in seller.items() if value is not None} or None

        data = {
            "callslot_id": self.callslot_id,
            "call_datetime": self.call_datetime,
            "status": self.status,
            "phone": self.phone,
            "buyer": buyer,
            "seller": seller,
        }
        return {key: value for key, value in data.items() if value is not None}

