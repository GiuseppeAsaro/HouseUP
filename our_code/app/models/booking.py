from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class Booking:
    booking_id: str
    booking_datetime: str
    house_id: Optional[str] = None
    house_city: Optional[str] = None
    house_price: Optional[float] = None
    seller_id: Optional[str] = None
    seller_name: Optional[str] = None
    seller_email: Optional[str] = None
    buyer_id: Optional[str] = None
    buyer_name: Optional[str] = None
    buyer_email: Optional[str] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Booking":
        booking_id = data.get("booking_id") or data.get("id") or ""
        raw_datetime = data.get("booking_datetime") or data.get("datetime") or ""
        if isinstance(raw_datetime, datetime):
            booking_datetime = raw_datetime.isoformat()
        else:
            booking_datetime = str(raw_datetime) if raw_datetime is not None else ""

        house_data = data.get("house") if isinstance(data.get("house"), dict) else {}
        seller_data = data.get("seller") if isinstance(data.get("seller"), dict) else {}
        buyer_data = data.get("buyer") if isinstance(data.get("buyer"), dict) else {}

        return cls(
            booking_id=str(booking_id),
            booking_datetime=booking_datetime,
            house_id=data.get("house_id") or house_data.get("house_id") or house_data.get("id"),
            house_city=data.get("house_city") or house_data.get("house_city") or house_data.get("city"),
            house_price=data.get("house_price") or house_data.get("house_price") or house_data.get("price"),
            seller_id=data.get("seller_id") or seller_data.get("id"),
            seller_name=data.get("seller_name") or seller_data.get("full_name") or seller_data.get("name"),
            seller_email=data.get("seller_email") or seller_data.get("email"),
            buyer_id=data.get("buyer_id") or buyer_data.get("id"),
            buyer_name=data.get("buyer_name") or buyer_data.get("full_name") or buyer_data.get("name"),
            buyer_email=data.get("buyer_email") or buyer_data.get("email"),
        )

    def to_json(self) -> Dict[str, Any]:
        data = {
            "booking_id": self.booking_id,
            "booking_datetime": self.booking_datetime,
            "house_id": self.house_id,
            "house_city": self.house_city,
            "house_price": self.house_price,
            "seller_id": self.seller_id,
            "seller_name": self.seller_name,
            "seller_email": self.seller_email,
            "buyer_id": self.buyer_id,
            "buyer_name": self.buyer_name,
            "buyer_email": self.buyer_email,
        }
        return {key: value for key, value in data.items() if value is not None}

    def to_embed_json(self) -> Dict[str, Any]:
        house = {
            "house_id": self.house_id,
            "house_city": self.house_city,
            "house_price": self.house_price,
        }
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

        house = {key: value for key, value in house.items() if value is not None} or None
        buyer = {key: value for key, value in buyer.items() if value is not None} or None
        seller = {key: value for key, value in seller.items() if value is not None} or None

        data = {
            "booking_id": self.booking_id,
            "booking_datetime": self.booking_datetime,
            "house": house,
            "buyer": buyer,
            "seller": seller,
        }
        return {key: value for key, value in data.items() if value is not None}
