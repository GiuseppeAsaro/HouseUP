from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


def _normalize_datetime_to_iso(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    else:
        return ""

    if dt.tzinfo is None:
        return dt.isoformat(timespec="minutes") + "Z"
    return dt.isoformat(timespec="minutes").replace("+00:00", "Z")


@dataclass
class BookingResult:
    booking_result_id: str
    booking_date: str
    house_bought: bool
    final_price: Optional[float] = None
    house_id: Optional[str] = None
    house_listing_price: Optional[float] = None
    house_city: Optional[str] = None
    house_zip_code: Optional[str] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "BookingResult":
        raw_booking_result_id = data.get("booking_result_id") or data.get("id") or data.get("_id") or ""
        booking_result_id = str(raw_booking_result_id).strip()

        booking_date = _normalize_datetime_to_iso(data.get("booking_date"))

        raw_house_bought = data.get("house_bought")
        if isinstance(raw_house_bought, bool):
            house_bought = raw_house_bought
        elif isinstance(raw_house_bought, (int, float)):
            house_bought = bool(raw_house_bought)
        elif isinstance(raw_house_bought, str):
            house_bought = raw_house_bought.strip().lower() in {"true", "1", "yes"}
        else:
            house_bought = False

        raw_final_price = data.get("final_price")
        if raw_final_price is None or raw_final_price == "":
            final_price = None
        else:
            try:
                final_price = float(raw_final_price)
            except (TypeError, ValueError):
                final_price = None
        if not house_bought:
            final_price = None

        house_data = data.get("house")
        if not isinstance(house_data, dict):
            house_data = {}

        raw_house_listing_price = house_data.get("listing_price")
        if raw_house_listing_price is None or raw_house_listing_price == "":
            raw_house_listing_price = house_data.get("price")
        if raw_house_listing_price is None or raw_house_listing_price == "":
            house_listing_price = None
        else:
            try:
                house_listing_price = float(raw_house_listing_price)
            except (TypeError, ValueError):
                house_listing_price = None

        raw_house_id = house_data.get("id") or house_data.get("_id")
        house_id = str(raw_house_id).strip() if raw_house_id is not None else None
        if house_id == "":
            house_id = None

        raw_house_city = house_data.get("city")
        house_city = str(raw_house_city).strip() if raw_house_city is not None else None
        if house_city == "":
            house_city = None

        raw_house_zip_code = house_data.get("zip_code")
        house_zip_code = str(raw_house_zip_code).strip() if raw_house_zip_code is not None else None
        if house_zip_code == "":
            house_zip_code = None

        return cls(
            booking_result_id=booking_result_id,
            booking_date=booking_date,
            house_bought=house_bought,
            final_price=final_price,
            house_id=house_id,
            house_listing_price=house_listing_price,
            house_city=house_city,
            house_zip_code=house_zip_code,
        )

    def to_json(self) -> Dict[str, Any]:
        house_payload = {
            "id": self.house_id,
            "listing_price": self.house_listing_price,
            "city": self.house_city,
            "zip_code": self.house_zip_code,
        }
        house_payload = {key: value for key, value in house_payload.items() if value is not None}

        data = {
            "booking_result_id": self.booking_result_id,
            "booking_date": self.booking_date,
            "house_bought": self.house_bought,
            "final_price": self.final_price if self.house_bought else None,
            "house": house_payload if house_payload else None,
        }
        return {key: value for key, value in data.items() if value is not None}
