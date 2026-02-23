from typing import Optional

from pydantic import BaseModel, Field


class BookingCreateRequest(BaseModel):
    datetime: str = Field(
        ...,
        example="2026-03-20T10:30:00Z",
        description="Appointment datetime. Accepted: ISO-8601 or 'YYYY-MM-DD HH:MM'.",
    )
    house_id: str = Field(..., example="67bc9f6a4b5d2e12cfa4a911")
    house_city: str = Field(..., example="Pisa")
    house_price: float = Field(..., example=289000.0)
    seller_id: Optional[str] = Field(
        None,
        example="67bc9f6a4b5d2e12cfa4a922",
        description="Required when caller role is buyer/admin.",
    )
    buyer_id: Optional[str] = Field(
        None,
        example="67bc9f6a4b5d2e12cfa4a933",
        description="Required when caller role is seller/admin.",
    )
    seller_name: Optional[str] = Field(None, example="Mario Rossi")
    seller_email: Optional[str] = Field(None, example="mario.rossi@example.com")
    buyer_name: Optional[str] = Field(None, example="Giulia Bianchi")
    buyer_email: Optional[str] = Field(None, example="giulia.bianchi@example.com")


class BookingUpdateRequest(BaseModel):
    new_datetime: str = Field(
        ...,
        example="2026-03-20T11:00:00Z",
        description="New datetime for the same booking, same accepted formats as create.",
    )


class BookingDateQuery(BaseModel):
    date: str = Field(..., example="2026-03-20")


class BookingTargetQuery(BaseModel):
    date: str = Field(..., example="2026-03-20")
    time: str = Field(..., example="10:30")
    with_user_id: str = Field(..., example="67bc9f6a4b5d2e12cfa4a922")
    house_id: str = Field(..., example="67bc9f6a4b5d2e12cfa4a911")


class BookingHouseSnapshot(BaseModel):
    house_id: Optional[str] = None
    house_city: Optional[str] = None
    house_price: Optional[float] = None


class BookingUserSnapshot(BaseModel):
    id: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None


class BookingResponse(BaseModel):
    booking_id: str
    booking_datetime: str
    house: Optional[BookingHouseSnapshot] = None
    buyer: Optional[BookingUserSnapshot] = None
    seller: Optional[BookingUserSnapshot] = None
