from typing import Optional

from pydantic import BaseModel, Field


class BookingResultSellerSnapshot(BaseModel):
    id: str
    full_name: Optional[str] = None
    email: Optional[str] = None


class BookingResultCreateRequest(BaseModel):
    booking_id: str = Field(
        ...,
        example="42a3b8d123884bdba489c91edc0b3389",
        description="Logical interaction id (booking) used as booking-result unique key.",
    )
    booking_datetime: str = Field(
        ...,
        example="2026-03-20T10:30:00Z",
        description="Booking datetime. Accepted: ISO-8601 or 'YYYY-MM-DD HH:MM'.",
    )
    house_bought: bool = Field(..., example=True)
    final_price: Optional[float] = Field(
        None,
        example=279000.0,
        description="Required when house_bought is true. Ignored otherwise.",
    )
    house_id: str = Field(..., example="67bc9f6a4b5d2e12cfa4a911")
    house_listing_price: float = Field(..., example=285000.0)
    house_city: str = Field(..., example="Pisa")
    house_zip_code: str = Field(..., example="56121")
    seller: Optional[BookingResultSellerSnapshot] = Field(
        None,
        description="Required only for admin callers. Ignored for seller callers.",
    )


class BookingResultUpdateRequest(BaseModel):
    booking_datetime: Optional[str] = Field(
        None,
        example="2026-03-20T11:15:00Z",
        description="Optional updated booking datetime.",
    )
    house_bought: Optional[bool] = Field(None, example=True)
    final_price: Optional[float] = Field(
        None,
        example=279000.0,
        description="When resulting house_bought is true, final_price must be present or already stored.",
    )
    house_id: Optional[str] = Field(None, example="67bc9f6a4b5d2e12cfa4a911")
    house_listing_price: Optional[float] = Field(None, example=285000.0)
    house_city: Optional[str] = Field(None, example="Pisa")
    house_zip_code: Optional[str] = Field(None, example="56121")
    seller: Optional[BookingResultSellerSnapshot] = Field(
        None,
        description="Admin-only seller reassignment.",
    )


class BookingResultHouseSnapshot(BaseModel):
    id: str
    listing_price: float
    city: str
    zip_code: str


class BookingResultResponse(BaseModel):
    booking_result_id: str
    booking_date: str
    house_bought: bool
    final_price: Optional[float] = None
    house: BookingResultHouseSnapshot
    seller: BookingResultSellerSnapshot
