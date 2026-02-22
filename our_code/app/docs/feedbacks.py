from typing import Optional

from pydantic import BaseModel, Field


class FeedbackCreateRequest(BaseModel):
    callslot_id: str = Field(
        ...,
        example="b8b4de2474754ab8a9f65ff8e357c7ac",
        description="Logical interaction id (callslot) used as feedback unique key.",
    )
    call_datetime: str = Field(
        ...,
        example="2026-03-20T10:30:00Z",
        description="Call datetime. Accepted: ISO-8601 or 'YYYY-MM-DD HH:MM'.",
    )
    rating: int = Field(..., ge=1, le=5, example=5)
    comment: Optional[str] = Field(None, example="Seller was clear and very responsive.")
    buyer_full_name: str = Field(..., example="Giulia Bianchi")
    buyer_email: str = Field(..., example="giulia.bianchi@example.com")
    buyer_phone: str = Field(..., example="+393401234567")
    seller_id: str = Field(..., example="67bc9f6a4b5d2e12cfa4a922")
    seller_full_name: str = Field(..., example="Mario Rossi")
    seller_email: str = Field(..., example="mario.rossi@example.com")
    seller_phone: str = Field(..., example="+390500000001")


class FeedbackUpdateRequest(BaseModel):
    call_datetime: Optional[str] = Field(
        None,
        example="2026-03-20T11:00:00Z",
        description="Optional updated call datetime.",
    )
    rating: Optional[int] = Field(None, ge=1, le=5, example=4)
    comment: Optional[str] = Field(
        None,
        example="Conversation was good, waiting for final docs.",
    )
    buyer_id: Optional[str] = Field(None, example="67bc9f6a4b5d2e12cfa4a933")
    buyer_full_name: Optional[str] = Field(None, example="Giulia Bianchi")
    buyer_email: Optional[str] = Field(None, example="giulia.bianchi@example.com")
    buyer_phone: Optional[str] = Field(None, example="+393401234567")
    seller_id: Optional[str] = Field(None, example="67bc9f6a4b5d2e12cfa4a922")
    seller_full_name: Optional[str] = Field(None, example="Mario Rossi")
    seller_email: Optional[str] = Field(None, example="mario.rossi@example.com")
    seller_phone: Optional[str] = Field(None, example="+390500000001")


class FeedbackUserSnapshot(BaseModel):
    id: str
    full_name: str
    email: str
    phone: str


class FeedbackResponse(BaseModel):
    id: str
    feedback_date: str
    callslot_id: str
    call_datetime: str
    rating: int
    comment: Optional[str] = None
    user: FeedbackUserSnapshot
    seller: FeedbackUserSnapshot
