from typing import Literal, Optional

from pydantic import BaseModel, Field


class CallslotCreateRequest(BaseModel):
    datetime: str = Field(
        ...,
        example="2026-03-20T10:30:00Z",
        description="Call datetime. Accepted: ISO-8601 or 'YYYY-MM-DD HH:MM'.",
    )
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
    phone: Optional[str] = Field(
        None,
        example="+393401234567",
        description="Required for seller/admin. Buyer can omit if phone exists in profile.",
    )
    buyer_name: Optional[str] = Field(None, example="Giulia Bianchi")
    buyer_email: Optional[str] = Field(None, example="giulia.bianchi@example.com")
    seller_name: Optional[str] = Field(None, example="Mario Rossi")
    seller_email: Optional[str] = Field(None, example="mario.rossi@example.com")


class CallslotUpdateRequest(BaseModel):
    new_datetime: Optional[str] = Field(
        None,
        example="2026-03-20T11:00:00Z",
        description="New call datetime, optional.",
    )
    phone: Optional[str] = Field(None, example="+393409998887", description="Optional phone update.")


class CallslotStatusUpdateRequest(BaseModel):
    status: Literal["requested", "called", "cancelled"] = Field(..., example="called")


class CallslotDateQuery(BaseModel):
    date: str = Field(..., example="2026-03-20")


class CallslotTargetQuery(BaseModel):
    date: str = Field(..., example="2026-03-20")
    time: str = Field(..., example="10:30")
    with_user_id: str = Field(..., example="67bc9f6a4b5d2e12cfa4a922")


class CallslotUserSnapshot(BaseModel):
    id: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None


class CallslotResponse(BaseModel):
    callslot_id: str
    call_datetime: str
    status: Literal["requested", "called", "cancelled"]
    phone: Optional[str] = None
    buyer: Optional[CallslotUserSnapshot] = None
    seller: Optional[CallslotUserSnapshot] = None
