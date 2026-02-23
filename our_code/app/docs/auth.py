from typing import Optional

from pydantic import BaseModel, Field


class UserSnapshot(BaseModel):
    id: str
    email: str
    role: str
    full_name: Optional[str] = None
    city: Optional[str] = None
    phone: Optional[str] = None


class AuthRegisterRequest(BaseModel):
    email: str = Field(..., example="giulia.bianchi@example.com")
    password: str = Field(..., example="Str0ngPass!23")
    full_name: Optional[str] = Field(None, example="Giulia Bianchi")


class AuthLoginRequest(BaseModel):
    email: str = Field(..., example="giulia.bianchi@example.com")
    password: str = Field(..., example="Str0ngPass!23")


class AuthRegisterResponse(UserSnapshot):
    pass


class AuthLoginResponse(BaseModel):
    user: UserSnapshot
    access_token: str
    token_type: str
    detail: str
