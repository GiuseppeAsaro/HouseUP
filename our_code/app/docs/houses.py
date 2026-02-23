from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class HouseCreateRequest(BaseModel):
    is_sold: Optional[bool] = Field(None, example=False)
    price: Optional[float] = Field(None, example=289000.0)
    bed: Optional[int] = Field(None, example=3)
    bath: Optional[int] = Field(None, example=2)
    city: Optional[str] = Field(None, example="Pisa")
    state: Optional[str] = Field(None, example="PI")
    zip_code: Optional[str] = Field(None, example="56121")
    house_size: Optional[float] = Field(None, example=1450.0)
    prev_sold_date: Optional[str] = Field(None, example="2025-11-15")
    for_sale_by: Optional[Dict[str, Any]] = Field(
        None,
        example={
            "id": "67bc9f6a4b5d2e12cfa4a922",
            "full_name": "Mario Rossi",
            "email": "mario.rossi@example.com",
        },
    )


class HouseUpdateRequest(HouseCreateRequest):
    pass


class HouseResponse(BaseModel):
    id: str = Field(..., alias="_id")
    is_sold: bool = False
    price: Optional[float] = None
    bed: Optional[int] = None
    bath: Optional[int] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    house_size: Optional[float] = None
    prev_sold_date: Optional[str] = None
    for_sale_by: Optional[Dict[str, Any]] = None


class HouseUserListResponse(BaseModel):
    id: str = Field(..., alias="_id")
    price: Optional[float] = None
    bed: Optional[int] = None
    bath: Optional[int] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    house_size: Optional[float] = None
    prev_sold_date: Optional[str] = None
    for_sale_by: Optional[Dict[str, Any]] = None
