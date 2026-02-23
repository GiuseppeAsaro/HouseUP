from typing import List, Optional

from pydantic import BaseModel, Field


class BookingOutcomeStat(BaseModel):
    period: str = Field(..., example="2026-03")
    outcome: str = Field(..., example="confirmed")
    total_bookings: int = Field(..., example=42)
    weighted_score: int = Field(..., example=38)
    paired_sample_size: int = Field(..., example=40)
    final_price_avg: Optional[float] = Field(None, example=281500.0)
    final_price_min: Optional[float] = Field(None, example=199000.0)
    final_price_max: Optional[float] = Field(None, example=395000.0)
    listing_price_avg: Optional[float] = Field(None, example=289200.0)
    # Positive gap values; direction tells whether final is below/above listing.
    final_vs_listing_amount: Optional[float] = Field(None, example=7700.0)
    final_vs_listing_pct: Optional[float] = Field(None, example=2.66)
    # One of: below_listing, above_listing, equal, unknown
    final_vs_listing_direction: Optional[str] = Field(None, example="below_listing")


class FeedbackSatisfactionStat(BaseModel):
    period: str = Field(..., example="2026-03")
    category: str = Field(..., example="seller_support")
    total_feedback: int = Field(..., example=18)
    feedback_with_comment: int = Field(..., example=11)
    feedback_without_comment: int = Field(..., example=7)
    average_rating: Optional[float] = Field(None, example=4.4)
    total_weighted_score: int = Field(..., example=79)


class HousingInventoryStat(BaseModel):
    price_category: str = Field(..., example="Budget")
    total_houses: int = Field(..., example=125)
    available_houses: int = Field(..., example=92)
    unavailable_houses: int = Field(..., example=33)
    average_price: Optional[float] = Field(None, example=254800.0)
    min_price: Optional[float] = Field(None, example=200000.0)
    max_price: Optional[float] = Field(None, example=299900.0)


class HousingInventoryResponse(BaseModel):
    data: List[HousingInventoryStat]
    note: Optional[str] = Field(None, example="Grouped by price buckets for current catalog.")
