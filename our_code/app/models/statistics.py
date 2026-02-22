from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.utils.statistics_model_utils import parse_optional_float, parse_optional_int


@dataclass
class BookingOutcomeStatRow:
    period: str
    outcome: str
    total_bookings: int
    weighted_score: int
    paired_sample_size: int
    final_price_avg: Optional[float] = None
    final_price_min: Optional[float] = None
    final_price_max: Optional[float] = None
    listing_price_avg: Optional[float] = None
    final_vs_listing_amount: Optional[float] = None
    final_vs_listing_pct: Optional[float] = None
    final_vs_listing_direction: Optional[str] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "BookingOutcomeStatRow":
        return cls(
            period=str(data.get("period") or "unknown"),
            outcome=str(data.get("outcome") or "unknown"),
            total_bookings=int(parse_optional_int(data.get("total_bookings")) or 0),
            weighted_score=int(parse_optional_int(data.get("weighted_score")) or 0),
            paired_sample_size=int(parse_optional_int(data.get("paired_sample_size")) or 0),
            final_price_avg=parse_optional_float(data.get("final_price_avg")),
            final_price_min=parse_optional_float(data.get("final_price_min")),
            final_price_max=parse_optional_float(data.get("final_price_max")),
            listing_price_avg=parse_optional_float(data.get("listing_price_avg")),
            final_vs_listing_amount=parse_optional_float(data.get("final_vs_listing_amount")),
            final_vs_listing_pct=parse_optional_float(data.get("final_vs_listing_pct")),
            final_vs_listing_direction=(
                str(data.get("final_vs_listing_direction")).strip().lower()
                if data.get("final_vs_listing_direction") is not None
                else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "period": self.period,
            "outcome": self.outcome,
            "total_bookings": int(self.total_bookings),
            "weighted_score": int(self.weighted_score),
            "paired_sample_size": int(self.paired_sample_size),
            "final_price_avg": self.final_price_avg,
            "final_price_min": self.final_price_min,
            "final_price_max": self.final_price_max,
            "listing_price_avg": self.listing_price_avg,
            "final_vs_listing_amount": self.final_vs_listing_amount,
            "final_vs_listing_pct": self.final_vs_listing_pct,
            "final_vs_listing_direction": self.final_vs_listing_direction,
        }
        return {k: v for k, v in data.items() if v is not None}


@dataclass
class FeedbackSatisfactionStatRow:
    period: str
    category: str
    total_feedback: int
    feedback_with_comment: int
    feedback_without_comment: int
    average_rating: Optional[float] = None
    total_weighted_score: int = 0

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "FeedbackSatisfactionStatRow":
        return cls(
            period=str(data.get("period") or "unknown"),
            category=str(data.get("category") or "Invalid"),
            total_feedback=int(parse_optional_int(data.get("total_feedback")) or 0),
            feedback_with_comment=int(parse_optional_int(data.get("feedback_with_comment")) or 0),
            feedback_without_comment=int(
                parse_optional_int(data.get("feedback_without_comment")) or 0
            ),
            average_rating=parse_optional_float(data.get("average_rating")),
            total_weighted_score=int(parse_optional_int(data.get("total_weighted_score")) or 0),
        )

    def to_json(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "period": self.period,
            "category": self.category,
            "total_feedback": int(self.total_feedback),
            "feedback_with_comment": int(self.feedback_with_comment),
            "feedback_without_comment": int(self.feedback_without_comment),
            "average_rating": self.average_rating,
            "total_weighted_score": int(self.total_weighted_score),
        }
        return {k: v for k, v in data.items() if v is not None}


@dataclass
class HousingInventoryStatRow:
    price_category: str
    total_houses: int
    available_houses: int
    unavailable_houses: int
    average_price: Optional[float] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "HousingInventoryStatRow":
        return cls(
            price_category=str(data.get("price_category") or "Unknown"),
            total_houses=int(parse_optional_int(data.get("total_houses")) or 0),
            available_houses=int(parse_optional_int(data.get("available_houses")) or 0),
            unavailable_houses=int(parse_optional_int(data.get("unavailable_houses")) or 0),
            average_price=parse_optional_float(data.get("average_price")),
            min_price=parse_optional_float(data.get("min_price")),
            max_price=parse_optional_float(data.get("max_price")),
        )

    def to_json(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "price_category": self.price_category,
            "total_houses": int(self.total_houses),
            "available_houses": int(self.available_houses),
            "unavailable_houses": int(self.unavailable_houses),
            "average_price": self.average_price,
            "min_price": self.min_price,
            "max_price": self.max_price,
        }
        return {k: v for k, v in data.items() if v is not None}


@dataclass
class HousingInventoryResponse:
    data: List[HousingInventoryStatRow]
    note: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"data": [row.to_json() for row in self.data]}
        if self.note is not None:
            payload["note"] = self.note
        return payload
