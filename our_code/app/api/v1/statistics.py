from typing import List

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_current_user
from app.docs.statistics import (
    BookingOutcomeStat,
    FeedbackSatisfactionStat,
    HousingInventoryResponse,
)
from app.models import User
from app.services import statistics_service


router = APIRouter(
    prefix="/statistics",
    tags=["statistics"],
    responses={
        401: {"description": "Authentication required"},
        403: {"description": "Forbidden for this user"},
    },
)


@router.get(
    "/booking-outcomes",
    response_model=List[BookingOutcomeStat],
    summary="Booking outcome and conversion statistics",
)
async def get_booking_outcomes(
    current_user: User = Depends(get_current_user),
):
    try:
        rows = statistics_service.get_booking_outcome_stats(
            current_user=current_user,
        )
        return [row.to_json() for row in rows]
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.get(
    "/feedback-satisfaction",
    response_model=List[FeedbackSatisfactionStat],
    summary="Feedback satisfaction and comment impact statistics",
)
async def get_feedback_satisfaction(
    current_user: User = Depends(get_current_user),
):
    try:
        rows = statistics_service.get_feedback_satisfaction_stats(
            current_user=current_user,
        )
        return [row.to_json() for row in rows]
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.get(
    "/housing-inventory",
    response_model=HousingInventoryResponse,
    summary="Housing market and inventory statistics",
)
async def get_housing_inventory(
    current_user: User = Depends(get_current_user),
):
    try:
        response = statistics_service.get_housing_inventory_stats(
            current_user=current_user,
        )
        return response.to_json()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
