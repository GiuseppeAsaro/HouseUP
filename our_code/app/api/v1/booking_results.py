from typing import Any, Dict, List

from fastapi import APIRouter, Body, Depends, HTTPException, Path, status

from app.api.deps import get_current_user
from app.docs.booking_results import (
    BookingResultCreateRequest,
    BookingResultResponse,
    BookingResultUpdateRequest,
)
from app.models import User
from app.services import booking_results_service


BOOKING_RESULT_CREATE_EXAMPLES = {
    "seller_flow": {
        "summary": "Seller creates booking result",
        "description": "Authenticated seller; seller snapshot is derived from token.",
        "value": {
            "booking_id": "42a3b8d123884bdba489c91edc0b3389",
            "booking_datetime": "2026-03-20T10:30:00Z",
            "house_bought": True,
            "final_price": 279000,
            "house_id": "67bc9f6a4b5d2e12cfa4a911",
            "house_listing_price": 285000,
            "house_city": "Pisa",
            "house_zip_code": "56121",
        },
    },
    "admin_flow": {
        "summary": "Admin creates booking result",
        "description": "Authenticated admin; seller snapshot is required.",
        "value": {
            "booking_id": "42a3b8d123884bdba489c91edc0b3389",
            "booking_datetime": "2026-03-20T10:30:00Z",
            "seller": {
                "id": "67bc9f6a4b5d2e12cfa4a922",
                "full_name": "Mario Rossi",
                "email": "mario.rossi@example.com",
            },
            "house_bought": False,
            "house_id": "67bc9f6a4b5d2e12cfa4a911",
            "house_listing_price": 285000,
            "house_city": "Pisa",
            "house_zip_code": "56121",
        },
    },
}

BOOKING_RESULT_UPDATE_EXAMPLE = {
    "default": {
        "summary": "Update booking result outcome",
        "value": {
            "house_bought": True,
            "final_price": 276000,
            "house_listing_price": 285000,
            "house_city": "Pisa",
            "house_zip_code": "56121",
            "seller": {
                "id": "67bc9f6a4b5d2e12cfa4a922",
                "full_name": "Mario Rossi",
                "email": "mario.rossi@example.com",
            },
        },
    }
}


router = APIRouter(
    prefix="/booking-results",
    tags=["booking-results"],
    responses={
        401: {"description": "Authentication required"},
        403: {"description": "Forbidden for this user"},
    },
)


def _map_value_error_to_status(detail: str) -> int:
    if "already exists" in detail.lower():
        return status.HTTP_409_CONFLICT
    return status.HTTP_400_BAD_REQUEST


@router.post(
    "",
    response_model=BookingResultResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create booking result",
    description=(
        "Create booking result. "
        "All required house and outcome fields are provided by payload; "
        "seller snapshot is auto-derived for seller callers and required for admin callers."
    ),
)
async def create_booking_result(
    payload: BookingResultCreateRequest = Body(..., examples=BOOKING_RESULT_CREATE_EXAMPLES),
    current_user: User = Depends(get_current_user),
):
    try:
        booking_result = booking_results_service.create_booking_result(
            current_user=current_user,
            payload=payload.dict(exclude_none=True),
        )
        return booking_result.to_json()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.get(
    "/me",
    response_model=List[Dict[str, Any]],
    summary="List my booking-result snapshots",
    description="Read booking-result snapshots from authenticated user profile only.",
)
async def list_my_booking_results(
    current_user: User = Depends(get_current_user),
):
    try:
        return booking_results_service.list_booking_results_from_user_list(current_user)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.put(
    "/{booking_result_id}",
    response_model=BookingResultResponse,
    summary="Update booking result",
    description="Update a booking result by id. Seller can update own records; admin can update any record.",
)
async def update_booking_result(
    booking_result_id: str = Path(..., example="42a3b8d123884bdba489c91edc0b3389"),
    payload: BookingResultUpdateRequest = Body(..., examples=BOOKING_RESULT_UPDATE_EXAMPLE),
    current_user: User = Depends(get_current_user),
):
    try:
        booking_result = booking_results_service.update_booking_result(
            current_user=current_user,
            booking_result_id=booking_result_id,
            payload=payload.dict(exclude_none=True),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    if booking_result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Booking result with id '{booking_result_id}' not found.",
        )
    return booking_result.to_json()


@router.delete(
    "/{booking_result_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete booking result",
)
async def delete_booking_result(
    booking_result_id: str = Path(..., example="42a3b8d123884bdba489c91edc0b3389"),
    current_user: User = Depends(get_current_user),
):
    try:
        deleted = booking_results_service.delete_booking_result(
            current_user=current_user,
            booking_result_id=booking_result_id,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Booking result with id '{booking_result_id}' not found.",
        )
    return None
