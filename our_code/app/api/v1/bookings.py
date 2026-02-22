from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from app.api.deps import get_current_user
from app.docs.bookings import BookingCreateRequest, BookingResponse, BookingUpdateRequest
from app.models import User
from app.services import bookings_service


BOOKING_CREATE_EXAMPLES = {
    "buyer_flow": {
        "summary": "Buyer creates booking",
        "description": "Authenticated buyer; seller_id is required.",
        "value": {
            "datetime": "2026-03-20T10:30:00Z",
            "house_id": "67bc9f6a4b5d2e12cfa4a911",
            "house_city": "Pisa",
            "house_price": 289000,
            "seller_id": "67bc9f6a4b5d2e12cfa4a922",
            "seller_name": "Mario Rossi",
            "seller_email": "mario.rossi@example.com",
        },
    },
    "seller_flow": {
        "summary": "Seller creates booking",
        "description": "Authenticated seller; buyer_id is required.",
        "value": {
            "datetime": "2026-03-20T10:30:00Z",
            "house_id": "67bc9f6a4b5d2e12cfa4a911",
            "house_city": "Pisa",
            "house_price": 289000,
            "buyer_id": "67bc9f6a4b5d2e12cfa4a933",
            "buyer_name": "Giulia Bianchi",
            "buyer_email": "giulia.bianchi@example.com",
        },
    },
    "admin_flow": {
        "summary": "Admin creates booking",
        "description": "Authenticated admin; buyer_id and seller_id are required.",
        "value": {
            "datetime": "2026-03-20T10:30:00Z",
            "house_id": "67bc9f6a4b5d2e12cfa4a911",
            "house_city": "Pisa",
            "house_price": 289000,
            "buyer_id": "67bc9f6a4b5d2e12cfa4a933",
            "seller_id": "67bc9f6a4b5d2e12cfa4a922",
        },
    },
}

BOOKING_UPDATE_EXAMPLE = {
    "default": {
        "summary": "Move booking to a new slot",
        "value": {"new_datetime": "2026-03-20T11:00:00Z"},
    }
}


router = APIRouter(
    prefix="/bookings",
    tags=["bookings"],
    responses={
        401: {"description": "Authentication required"},
        403: {"description": "Forbidden for this user"},
    },
)


def _map_value_error_to_status(detail: str) -> int:
    lowered = detail.lower()
    if "already booked" in lowered:
        return status.HTTP_409_CONFLICT
    return status.HTTP_400_BAD_REQUEST


@router.post(
    "",
    response_model=BookingResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a booking",
    description=(
        "Create a booking and persist it in both user namespaces in Redis. "
        "Required identity fields depend on caller role: buyer->seller_id, seller->buyer_id, "
        "admin->buyer_id+seller_id."
    ),
)
async def create_booking(
    payload: BookingCreateRequest = Body(..., examples=BOOKING_CREATE_EXAMPLES),
    current_user: User = Depends(get_current_user),
):
    try:
        booking = bookings_service.create_booking(
            current_user=current_user,
            payload=payload.dict(exclude_none=True),
        )
        return booking.to_embed_json()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.get(
    "",
    response_model=List[BookingResponse],
    summary="List bookings",
    description=(
        "Returns bookings visible to the authenticated user. "
        "If 'date' is provided, results are restricted to that day."
    ),
)
async def list_bookings(
    date: Optional[str] = Query(
        None,
        description="Optional date in YYYY-MM-DD format.",
        example="2026-03-20",
    ),
    current_user: User = Depends(get_current_user),
):
    try:
        bookings = bookings_service.list_bookings(current_user, date=date)
        return [booking.to_embed_json() for booking in bookings]
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.put(
    "",
    response_model=BookingResponse,
    summary="Update booking datetime",
    description="Find a booking by operational key (date/time/with_user_id/house_id) and move it to a new datetime.",
)
async def update_booking(
    payload: BookingUpdateRequest = Body(..., examples=BOOKING_UPDATE_EXAMPLE),
    date: str = Query(..., description="Date in YYYY-MM-DD format.", example="2026-03-20"),
    time: str = Query(..., description="Time in HH:MM format.", example="10:30"),
    with_user_id: str = Query(..., description="Counterparty user id.", example="67bc9f6a4b5d2e12cfa4a922"),
    house_id: str = Query(..., description="House id.", example="67bc9f6a4b5d2e12cfa4a911"),
    current_user: User = Depends(get_current_user),
):
    try:
        booking = bookings_service.update_booking(
            current_user=current_user,
            date=date,
            time=time,
            with_user_id=with_user_id,
            house_id=house_id,
            new_datetime=payload.new_datetime,
        )
        return booking.to_embed_json()
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a booking",
    description="Delete both mirrored booking keys (current user + counterparty) by operational key.",
)
async def delete_booking(
    date: str = Query(..., description="Date in YYYY-MM-DD format.", example="2026-03-20"),
    time: str = Query(..., description="Time in HH:MM format.", example="10:30"),
    with_user_id: str = Query(..., description="Counterparty user id.", example="67bc9f6a4b5d2e12cfa4a922"),
    house_id: str = Query(..., description="House id.", example="67bc9f6a4b5d2e12cfa4a911"),
    current_user: User = Depends(get_current_user),
):
    try:
        deleted = bookings_service.delete_booking(
            current_user=current_user,
            date=date,
            time=time,
            with_user_id=with_user_id,
            house_id=house_id,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Booking not found.",
        )

    return None
