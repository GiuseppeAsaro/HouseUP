from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from app.api.deps import get_current_user
from app.docs.callslots import (
    CallslotCreateRequest,
    CallslotResponse,
    CallslotStatusUpdateRequest,
    CallslotUpdateRequest,
)
from app.models import User
from app.services import callslots_service


CALLSLOT_CREATE_EXAMPLES = {
    "buyer_flow": {
        "summary": "Buyer creates callslot",
        "description": "Authenticated buyer; seller_id required. Phone can be omitted if user profile has phone.",
        "value": {
            "datetime": "2026-03-20T10:30:00Z",
            "seller_id": "67bc9f6a4b5d2e12cfa4a922",
            "phone": "+393401234567",
            "seller_name": "Mario Rossi",
            "seller_email": "mario.rossi@example.com",
        },
    },
    "seller_flow": {
        "summary": "Seller creates callslot",
        "description": "Authenticated seller; buyer_id and phone are required.",
        "value": {
            "datetime": "2026-03-20T10:30:00Z",
            "buyer_id": "67bc9f6a4b5d2e12cfa4a933",
            "phone": "+393401234567",
            "buyer_name": "Giulia Bianchi",
            "buyer_email": "giulia.bianchi@example.com",
        },
    },
    "admin_flow": {
        "summary": "Admin creates callslot",
        "description": "Authenticated admin; buyer_id, seller_id and phone are required.",
        "value": {
            "datetime": "2026-03-20T10:30:00Z",
            "buyer_id": "67bc9f6a4b5d2e12cfa4a933",
            "seller_id": "67bc9f6a4b5d2e12cfa4a922",
            "phone": "+393401234567",
        },
    },
}

CALLSLOT_UPDATE_EXAMPLES = {
    "move_and_change_phone": {
        "summary": "Move slot and update phone",
        "value": {
            "new_datetime": "2026-03-20T11:00:00Z",
            "phone": "+393409998887",
        },
    },
    "phone_only": {
        "summary": "Update phone only",
        "value": {
            "phone": "+393409998887",
        },
    },
}

CALLSLOT_STATUS_EXAMPLE = {
    "mark_called": {
        "summary": "Mark callslot as called",
        "value": {"status": "called"},
    }
}


router = APIRouter(
    prefix="/callslots",
    tags=["callslots"],
    responses={
        401: {"description": "Authentication required"},
        403: {"description": "Forbidden for this user"},
    },
)


def _map_value_error_to_status(detail: str) -> int:
    lowered = detail.lower()
    if (
        "already booked" in lowered
        or "only requested" in lowered
        or "transition not allowed" in lowered
        or "invalid callslot status transition" in lowered
    ):
        return status.HTTP_409_CONFLICT
    return status.HTTP_400_BAD_REQUEST


@router.post(
    "",
    response_model=CallslotResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a callslot",
    description=(
        "Create a callslot and persist mirrored keys in Redis. "
        "Required identity fields depend on caller role: buyer->seller_id, seller->buyer_id+phone, "
        "admin->buyer_id+seller_id+phone."
    ),
)
async def create_callslot(
    payload: CallslotCreateRequest = Body(..., examples=CALLSLOT_CREATE_EXAMPLES),
    current_user: User = Depends(get_current_user),
):
    try:
        callslot = callslots_service.create_callslot(
            current_user=current_user,
            payload=payload.dict(exclude_none=True),
        )
        return callslot.to_embed_json()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.get(
    "",
    response_model=List[CallslotResponse],
    summary="List callslots",
    description=(
        "Returns callslots visible to the authenticated user. "
        "If 'date' is provided, results are restricted to that day."
    ),
)
async def list_callslots(
    date: Optional[str] = Query(
        None,
        description="Optional date in YYYY-MM-DD format.",
        example="2026-03-20",
    ),
    current_user: User = Depends(get_current_user),
):
    try:
        callslots = callslots_service.list_callslots(current_user, date=date)
        return [callslot.to_embed_json() for callslot in callslots]
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.put(
    "",
    response_model=CallslotResponse,
    summary="Update callslot datetime/phone",
    description="Find a callslot by operational key (date/time/with_user_id) and update datetime and/or phone.",
)
async def update_callslot(
    payload: CallslotUpdateRequest = Body(..., examples=CALLSLOT_UPDATE_EXAMPLES),
    date: str = Query(..., description="Date in YYYY-MM-DD format.", example="2026-03-20"),
    time: str = Query(..., description="Time in HH:MM format.", example="10:30"),
    with_user_id: str = Query(..., description="Counterparty user id.", example="67bc9f6a4b5d2e12cfa4a922"),
    current_user: User = Depends(get_current_user),
):
    try:
        callslot = callslots_service.update_callslot(
            current_user=current_user,
            date=date,
            time=time,
            with_user_id=with_user_id,
            new_datetime=payload.new_datetime,
            phone=payload.phone,
        )
        return callslot.to_embed_json()
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.patch(
    "/status",
    response_model=CallslotResponse,
    summary="Update callslot status",
    description="Update callslot status by operational key.",
)
async def update_callslot_status(
    payload: CallslotStatusUpdateRequest = Body(..., examples=CALLSLOT_STATUS_EXAMPLE),
    date: str = Query(..., description="Date in YYYY-MM-DD format.", example="2026-03-20"),
    time: str = Query(..., description="Time in HH:MM format.", example="10:30"),
    with_user_id: str = Query(..., description="Counterparty user id.", example="67bc9f6a4b5d2e12cfa4a922"),
    current_user: User = Depends(get_current_user),
):
    try:
        callslot = callslots_service.update_callslot_status(
            current_user=current_user,
            date=date,
            time=time,
            with_user_id=with_user_id,
            status=payload.status,
        )
        return callslot.to_embed_json()
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
    summary="Delete a callslot",
    description="Delete both mirrored callslot keys (current user + counterparty) by operational key.",
)
async def delete_callslot(
    date: str = Query(..., description="Date in YYYY-MM-DD format.", example="2026-03-20"),
    time: str = Query(..., description="Time in HH:MM format.", example="10:30"),
    with_user_id: str = Query(..., description="Counterparty user id.", example="67bc9f6a4b5d2e12cfa4a922"),
    current_user: User = Depends(get_current_user),
):
    try:
        deleted = callslots_service.delete_callslot(
            current_user=current_user,
            date=date,
            time=time,
            with_user_id=with_user_id,
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
            detail="Callslot not found.",
        )

    return None
