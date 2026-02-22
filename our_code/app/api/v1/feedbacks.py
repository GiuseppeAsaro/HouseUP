from fastapi import APIRouter, Body, Depends, HTTPException, Path, status

from app.api.deps import get_current_user
from app.docs.feedbacks import FeedbackCreateRequest, FeedbackResponse, FeedbackUpdateRequest
from app.models import User
from app.services import feedbacks_service


FEEDBACK_CREATE_EXAMPLES = {
    "buyer_flow": {
        "summary": "Buyer creates feedback",
        "description": "Feedback uses callslot_id as unique interaction key.",
        "value": {
            "callslot_id": "b8b4de2474754ab8a9f65ff8e357c7ac",
            "call_datetime": "2026-03-20T10:30:00Z",
            "rating": 5,
            "comment": "Seller was clear and very responsive.",
            "buyer_full_name": "Giulia Bianchi",
            "buyer_email": "giulia.bianchi@example.com",
            "buyer_phone": "+393401234567",
            "seller_id": "67bc9f6a4b5d2e12cfa4a922",
            "seller_full_name": "Mario Rossi",
            "seller_email": "mario.rossi@example.com",
            "seller_phone": "+390500000001",
        },
    }
}

FEEDBACK_UPDATE_EXAMPLE = {
    "default": {
        "summary": "Update feedback rating/comment",
        "value": {
            "rating": 4,
            "comment": "Good call, some follow-up details still open.",
            "seller_phone": "+390500000009",
        },
    }
}


router = APIRouter(
    prefix="/feedbacks",
    tags=["feedbacks"],
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
    response_model=FeedbackResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create feedback",
    description=(
        "Create buyer feedback without resolving external references. "
        "All embedded buyer/seller fields are provided by payload."
    ),
)
async def create_feedback(
    payload: FeedbackCreateRequest = Body(..., examples=FEEDBACK_CREATE_EXAMPLES),
    current_user: User = Depends(get_current_user),
):
    try:
        feedback = feedbacks_service.create_feedback(
            current_user=current_user,
            payload=payload.dict(exclude_none=True),
        )
        return feedback.to_json()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))


@router.put(
    "/{feedback_id}",
    response_model=FeedbackResponse,
    summary="Update feedback",
    description="Update feedback by id. Buyer can update own feedback; admin can update any feedback.",
)
async def update_feedback(
    feedback_id: str = Path(..., example="b8b4de2474754ab8a9f65ff8e357c7ac"),
    payload: FeedbackUpdateRequest = Body(..., examples=FEEDBACK_UPDATE_EXAMPLE),
    current_user: User = Depends(get_current_user),
):
    try:
        feedback = feedbacks_service.update_feedback(
            current_user=current_user,
            feedback_id=feedback_id,
            payload=payload.dict(exclude_none=True),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=_map_value_error_to_status(str(exc)), detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    if feedback is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feedback with id '{feedback_id}' not found.",
        )
    return feedback.to_json()


@router.delete(
    "/{feedback_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete feedback",
)
async def delete_feedback(
    feedback_id: str = Path(..., example="b8b4de2474754ab8a9f65ff8e357c7ac"),
    current_user: User = Depends(get_current_user),
):
    try:
        deleted = feedbacks_service.delete_feedback(
            current_user=current_user,
            feedback_id=feedback_id,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feedback with id '{feedback_id}' not found.",
        )
    return None
