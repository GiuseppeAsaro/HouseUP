from typing import Any, Dict, List

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, status

from app.api.deps import get_current_user, require_roles
from app.docs.houses import (
    HouseCreateRequest,
    HouseResponse,
    HouseUpdateRequest,
    HouseUserListResponse,
)
from app.models import User
from app.services import houses_service


HOUSE_CREATE_EXAMPLES = {
    "seller_flow": {
        "summary": "Seller creates a house",
        "description": "Authenticated seller. for_sale_by is inferred from token.",
        "value": {
            "is_sold": False,
            "price": 289000,
            "bed": 3,
            "bath": 2,
            "city": "Pisa",
            "state": "PI",
            "zip_code": "56121",
            "house_size": 1450.0,
            "prev_sold_date": "2025-11-15",
        },
    },
    "admin_flow": {
        "summary": "Admin creates a house for a seller",
        "description": "Authenticated admin. for_sale_by must identify the seller.",
        "value": {
            "is_sold": False,
            "price": 289000,
            "bed": 3,
            "bath": 2,
            "city": "Pisa",
            "state": "PI",
            "zip_code": "56121",
            "house_size": 1450.0,
            "prev_sold_date": "2025-11-15",
            "for_sale_by": {
                "id": "67bc9f6a4b5d2e12cfa4a922",
                "full_name": "Mario Rossi",
                "email": "mario.rossi@example.com",
            },
        },
    },
}

HOUSE_UPDATE_EXAMPLE = {
    "default": {
        "summary": "Update asking price",
        "value": {
            "price": 279000,
            "is_sold": False,
        },
    }
}


router = APIRouter(
    prefix="/houses",
    tags=["houses"],
    responses={
        401: {"description": "Authentication required"},
        403: {"description": "Forbidden for this user"},
    },
)


@router.get(
    "",
    response_model=List[HouseResponse],
    summary="List houses with optional filters",
)
async def list_houses(
    request: Request,
    limit: int = Query(100, ge=0, description="Max number of houses to return.", example=100),
    skip: int = Query(0, ge=0, description="Number of houses to skip.", example=0),
    _current_user: User = Depends(get_current_user),
):
    query_params = dict(request.query_params)
    query_params.pop("limit", None)
    query_params.pop("skip", None)

    filters: Dict[str, Any] = query_params
    houses = houses_service.get_houses(filters=filters or None, limit=limit, skip=skip)
    return [house.to_json() for house in houses]


@router.get(
    "/{house_id}",
    response_model=HouseResponse,
    summary="Get house details by id",
)
async def get_house_detail(
    house_id: str = Path(..., example="67bc9f6a4b5d2e12cfa4a911"),
    _current_user: User = Depends(get_current_user),
):
    house = houses_service.get_house_detail(house_id)
    if house is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"House with id '{house_id}' not found.",
        )
    return house.to_json()


@router.post(
    "",
    response_model=HouseResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new house",
    description="Create a house listing. Seller/admin only.",
)
async def create_house(
    payload: HouseCreateRequest = Body(..., examples=HOUSE_CREATE_EXAMPLES),
    current_user: User = Depends(get_current_user),
):
    try:
        house = houses_service.create_house(
            current_user=current_user,
            payload=payload.dict(by_alias=True, exclude_none=True),
        )
        return house.to_json()
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.put(
    "/{house_id}",
    response_model=HouseResponse,
    summary="Update an existing house",
    description="Update house fields by id. Seller can update owned houses; admin can update any house.",
)
async def update_house(
    house_id: str = Path(..., example="67bc9f6a4b5d2e12cfa4a911"),
    payload: HouseUpdateRequest = Body(..., examples=HOUSE_UPDATE_EXAMPLE),
    current_user: User = Depends(get_current_user),
):
    try:
        house = houses_service.update_house(
            current_user=current_user,
            house_id=house_id,
            payload=payload.dict(by_alias=True, exclude_none=True),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    if house is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"House with id '{house_id}' not found.",
        )
    return house.to_json()


@router.delete(
    "/{house_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a house",
)
async def delete_house(
    house_id: str = Path(..., example="67bc9f6a4b5d2e12cfa4a911"),
    current_user: User = Depends(get_current_user),
):
    try:
        deleted = houses_service.delete_house(current_user=current_user, house_id=house_id)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"House with id '{house_id}' not found.",
        )
    return None


@router.get(
    "/me/for-sale",
    response_model=List[HouseUserListResponse],
    summary="List my for-sale houses",
)
async def get_my_for_sale_houses(
    current_user: User = Depends(require_roles("seller", "admin")),
):
    houses = houses_service.get_my_for_sale_houses(current_user)
    return [house.to_json() for house in houses]


@router.get(
    "/me/sold",
    response_model=List[HouseUserListResponse],
    summary="List my sold houses",
)
async def get_my_sold_houses(
    current_user: User = Depends(require_roles("seller", "admin")),
):
    houses = houses_service.get_my_sold_houses(current_user)
    return [house.to_json() for house in houses]


@router.get(
    "/me/bought",
    response_model=List[HouseUserListResponse],
    summary="List my bought houses",
)
async def get_my_bought_houses(
    current_user: User = Depends(require_roles("buyer", "seller", "admin")),
):
    houses = houses_service.get_my_bought_houses(current_user)
    return [house.to_json() for house in houses]
