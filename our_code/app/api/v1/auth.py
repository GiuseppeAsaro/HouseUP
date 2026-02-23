from fastapi import APIRouter, Body, HTTPException, status

from app.docs.auth import (
    AuthLoginRequest,
    AuthLoginResponse,
    AuthRegisterRequest,
    AuthRegisterResponse,
)
from app.services.auth_service import authenticate_user, register_user


router = APIRouter(tags=["auth"])

AUTH_REGISTER_EXAMPLES = {
    "buyer_registration": {
        "summary": "Standard buyer registration",
        "value": {
            "email": "giulia.bianchi@example.com",
            "password": "Str0ngPass!23",
            "full_name": "Giulia Bianchi",
        },
    }
}

AUTH_LOGIN_EXAMPLES = {
    "valid_credentials": {
        "summary": "Login with email and password",
        "value": {
            "email": "giulia.bianchi@example.com",
            "password": "Str0ngPass!23",
        },
    }
}


@router.post(
    "/auth/register",
    response_model=AuthRegisterResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user account",
    description="Create a standard user and return its public data. Body requires email and password.",
)
async def register_user_endpoint(
    payload: AuthRegisterRequest = Body(..., examples=AUTH_REGISTER_EXAMPLES)
):
    email = payload.email
    password = payload.password
    full_name = payload.full_name

    try:
        user = register_user(email, password, full_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    return {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role,
    }


@router.post(
    "/auth/login",
    response_model=AuthLoginResponse,
    summary="Authenticate and receive an access token",
    description="Validate user credentials and return bearer token plus public user data.",
)
async def login_user_endpoint(
    payload: AuthLoginRequest = Body(..., examples=AUTH_LOGIN_EXAMPLES)
):
    email = payload.email
    password = payload.password

    try:
        user, token_data = authenticate_user(email, password)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    user_read = {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role,
    }

    return {
        "user": user_read,
        "access_token": token_data["access_token"],
        "token_type": token_data["token_type"],
        "detail": "Login successful",
    }
