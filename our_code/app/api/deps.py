from typing import Callable, Optional

from bson import ObjectId
from fastapi import Depends, Header, HTTPException, status

from app.core.database import mongo_db
from app.core.security import decode_access_token_payload
from app.models import User
from app.utils.permissions_utils import ensure_roles


USERS_COLLECTION = mongo_db["users"]


async def get_current_user(authorization: Optional[str] = Header(default=None)) -> User:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
        )

    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_access_token_payload(token)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
        )

    subject = str(payload.get("sub") or "").strip()
    role = payload.get("role")
    email = payload.get("email")
    if not subject or role is None or email is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token payload",
        )

    return User.from_json(
        {
            "id": subject,
            "email": email,
            "role": role,
            "full_name": payload.get("full_name"),
            "phone": payload.get("phone"),
            "city": payload.get("city"),
        }
    )


def require_roles(*roles: str) -> Callable[[User], User]:
    allowed = {str(role).strip().lower() for role in roles if str(role).strip()}

    async def _dep(current_user: User = Depends(get_current_user)) -> User:
        try:
            ensure_roles(
                current_user,
                allowed_roles=allowed,
                message="Forbidden for this user",
            )
        except PermissionError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
        return current_user

    return _dep
