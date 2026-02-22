from typing import Dict, Optional, Tuple

from app.core.database import mongo_db
from app.core.security import create_access_token_with_claims, hash_password, verify_password
from app.models import User


_USERS_COLLECTION = mongo_db["users"]


def register_user(email: str, password: str, full_name: Optional[str] = None) -> User:
    existing = _USERS_COLLECTION.find_one({"email": email})
    if existing is not None:
        raise ValueError("Email already registered")

    safe_password = password[:72]

    user = User(
        id="",
        email=email,
        full_name=full_name,
        role="buyer",
    )

    user_doc = user.to_json()
    user_doc["password_hash"] = hash_password(safe_password)
    result = _USERS_COLLECTION.insert_one(user_doc)
    user_id = str(result.inserted_id)

    user.id = user_id
    return user


def authenticate_user(email: str, password: str) -> Tuple[User, Dict[str, str]]:
    user_doc = _USERS_COLLECTION.find_one({"email": email})
    if not user_doc:
        raise ValueError("Invalid email or password")

    if not verify_password(password, user_doc.get("password_hash", "")):
        raise ValueError("Invalid email or password")

    user = User.from_json(user_doc)

    access_token = create_access_token_with_claims(
        subject=user.id,
        claims={
            "role": user.role,
            "email": user.email,
            "full_name": user.full_name,
            "phone": user.phone,
            "city": user.city,
        },
    )

    return user, {
        "access_token": access_token,
        "token_type": "bearer",
    }
