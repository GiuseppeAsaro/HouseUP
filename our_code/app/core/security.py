import os
from datetime import datetime, timedelta

import jwt
from passlib.context import CryptContext





_JWT_SECRET = os.getenv("HOUSEUP_SECRET_KEY", "houseup-development-secret")
_JWT_ALG = "HS256"
_ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 30

_pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
    bcrypt__truncate_error=False,
)


def hash_password(plain_password: str) -> str:
    
    return _pwd_context.hash(plain_password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    
    return _pwd_context.verify(plain_password, hashed_password)


def create_access_token(subject: str) -> str:
    
    expire = datetime.utcnow() + timedelta(minutes=_ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"sub": subject, "exp": expire}
    return jwt.encode(to_encode, _JWT_SECRET, algorithm=_JWT_ALG)


def decode_access_token(token: str) -> str:
    
    try:
        payload = jwt.decode(token, _JWT_SECRET, algorithms=[_JWT_ALG])
    except jwt.PyJWTError as exc:
        raise ValueError("Invalid access token") from exc

    subject = payload.get("sub")
    if not subject:
        raise ValueError("Invalid access token payload")

    return str(subject)


def create_access_token_with_claims(subject: str, claims: dict) -> str:
    expire = datetime.utcnow() + timedelta(minutes=_ACCESS_TOKEN_EXPIRE_MINUTES)
    safe_claims = dict(claims or {})
    safe_claims.pop("sub", None)
    safe_claims.pop("exp", None)
    to_encode = {"sub": subject, "exp": expire, **safe_claims}
    return jwt.encode(to_encode, _JWT_SECRET, algorithm=_JWT_ALG)


def decode_access_token_payload(token: str) -> dict:
    try:
        payload = jwt.decode(token, _JWT_SECRET, algorithms=[_JWT_ALG])
    except jwt.PyJWTError as exc:
        raise ValueError("Invalid access token") from exc
    if not payload.get("sub"):
        raise ValueError("Invalid access token payload")
    return dict(payload)
