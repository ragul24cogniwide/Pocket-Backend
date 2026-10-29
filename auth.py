"""
Authentication and JWT token handling module.
Supports passwordless phone number OTP login and token verification.
"""

import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import User

# The signing secret must come from the environment. A default baked into the
# source would let anyone who can read the code mint tokens for any user.
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "").strip()
if not JWT_SECRET_KEY:
    raise RuntimeError(
        "JWT_SECRET_KEY is not set. Generate one with "
        "`python -c \"import secrets; print(secrets.token_urlsafe(48))\"` "
        "and add it to the environment (.env locally, the Render dashboard in production)."
    )
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_DAYS = int(os.getenv("ACCESS_TOKEN_EXPIRE_DAYS", "30"))

security_bearer = HTTPBearer(auto_error=False)


def generate_otp_code(length: int = 6) -> str:
    """Generates a random numeric OTP using a cryptographically secure source."""
    return "".join(secrets.choice("0123456789") for _ in range(length))


def create_access_token(user_id: str, phone_number: str, token_version: int = 0) -> str:
    """Generates a signed JWT access token for the authenticated user."""
    expire = datetime.now(timezone.utc) + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    payload = {
        "sub": user_id,
        "phone_number": phone_number,
        "tv": int(token_version or 0),  # bumped on logout: older tokens stop working
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    """Decodes and validates a JWT access token (signature and expiry)."""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
        return None


def same_identity(a: Optional[str], b: Optional[str]) -> bool:
    """True when two user ids / phone numbers refer to the same person (tolerates +91 vs bare digits)."""
    if not a or not b:
        return False
    a, b = str(a).strip(), str(b).strip()
    if a == b:
        return True
    da = "".join(filter(str.isdigit, a))
    db_ = "".join(filter(str.isdigit, b))
    return len(da) >= 10 and len(db_) >= 10 and da[-10:] == db_[-10:]


async def _find_user(db: AsyncSession, identifier: str) -> Optional[User]:
    identifier = str(identifier).strip()
    digits = "".join(filter(str.isdigit, identifier))
    conditions = [User.id == identifier, User.phone_number == identifier]
    if len(digits) >= 10:
        last10 = digits[-10:]
        conditions.append(User.phone_number.endswith(last10))
        conditions.append(User.id.endswith(last10))
    result = await db.execute(select(User).where(or_(*conditions)))
    return result.scalars().first()


async def user_from_token(token: Optional[str], db: AsyncSession) -> Optional[User]:
    """Resolves the user a bearer token belongs to, or None if the token is missing, invalid or expired."""
    if not token:
        return None
    payload = decode_access_token(token)
    if not payload or "sub" not in payload:
        return None
    # Exact match only. Matching by "last 10 digits" could resolve a token to a different
    # person whose number shares those digits (e.g. +1 917 555 0123 vs +91 91755 50123).
    sub = str(payload["sub"]).strip()
    result = await db.execute(select(User).where(or_(User.id == sub, User.phone_number == sub)))
    user = result.scalars().first()
    if user is None:
        return None
    # Logged out (or account deleted and re-created): tokens from before are revoked
    if int(payload.get("tv") or 0) != int(getattr(user, "token_version", 0) or 0):
        return None
    return user


async def get_current_user(
    auth: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer),
    db: AsyncSession = Depends(get_db),
) -> User:
    """FastAPI dependency for protecting REST routes with JWT bearer token."""
    if not auth or not auth.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token is missing.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = await user_from_token(auth.credentials, db)
    if not user:
        # 401 (not 404) so clients treat a deleted account the same as an expired session
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def get_current_user_optional(
    auth: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer),
    db: AsyncSession = Depends(get_db),
) -> Optional[User]:
    """Optional FastAPI dependency: returns the user when a valid token is sent, otherwise None."""
    if not auth or not auth.credentials:
        return None
    return await user_from_token(auth.credentials, db)
