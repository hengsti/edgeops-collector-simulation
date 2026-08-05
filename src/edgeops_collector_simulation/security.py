import secrets
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Header, HTTPException, status
from pydantic import SecretStr

ApiKeyDependency = Callable[[str | None], Awaitable[None]]


def build_secret_dependency(secret: SecretStr, key_name: str) -> ApiKeyDependency:
    async def require_key(x_edgeops_key: Annotated[str | None, Header()] = None) -> None:
        supplied = x_edgeops_key or ""
        if not secrets.compare_digest(supplied, secret.get_secret_value()):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid {key_name} API key",
            )

    return require_key
