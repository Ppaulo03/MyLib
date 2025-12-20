from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union, TypedDict
from pydantic import BaseModel, ValidationError, Field
from common.responses import (
    bad_request,
    internal_error,
    unauthorized,
    unprocessable_entity,
)
from loguru import logger
import functools
import base64
import json

T = TypeVar("T", bound=BaseModel)
JsonDict = Dict[str, Any]


class APIGatewayResponse(TypedDict):
    statusCode: int
    body: str
    headers: Dict[str, str]


Response = Union[APIGatewayResponse, Dict[str, Any]]


class AuthRequest(BaseModel):
    """Base class for all authenticated requests."""

    user_id: str = Field(description="Injected automatically by Auth Middleware")


def _extract_auth_data(
    event: JsonDict, require_auth: bool
) -> tuple[Dict[str, Any], Optional[Response]]:
    """
    Extracts user_id and claims to inject into the model.
    """
    ctx = event.get("requestContext") or {}
    authorizer = ctx.get("authorizer") or {}

    claims = authorizer.get("jwt", {}).get("claims") or authorizer.get("claims") or {}
    user_id = claims.get("sub") or claims.get("username")

    if require_auth and not user_id:
        logger.warning("Unauthorized access. Context: {}", ctx)
        return {}, unauthorized("Unauthorized: Missing or invalid token")

    auth_data = {}
    if user_id:
        auth_data["user_id"] = str(user_id)
    if claims:
        auth_data["claims"] = claims

    return auth_data, None


def _parse_body(event: JsonDict) -> tuple[Dict[str, Any], Optional[Response]]:
    """Parses JSON body safely."""
    raw_body = event.get("body")
    if not raw_body:
        return {}, None

    try:
        if event.get("isBase64Encoded"):
            raw_body = base64.b64decode(raw_body).decode("utf-8")
        return json.loads(raw_body), None
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return {}, bad_request("Invalid JSON body")


def _merge_request_data(event: JsonDict, body: Dict, auth_data: Dict) -> Dict[str, Any]:
    qs = event.get("queryStringParameters") or {}
    path = event.get("pathParameters") or {}
    return {**qs, **path, **body, **auth_data}


def lambda_wrapper(
    model: Type[T],
    require_auth: bool = True,
) -> Callable[[Callable[[T, Any], Any]], Callable[..., Any]]:
    """
    Decorator that hydrates a Pydantic model from the AWS Lambda event.

    Args:
        model: The Pydantic class to validate against.
        require_auth: If True, blocks requests without a valid token.
    """

    def decorator(func: Callable[[T, Any], Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(event: Optional[JsonDict], context: Any) -> Any:
            event = event or {}

            try:

                auth_data, err = _extract_auth_data(event, require_auth)
                if err:
                    return err

                body_data, err = _parse_body(event)
                if err:
                    return err

                request_data = _merge_request_data(event, body_data, auth_data)

                try:
                    request_model = model(**request_data)
                except ValidationError as e:
                    logger.warning(f"Validation failed: {e.errors()}")
                    return unprocessable_entity(error=e.json())

                return func(request_model, context)

            except Exception as e:
                logger.exception("Unhandled exception in lambda_wrapper")
                return internal_error(error=str(e))

        return wrapper

    return decorator
