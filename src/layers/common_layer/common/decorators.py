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


def _is_sqs_event(event: JsonDict) -> bool:
    """Detecta se o evento é do SQS verificando a presença de 'Records' e 'eventSource'."""
    if "Records" in event and isinstance(event["Records"], list):
        if (
            len(event["Records"]) > 0
            and event["Records"][0].get("eventSource") == "aws:sqs"
        ):
            return True
    return False


def _process_sqs_record(record: JsonDict, model: Type[T]) -> T:
    try:
        raw_body = record.get("body", "{}")
        body_data = json.loads(raw_body)
        if not isinstance(body_data, dict):
            body_data = {}
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in SQS Record: {record.get('messageId')}")
        raise ValueError("Invalid JSON body in SQS message")

    user_id = None
    msg_attrs = record.get("messageAttributes", {})
    if "UserId" in msg_attrs and "stringValue" in msg_attrs["UserId"]:
        user_id = msg_attrs["UserId"]["stringValue"]

    if user_id:
        body_data["user_id"] = user_id

    try:
        return model(**body_data)
    except ValidationError as e:
        logger.error(
            f"Validation failed for SQS message {record.get('messageId')}: {e.json()}"
        )
        raise ValueError(f"Schema Validation Error: {e.json()}")


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

            if _is_sqs_event(event):
                results = []
                for record in event["Records"]:
                    try:
                        request_model = _process_sqs_record(record, model)
                        result = func(request_model, context)
                        results.append(result)

                    except Exception as e:
                        logger.exception(
                            f"Error processing SQS record {record.get('messageId')}"
                        )
                        raise e
                return results[0] if len(results) == 1 else results

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
