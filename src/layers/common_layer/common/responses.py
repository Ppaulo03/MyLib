from typing import Any, Optional, Dict, Union
import json

_DEFAULT_HEADERS = {
    "Content-Type": "application/json",
}


def api_response(
    status_code: int,
    body: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Centralized API Gateway response formatter.

    Args:
        status_code: HTTP status code
        body: Response body dict
        error: Error message (if error, body is ignored)
        headers: Custom headers to include

    Returns:
        Formatted Lambda response for API Gateway
    """
    final_headers = (
        _DEFAULT_HEADERS if headers is None else {**_DEFAULT_HEADERS, **headers}
    )
    payload = {"error": error} if error else body

    return {
        "statusCode": status_code,
        "headers": final_headers,
        "body": json.dumps(payload) if payload is not None else "",
    }


def success(
    data: Dict[str, Any],
    status_code: int = 200,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Success response (2xx)."""
    return api_response(status_code, body=data, headers=headers)


def created(
    data: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Created response (201)."""
    return api_response(201, body=data, headers=headers)


def bad_request(error: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Bad request response (400)."""
    return api_response(400, error=error, headers=headers)


def unauthorized(
    error: str, headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Unauthorized response (401)."""
    return api_response(401, error=error, headers=headers)


def forbidden(error: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Forbidden response (403)."""
    return api_response(403, error=error, headers=headers)


def not_found(
    error: str = "Not found", headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Not found response (404)."""
    return api_response(404, error=error, headers=headers)


def conflict(error: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Conflict response (409)."""
    return api_response(409, error=error, headers=headers)


def not_acceptable(
    error: str, headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Not acceptable response (406)."""
    return api_response(406, error=error, headers=headers)


def internal_error(
    error: str = "Internal server error", headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Internal server error response (500)."""
    return api_response(500, error=error, headers=headers)


def unprocessable_entity(
    error: str, headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Unprocessable entity response (422)."""
    return api_response(422, error=error, headers=headers)
