import json
import functools
from common.errors import AppError, BadRequestError, UnauthorizedError
from loguru import logger
from datetime import datetime, date


def calculate_age(birthdate_str):
    try:
        birth_date = datetime.strptime(birthdate_str, "%Y-%m-%d").date()
        today = date.today()
        return (
            today.year
            - birth_date.year
            - ((today.month, today.day) < (birth_date.month, birth_date.day))
        )
    except ValueError:
        return 0


def lambda_wrapper(required_fields=None, required_params=None, require_auth=True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(event, context):
            try:

                request_context = event.get("requestContext", {})
                authorizer = request_context.get("authorizer", {})
                claims = {}
                if "jwt" in authorizer:
                    claims = authorizer.get("jwt", {}).get("claims", {})
                elif "claims" in authorizer:
                    claims = authorizer.get("claims", {})
                print(claims)
                user_id = claims.get("sub") or claims.get("username")

                if not user_id and require_auth:
                    print(
                        f"DEBUG AUTH FALHOU. Evento recebido: {json.dumps(request_context)}"
                    )
                    raise UnauthorizedError(
                        "Unauthorized: user id not found in authorizer claims"
                    )
                event["user_id"] = str(user_id)

                if birthdate_str := claims.get("birthdate"):
                    event["user_age"] = calculate_age(birthdate_str)

                body = {}
                if event.get("body"):
                    try:
                        body = json.loads(event["body"])
                    except json.JSONDecodeError:
                        raise BadRequestError("Invalid JSON body", "INVALID_JSON")

                event["parsed_body"] = body

                if required_fields:
                    missing = [f for f in required_fields if f not in body]
                    if missing:
                        raise BadRequestError(
                            f"Campos obrigatórios faltando: {', '.join(missing)}",
                            "MISSING_FIELDS",
                        )

                if required_params:
                    missing_params = [
                        p
                        for p in required_params
                        if p not in (event.get("queryStringParameters") or {})
                    ]
                    if missing_params:
                        raise BadRequestError(
                            f"Parâmetros obrigatórios faltando: {', '.join(missing_params)}",
                            "MISSING_PARAMS",
                        )

                return func(event, context)

            except AppError as e:
                return {
                    "statusCode": e.status_code,
                    "body": json.dumps(
                        {"message": e.message, "error_code": e.error_code}
                    ),
                }

            except Exception as e:
                logger.exception("Unhandled exception in lambda_wrapper")
                return {
                    "statusCode": 500,
                    "body": json.dumps(
                        {
                            "message": "Erro interno do servidor",
                            "error_code": "INTERNAL_SERVER_ERROR",
                        }
                    ),
                }

        return wrapper

    return decorator
