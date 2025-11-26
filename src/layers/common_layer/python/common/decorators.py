import json
import functools
from common.errors import AppError, BadRequestError, UnauthorizedError


def lambda_wrapper(required_fields=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(event, context):
            try:

                claims = (
                    event.get("requestContext", {})
                    .get("authorizer", {})
                    .get("claims", {})
                )
                user_id = claims.get("sub")
                if not user_id:
                    raise UnauthorizedError(
                        "Unauthorized: user id not found in authorizer claims"
                    )

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

                return func(event, context)

            except AppError as e:
                return {
                    "statusCode": e.status_code,
                    "body": json.dumps(
                        {"message": e.message, "error_code": e.error_code}
                    ),
                }

            except Exception as e:
                print(f"ERRO CRÍTICO: {str(e)}")
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
