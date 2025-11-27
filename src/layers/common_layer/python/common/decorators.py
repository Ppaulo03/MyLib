import json
import functools
from common.errors import AppError, BadRequestError, UnauthorizedError


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

                user_id = claims.get("sub") or claims.get("username")

                if not user_id and require_auth:
                    print(
                        f"DEBUG AUTH FALHOU. Evento recebido: {json.dumps(request_context)}"
                    )
                    raise UnauthorizedError(
                        "Unauthorized: user id not found in authorizer claims"
                    )
                event["user_id"] = str(user_id)
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
