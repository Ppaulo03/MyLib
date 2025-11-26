import json
from common.decorators import lambda_wrapper
from common.errors import BadRequestError
from common.dynamo_client import db_client


@lambda_wrapper(required_params=["id", "categoria"])
def lambda_handler(event, context):
    user_id = event.get("user_id")

    params = event.get("queryStringParameters") or {}
    media_id = params.get("id")
    categoria = params.get("categoria")

    sk_value = f"item#{categoria.lower()}#{media_id}"
    db_client.delete_item(user_id, sk_value)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {"message": "Item removido com sucesso", "deleted_id": media_id}
        ),
    }
