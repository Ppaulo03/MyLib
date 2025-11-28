import json
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client


@lambda_wrapper(required_fields=["id", "category"])
def lambda_handler(event, context):
    user_id = event.get("user_id")
    body = event["parsed_body"]

    media_id = str(body["id"])
    category = body["category"]
    sk_value = f"item#{category.lower()}#{media_id}"
    db_client.update_item(user_id, sk_value, body)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Item atualizado com sucesso",
                "updated_fields": list(body.keys()),
            }
        ),
    }
