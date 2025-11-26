import json
from datetime import datetime, timezone
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client


@lambda_wrapper(required_fields=["id", "categoria", "titulo"])
def lambda_handler(event, context):
    try:

        body = event["parsed_body"]
        user_id = event["user_id"]

        media_id = str(body["id"])
        category = body["categoria"].lower()
        sk_value = f"item#{category}#{media_id}"

        item = {
            "user_id": user_id,
            "sk": sk_value,
            "titulo": body["titulo"],
            "status": body.get("status", "plan_to_watch"),
            "rating": body.get("rating", None),
            "progress": body.get("progress", 0),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        db_client.put_item(item)

        return {
            "statusCode": 201,
            "body": json.dumps(
                {"message": "Item adicionado com sucesso", "item_sk": sk_value}
            ),
        }

    except Exception as e:
        print(f"Erro: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro interno ao processar item"}),
        }
