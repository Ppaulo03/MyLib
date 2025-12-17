import json
from datetime import datetime, timezone
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client
from utils import get_6_star_dict


@lambda_wrapper(required_fields=["id", "category", "title"])
def lambda_handler(event, context):
    body = event["parsed_body"]
    user_id = event["user_id"]

    media_id = str(body["id"])
    category = body["category"].lower()
    sk_value = f"item#{category}#{media_id}"
    rating = body.get("rating", None)
    rating = float(rating) if rating else None

    if rating and rating > 5:
        can_6_star_dict = get_6_star_dict(user_id).model_dump()
        if not can_6_star_dict.get(category):
            return {
                "statusCode": 406,
                "body": json.dumps(
                    {
                        "message": "Não foi possivel usar superlike",
                        "item_sk": sk_value,
                    }
                ),
            }
    item = {
        "user_id": user_id,
        "sk": sk_value,
        "title": body["title"],
        "status": body.get("status", "planned"),
        "rating": rating,
        "progress": body.get("progress", 0),
        "review": body.get("review", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    db_client.put_item(item)

    if rating and rating > 5:
        db_client.put_item({"user_id": user_id, "sk": "can_6_star", category: False})

    return {
        "statusCode": 201,
        "body": json.dumps(
            {"message": "Item adicionado com sucesso", "item_sk": sk_value}
        ),
    }
