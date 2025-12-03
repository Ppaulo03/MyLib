import json
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client
from utils import get_6_star_dict


@lambda_wrapper(required_fields=["id", "category"])
def lambda_handler(event, context):
    user_id = event.get("user_id")
    body = event["parsed_body"]

    media_id = str(body["id"])
    category = body["category"]
    sk_value = f"item#{category.lower()}#{media_id}"

    rating = body.get("rating", None)
    rating = float(rating) if rating else None

    old_item = db_client.query_items(user_id, sk_value)
    old_rating = None
    if old_item["items"]:
        old_rating = old_item["items"][0].get("rating", 0)
        old_rating = float(old_rating) if old_rating else None

    if rating:
        if rating > 5 and (old_rating <= 5 or not rating):
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

    db_client.update_item(user_id, sk_value, body)

    if rating:
        if rating > 5 and old_rating <= 5:
            db_client.put_item(
                {"user_id": user_id, "sk": "can_6_star", category: False}
            )
        elif rating <= 5 and old_rating > 5:
            db_client.put_item({"user_id": user_id, "sk": "can_6_star", category: True})

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Item atualizado com sucesso",
                "updated_fields": list(body.keys()),
            }
        ),
    }
