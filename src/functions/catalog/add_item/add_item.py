from datetime import datetime, timezone
from common.decorators import lambda_wrapper
from common.responses import created, not_acceptable
from common.dynamo_client import db_client
from utils import get_6_star_dict


def get_6_star_dict(user_id):
    items = db_client.query_items(
        user_id=user_id,
        sk_prefix="can_6_star",
    )
    return items.get("items", [{}])[0]


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
        can_6_star_dict = get_6_star_dict(user_id)
        if not can_6_star_dict.get(category, True):
            return not_acceptable("Não foi possivel usar superlike")

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

    return created({"message": "Item adicionado com sucesso", "item_sk": sk_value})
