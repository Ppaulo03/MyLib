import json
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client
from common.supabase_funcs import get_midia_info
from pydantic import BaseModel, Field


class Can6Star(BaseModel):
    filme: bool = Field(True)
    anime: bool = Field(True)
    jogo: bool = Field(True)
    livro: bool = Field(True)


def get_6_star_dict(user_id):
    items = db_client.query_items(
        user_id=user_id,
        sk_prefix="can_6_star",
    )
    return Can6Star(**items["items"][0]) if items["items"] else Can6Star()


@lambda_wrapper()
def lambda_handler(event, context):

    user_id = event.get("user_id")
    query_params = event.get("queryStringParameters") or {}

    limit = int(query_params.get("limit", 1000))
    next_token = query_params.get("next_token")

    prefix = "item#"
    if categoria := query_params.get("categoria", ""):
        prefix += f"{str(categoria).lower()}#"

    items = db_client.query_items(
        user_id=user_id,
        sk_prefix=prefix,
        limit=limit,
        next_token=next_token,
    )

    items["items"] = [
        {**it, **get_midia_info(it["sk"].split("#")[-1])} for it in items["items"]
    ]

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "items": items["items"],
                "count": items["count"],
                "next_token": items["next_token"],
                "6star": get_6_star_dict(user_id),
            }
        ),
    }
