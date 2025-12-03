import json
from common.decorators import lambda_wrapper
from common.supabase_funcs import get_midia_info
from common.dynamo_client import db_client
from utils import get_6_star_dict


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
                "6star": get_6_star_dict(user_id).model_dump(),
            }
        ),
    }
