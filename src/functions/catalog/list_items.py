import json
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client


@lambda_wrapper()
def lambda_handler(event, context):

    user_id = event.get("user_id")
    query_params = event.get("queryStringParameters") or {}

    limit = int(query_params.get("limit", 20))
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

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "items": items["items"],
                "count": items["count"],
                "next_token": items["next_token"],
            }
        ),
    }
