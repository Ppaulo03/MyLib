import json
from common.decorators import lambda_wrapper
from common.supabase_funcs import get_bulk_midia_info
from common.dynamo_client import db_client
from utils import get_6_star_dict
from collections import defaultdict


@lambda_wrapper()
def lambda_handler(event, context):

    user_id = event.get("user_id")
    query_params = event.get("queryStringParameters") or {}

    limit = int(query_params.get("limit", 1000))
    next_token = query_params.get("next_token")
    categoria = query_params.get("categoria", "")
    prefix = "item#"
    if categoria:
        prefix += f"{str(categoria).lower()}#"

    items = db_client.query_items(
        user_id=user_id,
        sk_prefix=prefix,
        limit=limit,
        next_token=next_token,
    )
    ids_para_buscar = [it["sk"].split("#")[-1] for it in items["items"]]
    infos_bulk = get_bulk_midia_info(ids_para_buscar)
    items["items"] = [
        {**it, **infos_bulk.get(it["sk"].split("#")[-1], {})} for it in items["items"]
    ]
    if not categoria:
        grouped_items = defaultdict(list)

        for it in items["items"]:
            key = it["sk"].split("#")[-2]
            grouped_items[key].append(it)

        items["items"] = dict(grouped_items)
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
