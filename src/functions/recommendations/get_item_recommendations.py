from common.decorators import lambda_wrapper
from common.supabase_funcs import (
    get_item_recommendation,
    get_fallback_recommendations,
    get_midia_info,
)
from utils import get_user_history, get_user_consumed_ids
import json


@lambda_wrapper(required_params=["id", "category"])
def lambda_handler(event, context):
    user_id = event.get("user_id")
    user_history = get_user_history(user_id)
    consumed_ids = get_user_consumed_ids(user_history)

    params = event.get("queryStringParameters", {}) or {}
    source_id = params.get("id")
    source_category = params.get("category")
    target_category = params.get("target_category")

    recomendations = get_item_recommendation(
        source_id, source_category, target_category
    )

    if target_category:
        recomendations = {target_category: recomendations[target_category]}

    fallback = None

    for k, v in recomendations.items():
        v = [v_item for v_item in v if v_item["id"] not in consumed_ids]
        if len(v) < 5:
            if fallback == None:
                midia = get_midia_info(source_id)
                fallback = get_fallback_recommendations(
                    consumed_ids, midia["unified_genres"]
                )

            rec_ids = [r["id"] for r in v]
            recomendations[k].extend(
                [f for f in fallback if f["categoria"] == k and f["id"] not in rec_ids]
            )

    if target_category:
        recomendations = recomendations[target_category]
    return {
        "statusCode": 200,
        "body": json.dumps({"recommendations": recomendations}),
    }
