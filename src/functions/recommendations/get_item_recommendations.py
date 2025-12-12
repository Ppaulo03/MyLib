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

    consumed_ids.append(int(source_id))
    recomendations = get_item_recommendation(
        source_id, source_category, target_category
    )

    if target_category:
        recomendations = {target_category: recomendations[target_category]}

    fallback = None
    CAT_LIMIT = 5
    for k, v in recomendations.items():
        v = [v_item for v_item in v if int(v_item["id"]) not in consumed_ids]
        if len(v) < CAT_LIMIT:
            if fallback == None:
                midia = get_midia_info(source_id)
                if not midia:
                    return {"statusCode": 404}
                fallback = get_fallback_recommendations(
                    consumed_ids, {g: 10 for g in midia["unified_genres"]}
                )

            rec_ids = [r["id"] for r in v]
            v.extend(
                [f for f in fallback if f["category"] == k and f["id"] not in rec_ids]
            )

        recomendations[k] = v[:CAT_LIMIT]

    if target_category:
        recomendations = recomendations[target_category]
    return {
        "statusCode": 200,
        "body": json.dumps({"recommendations": recomendations}),
    }
