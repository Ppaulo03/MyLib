from common.decorators import lambda_wrapper
from common.supabase_funcs import (
    supabase,
    get_midia_info,
    get_fallback_recommendations,
)
from utils import get_user_history, get_user_top_genres
import json


@lambda_wrapper()
def lambda_handler(event, context):
    params = event.get("queryStringParameters", {}) or {}
    target_category = params.get("target_category")

    user_id = event.get("user_id")
    user_history = get_user_history(user_id)
    liked_items = [item for item in user_history if float(item.get("rating", 0)) >= 4.0]
    if not liked_items:
        return {
            "statusCode": 200,
            "body": json.dumps({"recommendations": []}),
        }

    source_ids = []
    source_types = []
    seen_set = set()

    for item in user_history:
        sk = item["sk"].split("#")
        i_id = sk[-1]
        i_type = sk[-2]

        seen_set.add(f"{i_type}_{i_id}")

        if float(item.get("rating", 0)) >= 4.0:
            source_ids.append(i_id)
            source_types.append(i_type)

    if not source_ids:
        return {
            "statusCode": 200,
            "body": json.dumps({"recommendations": []}),
        }

    rpc_response = supabase.rpc(
        "get_batch_recommendations",
        {"source_ids": source_ids, "source_types": source_types},
    ).execute()

    raw_recs = rpc_response.data

    candidates = {}
    for rec in raw_recs:
        t_id = rec["alvo_id"]
        t_type = rec["alvo_categoria"]
        score = rec["score"]
        unique_key = f"{t_type}_{t_id}"

        if unique_key in seen_set:
            continue

        if unique_key in candidates:
            candidates[unique_key]["score"] += score
            candidates[unique_key]["sources_count"] += 1
        else:
            candidates[unique_key] = {
                "item_id": t_id,
                "categoria": t_type,
                "score": score,
                "sources_count": 1,
            }

    grouped_recs = {"anime": [], "filme": [], "jogo": [], "livro": []}
    if target_category:
        grouped_recs = {target_category: grouped_recs[target_category]}

    for item in candidates.values():
        cat = item["categoria"]
        if cat not in grouped_recs:
            grouped_recs[cat] = []

        grouped_recs[cat].append(item)

    LIMIT_PER_CATEGORY = 5
    fallback = None
    for cat in grouped_recs:
        grouped_recs[cat].sort(key=lambda x: x["score"], reverse=True)
        grouped_recs[cat] = [
            get_midia_info(item["item_id"])
            for item in grouped_recs[cat][:LIMIT_PER_CATEGORY]
        ]

        if len(grouped_recs[cat]) < LIMIT_PER_CATEGORY:
            if fallback == None:
                consumed_ids, top_genres_list = get_user_top_genres(user_history)
                fallback = get_fallback_recommendations(
                    consumed_ids, top_genres_list, limit=LIMIT_PER_CATEGORY
                )

            rec_ids = [r["id"] for r in grouped_recs[cat]]
            grouped_recs[cat].extend(
                [f for f in fallback if f["category"] == cat and f["id"] not in rec_ids]
            )

    return {
        "statusCode": 200,
        "body": json.dumps({"recommendations": grouped_recs}),
    }
