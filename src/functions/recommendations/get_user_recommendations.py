from common.decorators import lambda_wrapper
from common.supabase_funcs import (
    supabase,
    get_bulk_midia_info,
    get_fallback_recommendations,
)
from common.responses import success
from utils import get_user_history, get_user_top_genres


@lambda_wrapper()
def lambda_handler(event, context):
    params = event.get("queryStringParameters", {}) or {}
    target_category = params.get("target_category")

    user_id = event.get("user_id")
    user_history = get_user_history(user_id)
    liked_items = [
        item for item in user_history if float(item.get("rating", 0) or 0) >= 4.0
    ]
    if not liked_items:
        return success({"recommendations": []})

    source_ids = []
    source_types = []
    seen_set = set()

    for item in user_history:
        sk = item["sk"].split("#")
        i_id = sk[-1]
        i_type = sk[-2]

        seen_set.add(f"{i_type}_{i_id}")

        if float(item.get("rating", 0) or 0) >= 4.0:
            source_ids.append(i_id)
            source_types.append(i_type)

    if not source_ids:
        return success({"recommendations": []})

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

    grouped_recs = {
        "anime": [],
        "filme": [],
        "jogo": [],
        "livro": [],
        "serie": [],
        "manga": [],
    }

    for item in candidates.values():
        cat = item["categoria"]
        if cat not in grouped_recs:
            grouped_recs[cat] = []

        grouped_recs[cat].append(item)

    if target_category:
        grouped_recs = {target_category: grouped_recs[target_category]}

    LIMIT_PER_CATEGORY = 24
    fallback = None
    for cat in grouped_recs:

        grouped_recs[cat].sort(key=lambda x: x["score"], reverse=True)

        flitered_recs = []
        midia_ids = [item["item_id"] for item in grouped_recs[cat]]
        midia_info = get_bulk_midia_info(midia_ids)
        for item in midia_info.values():
            if item:
                flitered_recs.append(item)
            if len(flitered_recs) >= LIMIT_PER_CATEGORY:
                break

        grouped_recs[cat] = flitered_recs

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
            grouped_recs[cat] = grouped_recs[cat][:LIMIT_PER_CATEGORY]

    return success({"recommendations": grouped_recs})
