from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
from common.configs import CATEGORIES_AVAILABLE
from common.supabase_funcs import (
    supabase,
    get_bulk_midia_info,
    get_fallback_recommendations,
)
from utils import get_user_history, get_user_top_genres


def process_user_recommendations(
    user_id: str, target_category: Optional[str] = None, limit: int = 24
) -> Dict[str, List[Dict[str, Any]]]:
    user_history = get_user_history(user_id)

    seen_keys: Set[str] = set()
    seen_ids_only: Set[int] = set()
    source_ids: List[str] = []
    source_types: List[str] = []

    for item in user_history:
        parts = item["sk"].split("#")
        i_id = parts[-1]
        i_type = parts[-2]

        seen_keys.add(f"{i_type}_{i_id}")
        seen_ids_only.add(int(i_id))

        rating = float(item.get("rating", 0) or 0)
        if rating >= 4.0:
            source_ids.append(i_id)
            source_types.append(i_type)

    if not source_ids:
        return {}

    rpc_response = supabase.rpc(
        "get_batch_recommendations",
        {"source_ids": source_ids, "source_types": source_types},
    ).execute()

    candidates = defaultdict(lambda: {"score": 0.0, "sources": 0, "id": "", "cat": ""})

    for rec in rpc_response.data:
        t_id = str(rec["alvo_id"])
        t_type = rec["alvo_categoria"]
        unique_key = f"{t_type}_{t_id}"

        if unique_key in seen_keys:
            continue

        entry = candidates[unique_key]
        entry["score"] += rec["score"]
        entry["sources"] += 1
        entry["id"] = t_id
        entry["cat"] = t_type

    grouped_recs: Dict[str, List[Dict]] = {c: [] for c in CATEGORIES_AVAILABLE}

    for item in candidates.values():
        cat = item["cat"]
        if cat in grouped_recs:
            grouped_recs[cat].append(item)

    if target_category:
        grouped_recs = {target_category: grouped_recs.get(target_category, [])}

    fallback_pool = None

    for cat in grouped_recs:

        category_candidates = grouped_recs[cat]
        category_candidates.sort(key=lambda x: x["score"], reverse=True)
        top_candidates = category_candidates[: int(limit * 1.5)]

        candidate_ids = [c["id"] for c in top_candidates]

        metadata_map = get_bulk_midia_info(candidate_ids)
        final_items = []
        current_ids_in_batch = set()

        for cand in top_candidates:
            c_id = cand["id"]
            info = metadata_map.get(str(c_id))

            if info:
                final_items.append(info)
                current_ids_in_batch.add(int(c_id))

            if len(final_items) >= limit:
                break

        if len(final_items) < limit:
            if fallback_pool is None:
                _, top_genres_list = get_user_top_genres(user_history)
                fallback_pool = get_fallback_recommendations(
                    list(seen_ids_only), top_genres_list, limit=limit
                )

            for fb in fallback_pool:
                if len(final_items) >= limit:
                    break

                fb_id = int(fb["id"])
                if (
                    (fb["category"] == cat)
                    and (fb_id not in seen_ids_only)
                    and (fb_id not in current_ids_in_batch)
                ):
                    final_items.append(fb)
                    current_ids_in_batch.add(fb_id)

        grouped_recs[cat] = final_items

    return grouped_recs
