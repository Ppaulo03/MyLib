from typing import Dict, List, Any, Optional, Set
from common.supabase_funcs import (
    get_item_recommendation,
    get_fallback_recommendations,
    get_midia_info,
)
from utils import get_user_history, get_user_consumed_ids


class MediaNotFoundError(Exception):
    """Raised when the source media cannot be found."""

    pass


def process_recommendations(
    user_id: str,
    source_id: int,
    source_category: str,
    target_category: Optional[str] = None,
    limit: int = 5,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Orchestrates the recommendation logic: fetching history, filtering,
    and filling gaps with fallbacks.
    """

    user_history = get_user_history(user_id)
    consumed_ids: Set[int] = set(get_user_consumed_ids(user_history))
    consumed_ids.add(source_id)

    recommendations = get_item_recommendation(
        source_id, source_category, target_category
    )

    if target_category:
        recommendations = {target_category: recommendations.get(target_category, [])}

    fallback_pool: Optional[List[Dict[str, Any]]] = None

    for category, items in recommendations.items():
        valid_items = [item for item in items if int(item["id"]) not in consumed_ids]
        if len(valid_items) < limit:
            if fallback_pool is None:
                media_info = get_midia_info(source_id)
                if not media_info:
                    raise MediaNotFoundError(f"Media source {source_id} not found.")

                weights = {g: 10 for g in media_info.get("unified_genres", [])}
                fallback_pool = get_fallback_recommendations(
                    list(consumed_ids), weights
                )
            current_ids = {int(item["id"]) for item in valid_items}

            for fb in fallback_pool:
                if len(valid_items) >= limit:
                    break

                fb_id = int(fb["id"])
                if fb["category"] == category and fb_id not in current_ids:
                    valid_items.append(fb)
                    current_ids.add(fb_id)

        recommendations[category] = valid_items[:limit]
    return recommendations
