from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from typing import Dict, List, Tuple, Union, Optional

from common.supabase_funcs import get_bulk_midia_info
from common.dynamo_client import db_client


def fetch_library_data(
    user_id: str, prefix: str, limit: int, next_token: Optional[str]
) -> Tuple[Dict, List]:
    """
    Orchestrates parallel fetching of library items and 6-star data.
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_items = executor.submit(
            db_client.query_items,
            user_id=user_id,
            sk_prefix=prefix,
            limit=limit,
            next_token=next_token,
        )
        future_6star = executor.submit(
            db_client.query_items,
            user_id=user_id,
            sk_prefix="can_6_star",
        )
        return future_items.result(), future_6star.result().get("items", [])


def enrich_and_group_items(
    raw_items: List[Dict], group_by_category: bool
) -> Union[List[Dict], Dict[str, List[Dict]]]:
    """
    Merges DynamoDB items with Supabase metadata and optionally groups them.
    """
    if not raw_items:
        return {} if group_by_category else []

    ids = [it["sk"].split("#")[-1] for it in raw_items]
    metadata_map = get_bulk_midia_info(ids)

    if not group_by_category:
        return [
            item | metadata_map.get(item["sk"].split("#")[-1], {}) for item in raw_items
        ]

    grouped = defaultdict(list)
    for item in raw_items:
        parts = item["sk"].split("#")
        category = parts[-2]
        item_id = parts[-1]

        enriched = item | metadata_map.get(item_id, {})
        grouped[category].append(enriched)

    return dict(grouped)
