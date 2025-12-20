from common.dynamo_client import DynamoClient
from common.supabase_funcs import get_bulk_midia_info
from collections import defaultdict


def get_user_history(user_id):
    dynamo = DynamoClient()

    user_history = []
    next_token = None
    while True:
        dynamo_response = dynamo.query_items(user_id, "item#", next_token=next_token)
        next_token = dynamo_response["next_token"]
        user_history.extend(dynamo_response["items"])
        if not next_token:
            break
    return user_history


def get_user_top_genres(user_history):
    consumed_ids = []
    genre_scores = defaultdict(float)

    items_to_process = []
    ids_to_fetch = set()

    for item in user_history:
        try:
            midia_id = int(item["sk"].split("#")[-1])
        except (ValueError, IndexError):
            continue

        consumed_ids.append(midia_id)
        rating = float(item.get("rating", 0) or 0)
        status = str(item.get("status") or "planned")

        if rating <= 0 or status in ["planned", "abandoned"]:
            continue

        items_to_process.append((midia_id, rating))
        ids_to_fetch.add(midia_id)

    midia_info_map = get_bulk_midia_info(list(ids_to_fetch))
    genre_scores = defaultdict(float)

    for midia_id, rating in items_to_process:
        midia_info = midia_info_map.get(midia_id)

        if not midia_info:
            continue

        genres = midia_info.get("unified_genres", [])

        if rating >= 4:
            weight = rating
        elif rating == 3:
            weight = 1
        else:
            weight = rating - 10

        for genre in genres:
            genre_scores[genre] += weight

    sorted_genres = dict(sorted(genre_scores.items(), key=lambda x: x[1], reverse=True))

    return consumed_ids, sorted_genres


def get_user_consumed_ids(user_history):
    return [int(item["sk"].split("#")[-1]) for item in user_history]
