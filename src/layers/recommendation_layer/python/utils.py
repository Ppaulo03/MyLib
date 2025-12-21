from common.dynamo_client import DynamoClient
from common.supabase_funcs import get_bulk_midia_info
from collections import defaultdict


def get_user_history(user_id):
    dynamo = DynamoClient()
    user_history = []
    next_token = None

    while True:
        response = dynamo.query_items(user_id, "item#", next_token=next_token)
        user_history.extend(response.get("items", []))

        next_token = response.get("next_token")
        if not next_token:
            break

    return user_history


def get_user_top_genres(user_history):
    all_consumed_ids = []
    items_to_score = []
    ids_to_fetch_info = set()

    IGNORED_STATUSES = {"planned", "abandoned"}

    for item in user_history:
        sk = item.get("sk")
        if not sk:
            continue

        try:
            midia_id = int(sk.split("#")[-1])
        except ValueError:
            continue

        try:
            midia_id = int(item["sk"].split("#")[-1])
        except (ValueError, IndexError):
            continue

        all_consumed_ids.append(midia_id)

        rating = float(item.get("rating") or 0)
        status = str(item.get("status") or "planned")

        if rating > 0 and status not in IGNORED_STATUSES:
            items_to_score.append((midia_id, rating))
            ids_to_fetch_info.add(midia_id)

    if not ids_to_fetch_info:
        return all_consumed_ids, {}

    midia_info_map = get_bulk_midia_info(list(ids_to_fetch_info))
    genre_scores = defaultdict(float)

    for midia_id, rating in items_to_score:
        midia_info = midia_info_map.get(midia_id)
        if not midia_info:
            continue

        genres = midia_info.get("unified_genres", [])
        if not genres:
            continue

        if rating >= 4:
            weight = rating
        elif rating == 3:
            weight = 1
        else:
            weight = rating - 10

        for genre in genres:
            genre_scores[genre] += weight

    sorted_genres = dict(sorted(genre_scores.items(), key=lambda x: x[1], reverse=True))

    return all_consumed_ids, sorted_genres


def get_user_consumed_ids(user_history):
    return [
        int(item["sk"].split("#")[-1])
        for item in user_history
        if item.get("sk") and "#" in item["sk"]
    ]
