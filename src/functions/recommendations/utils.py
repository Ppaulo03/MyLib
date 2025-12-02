from common.dynamo_client import DynamoClient
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

    for item in user_history:
        midia_id = int(item["sk"].split("#")[-1])
        consumed_ids.append(midia_id)

        rating = float(item["rating"]) if item.get("rating") is not None else 0
        status = str(item["status"]) if item.get("status") else "planned"
        if rating <= 0 or status in ["planned", "abandoned"]:
            continue

        genres = item.get("unified_genres", [])

        weight = 0
        if rating >= 4:
            weight = rating
        elif rating == 3:
            weight = 1
        else:
            weight = 0.1

        for genre in genres:
            genre_scores[genre] += weight

    sorted_genres = sorted(genre_scores.items(), key=lambda x: x[1], reverse=True)
    top_genres_list = [g[0] for g in sorted_genres]
    return consumed_ids, top_genres_list


def get_user_consumed_ids(user_history):
    return [int(item["sk"].split("#")[-1]) for item in user_history]
