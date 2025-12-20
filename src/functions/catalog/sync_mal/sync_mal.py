from common.dynamo_client import db_client
from common.decorators import lambda_wrapper
from common.supabase_funcs import search_midia
from common.responses import success
from datetime import datetime, timezone
import html

import requests
import time
import os

status_list = {
    "watching": "in_progress",
    "reading": "in_progress",
    "completed": "completed",
    "on_hold": "on_hold",
    "dropped": "abandoned",
    "plan_to_watch": "planned",
    "plan_to_read": "planned",
}


def my_anime_list_getter(username, list_type):
    type_map = {
        "anime": ("animelist", "num_episodes_watched"),
        "manga": ("mangalist", "num_chapters_read"),
    }

    endpoint, progress_field = type_map.get(list_type, (None, None))
    if not endpoint or not progress_field:
        return []

    url = f"https://api.myanimelist.net/v2/users/{username}/{endpoint}"
    headers = {"X-MAL-CLIENT-ID": os.getenv("MAL_CLIENT_ID")}
    params = {
        "fields": f"list_status{{comments}},{progress_field},score,alternative_titles",
        "limit": 100,
        "nsfw": "true",
    }

    all_items = []
    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Erro: {response.status_code} - {response.text}")
            break

        data = response.json()
        for node in data.get("data", []):
            raw_comment = node["list_status"].get("comments", "")
            clean_comment = html.unescape(raw_comment) if raw_comment else ""
            item_info = {
                "mal_id": node["node"]["id"],
                "title": node["node"]["alternative_titles"].get("en")
                or node["node"]["title"],
                "user_status": status_list.get(
                    node["list_status"]["status"], "planned"
                ),
                "user_score": node["list_status"]["score"] / 2,
                "progress": node["list_status"].get(progress_field, 0),
                "comments": clean_comment,
            }
            all_items.append(item_info)

        if "paging" in data and "next" in data["paging"]:
            url = data["paging"]["next"]
            params = {}
            time.sleep(1)
        else:
            break
    return all_items


def sync_supabase_mal(items, category, user_id, override=False):
    for item in items:
        search_item, _ = search_midia(search_term=item["title"], category=category)
        if not search_item or str(search_item[0]["metadata"].get("mal_id", "")) != str(
            item["mal_id"]
        ):
            continue

        sk_value = f"item#{category}#{search_item[0]['id']}"
        rating = item.get("user_score", None)
        item = {
            "status": item.get("user_status", "planned"),
            "rating": rating,
            "progress": item.get("progress", 0),
            "review": item.get("comments", ""),
        }

        if db_client.query_items(user_id, sk_value)["items"]:
            if override:
                old_item = db_client.query_items(user_id, sk_value)
                old_rating = None
                if old_item["items"]:
                    old_rating = old_item["items"][0].get("rating", 0)
                    old_rating = float(old_rating) if old_rating else None
                db_client.update_item(user_id, sk_value, item)

                if rating and old_rating:
                    if rating > 5 and old_rating <= 5:
                        db_client.put_item(
                            {
                                "user_id": user_id,
                                "sk": "can_6_star",
                                category: False,
                            }
                        )
                    elif rating <= 5 and old_rating > 5:
                        db_client.put_item(
                            {"user_id": user_id, "sk": "can_6_star", category: True}
                        )
                continue

        item |= {
            "user_id": user_id,
            "sk": sk_value,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        db_client.put_item(item)


@lambda_wrapper(required_fields=["username", "category"])
def lambda_handler(event, context):
    body = event["parsed_body"]
    user_id = event["user_id"]
    override = body.get("override", False)
    category = body.get("category").lower()

    items = my_anime_list_getter(body.get("username"), category)
    sync_supabase_mal(items, category, user_id, override)
    return success({"message": "Sync completed successfully"})
