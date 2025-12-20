from common.dynamo_client import db_client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from common.supabase_funcs import build_media_map
from datetime import datetime, timezone
from requests.exceptions import RequestException
from loguru import logger
import requests
import time
import html
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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(RequestException),
    before_sleep=before_sleep_log(logger, "WARNING"),
)
def safe_mal_request(url, headers, params):
    """
    Faz o pedido à API de forma segura.
    Lança uma exceção se falhar após 3 tentativas.
    """
    response = requests.get(url, headers=headers, params=params, timeout=10)
    response.raise_for_status()

    return response.json()


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
        try:
            data = safe_mal_request(url, headers, params)
        except Exception as e:
            logger.error(
                f"Falha crítica ao obter dados do MAL após várias tentativas: {e}"
            )
            break

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


def sync_database(items, category, user_id, override=False):
    id_map = build_media_map(
        items,
        category,
        item_id_key="mal_id",
        item_title_key="title",
        supabase_id_key="mal_id" if category == "manga" else "id_original",
    )

    sk_prefix = f"item#{category}#"
    existing_response = db_client.query_items(user_id, sk_prefix)
    next_token = existing_response.get("next_token", None)
    while next_token:
        more_response = db_client.query_items(user_id, sk_prefix, next_token=next_token)
        existing_response["items"].extend(more_response.get("items", []))
        next_token = more_response.get("next_token", None)

    existing_items_map = {}
    for db_item in existing_response.get("items", []):
        internal_id = db_item["sk"].split("#")[-1]
        existing_items_map[internal_id] = db_item

    for item in items:
        mal_id = str(item["mal_id"])
        internal_id = id_map.get(mal_id)

        if not internal_id:
            logger.info(f"Skipping {item['title']} (Not found in DB)")
            continue

        sk_value = f"item#{category}#{internal_id}"
        rating = item.get("user_score", None)
        new_item_data = {
            "status": item.get("user_status", "planned"),
            "rating": rating,
            "progress": item.get("progress", 0),
            "review": item.get("comments", ""),
        }
        old_item = existing_items_map.get(str(internal_id))

        if old_item:
            if override:
                db_client.update_item(user_id, sk_value, new_item_data)
                old_rating = old_item.get("rating")
                if old_rating is not None and rating is not None:
                    old_rating = float(old_rating)

                    if old_rating > 5:
                        db_client.update_item(user_id, "can_6_star", {category: True})
        else:
            full_item = new_item_data | {
                "user_id": user_id,
                "sk": sk_value,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            db_client.put_item(full_item)
