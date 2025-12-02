from common.dynamo_client import db_client
from common.decorators import lambda_wrapper
from common.supabase_funcs import search_midia
from datetime import datetime, timezone

import requests
import time
import json
import os


def get_user_animelist(username):
    url = f"https://api.myanimelist.net/v2/users/{username}/animelist"

    headers = {"X-MAL-CLIENT-ID": os.getenv("MAL_CLIENT_ID")}
    params = {
        "fields": "list_status{comments},num_episodes_watched,score,alternative_titles",
        "limit": 100,
        "nsfw": "true",
    }

    all_animes = []
    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Erro: {response.status_code} - {response.text}")
            break

        data = response.json()
        for node in data.get("data", []):
            anime_info = {
                "mal_id": node["node"]["id"],
                "title": node["node"]["alternative_titles"].get("en")
                or node["node"]["title"],
                "main_picture": node["node"]["main_picture"]["medium"],
                "user_status": node["list_status"]["status"],
                "user_score": node["list_status"]["score"] / 2,
                "watched_episodes": node["list_status"]["num_episodes_watched"],
                "comments": node["list_status"]["comments"],
            }
            all_animes.append(anime_info)

        if "paging" in data and "next" in data["paging"]:
            url = data["paging"]["next"]
            params = {}
            time.sleep(1)  # Respeitar rate limit
        else:
            break

    return all_animes


@lambda_wrapper(required_fields=["username"])
def lambda_handler(event, context):
    try:

        body = event["parsed_body"]
        user_id = event["user_id"]
        animes = get_user_animelist(body.get("username"))
        override = body.get("override", False)
        category = "anime"

        for anime in animes:
            item = search_midia(search_term=anime["title"], category="anime")
            if str(item[0]["metadata"].get("mal_id", "")) != str(anime["mal_id"]):
                continue

            sk_value = f"item#{category}#{item[0]['id']}"

            item = {
                "status": anime.get("user_status", "planned"),
                "rating": anime.get("user_score", None),
                "progress": anime.get("watched_episodes", 0),
                "review": anime.get("comments", ""),
            }

            if db_client.query_items(user_id, sk_value)["items"]:
                if override:
                    db_client.update_item(user_id, sk_value, item)
                continue

            item |= {
                "user_id": user_id,
                "sk": sk_value,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            db_client.put_item(item)

        return {
            "statusCode": 200,
        }

    except Exception as e:
        print(f"Erro: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro interno ao processar item"}),
        }
