import json
import os
import re
import time
import random
from datetime import datetime, timezone

import boto3
import requests
from bs4 import BeautifulSoup
from loguru import logger
from requests.adapters import HTTPAdapter
from supabase import create_client, Client
from urllib3.util.retry import Retry
from common.dynamo_client import db_client


# --- Configurações ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
REVIEWS_QUEUE_URL = os.getenv("REVIEWS_QUEUE_URL")

supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=2,  # Wait 1s, 2s, 4s, 8s...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)
sqs_client = boto3.client("sqs")


def get_letterboxd_films(username):
    base_url = f"https://letterboxd.com/{username}/films/"
    films = []
    page = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Referer": "https://letterboxd.com/",
    }

    while True:
        url = base_url + (f"page/{page}/" if page > 1 else "")
        try:
            resp = session.get(url, headers=headers, timeout=15)

            if resp.status_code != 200:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("li", class_="griditem")

            if not items:
                break

            for item in items:
                poster_div = item.select_one("div.react-component")

                if not poster_div:
                    continue

                raw_title = poster_div.get("data-item-name", "N/A")
                title = re.sub(r"\s*\(\d{4}\)$", "", raw_title)

                full_display_name = poster_div.get("data-item-full-display-name", "")
                year_match = re.search(r"\((\d{4})\)$", full_display_name)
                year = year_match.group(1) if year_match else "N/A"

                rating_data = item.select_one(".poster-viewingdata")
                numeric_rating = None

                if rating_data:
                    rating_span = rating_data.select_one(".rating")
                    if rating_span:
                        classes = rating_span.get("class", [])
                        for c in classes:
                            if c.startswith("rated-"):
                                try:
                                    numeric_rating = int(c.split("-")[-1])
                                except ValueError:
                                    pass
                                break

                rating = numeric_rating / 2 if numeric_rating else None

                review_link = None
                if rating_data:
                    review_tag = rating_data.select_one("a.review-micro")
                    if review_tag:
                        review_link = f"https://letterboxd.com{review_tag['href']}"

                films.append(
                    {
                        "title": title,
                        "year": year,
                        "rating": rating,
                        "review_link": review_link,
                    }
                )

            next_button = soup.select_one(".paginate-nextprev a.next")
            if not next_button:
                break

            page += 1
            time.sleep(random.uniform(1.5, 3.0))

        except Exception as e:
            break

    return films


def match_movies_rpc(letterboxd_films):
    payload = []
    for f in letterboxd_films:
        item = f.copy()
        try:
            item["year"] = int(f["year"])
        except (ValueError, TypeError):
            item["year"] = 0
        payload.append(item)

    batch_size = 20
    matched_films = []

    for i in range(0, len(payload), batch_size):
        batch = payload[i : i + batch_size]

        try:
            response = supabase_client.rpc(
                "match_filmes_inteligente",
                {
                    "filmes_json": batch,
                    "match_minimo": 0.7,
                },
            ).execute()

            for row in response.data:
                movie_data = row.get("filme_completo")
                if movie_data and movie_data.get("supabase_id"):
                    matched_films.append(movie_data)

        except Exception as e:
            logger.exception(f"Erro no lote {i}: {e}")

    return matched_films


def add_review_queue(
    sqs_client, REVIEWS_QUEUE_URL, user_id, sk_value, review_link, override
):
    try:
        sqs_client.send_message(
            QueueUrl=REVIEWS_QUEUE_URL,
            MessageBody=json.dumps(
                {
                    "user_id": user_id,
                    "sk": sk_value,
                    "review_link": review_link,
                    "override": override,
                }
            ),
        )
    except Exception as e:
        logger.error(f"Falha ao enviar review para fila: {e}")


def sync_database(items, user_id, override=False):
    sk_prefix = f"item#filme#"
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

        internal_id = str(item["supabase_id"])
        sk_value = f"item#filme#{internal_id}"
        rating = item.get("rating")
        review_link = item.get("review_link")

        new_item_data = {
            "status": "watched",
            "rating": rating,
            "progress": 100,
            "review": "",
        }
        old_item = existing_items_map.get(str(internal_id))

        if old_item:
            if override:
                db_client.update_item(user_id, sk_value, new_item_data)
                old_rating = old_item.get("rating")
                if old_rating is not None and rating is not None:
                    old_rating = float(old_rating)

                    if old_rating > 5:
                        db_client.update_item(user_id, "can_6_star", {"filme": True})

                if review_link:
                    add_review_queue(
                        sqs_client,
                        REVIEWS_QUEUE_URL,
                        user_id,
                        sk_value,
                        review_link,
                        override,
                    )
        else:
            full_item = new_item_data | {
                "user_id": user_id,
                "sk": sk_value,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            db_client.put_item(full_item)

            if review_link:
                add_review_queue(
                    sqs_client,
                    REVIEWS_QUEUE_URL,
                    user_id,
                    sk_value,
                    review_link,
                    override,
                )
