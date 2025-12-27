import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import time
import random
import re
import os
from supabase import create_client, Client
from dotenv import load_dotenv


load_dotenv(override=True)

# --- Configurações ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


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
                print(f"Stopping: Status code {resp.status_code}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("li", class_="griditem")

            if not items:
                print("No films found on this page. Finished.")
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
                print("Last page reached.")
                break

            page += 1
            time.sleep(random.uniform(1.5, 3.0))

        except Exception as e:
            print(f"Error on page {page}: {e}")
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

    print(f"Processando {len(payload)} filmes via RPC...")
    for i in range(0, len(payload), batch_size):
        batch = payload[i : i + batch_size]

        try:
            response = supabase_client.rpc(
                "match_filmes_inteligente",
                {
                    "filmes_json": batch,
                    "match_minimo": 0.0,  # Pode ajustar a rigidez aqui
                },
            ).execute()

            for row in response.data:
                movie_data = row.get("filme_completo")
                if movie_data and movie_data.get("supabase_id"):
                    matched_films.append(movie_data)

            print(f"Lote {i}: Processado.")

        except Exception as e:
            print(f"Erro no lote {i}: {e}")

    return matched_films


def get_full_review_text(review_url):
    """
    Entra no link da review e extrai o texto completo.
    Retorna None se falhar ou se não houver texto.
    """
    if not review_url:
        return None

    try:
        print(f"--> Baixando review: {review_url}")
        resp = session.get(review_url, timeout=10)

        if resp.status_code == 429:
            print("BLOCK 429 Detectado! Pausando por 60s...")
            time.sleep(60)
            return get_full_review_text(session, review_url)  # Tenta de novo

        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        body_div = soup.select_one("div.body-text")

        if body_div:
            return body_div.get_text(separator="\n", strip=True)

    except Exception as e:
        print(f"Erro ao baixar review: {e}")

    return None


if __name__ == "__main__":

    username = "Hepamynondas"
    data = get_letterboxd_films(username)
    print(f"\nTotal films scraped: {len(data)}")
    matches = match_movies_rpc(data)
    print(matches[0])
