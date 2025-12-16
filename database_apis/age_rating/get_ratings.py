import requests
import os
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re

load_dotenv(override=True)
TMDB_HEADERS = {"Authorization ": f"Bearer {os.getenv('TMDB_API_KEY')}"}

IGDB_CLIENT_ID = os.getenv("IGBD_CLIENT_ID")
IGDB_CLIENT_SECRET = os.getenv("IGBD_CLIENT_SECRET")


def get_igdb_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": IGDB_CLIENT_ID,
        "client_secret": IGDB_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    resp = requests.post(url, params=params)
    return resp.json().get("access_token")


IGDB_token = get_igdb_token()


def create_retry_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


tmdb_session = create_retry_session()


def imdb_rating(imdb_id, type):
    url_base = f"https://api.themoviedb.org/3"
    if type == "filme":
        url = f"{url_base}/movie/{imdb_id}/release_dates"
    else:
        url = f"{url_base}/tv/{imdb_id}/content_ratings"

    try:
        response = tmdb_session.get(url, headers=TMDB_HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])

        found_ratings = {}
        for country in results:
            iso = country["iso_3166_1"]
            val = None

            if type == "filme":
                for release in country.get("release_dates", []):
                    if release.get("certification"):
                        val = release["certification"]
                        break
            else:
                val = country.get("rating")

            if val:
                found_ratings[iso] = val

        if "BR" in found_ratings:
            r = found_ratings["BR"]
            if r == "L" or r == "Livre":
                return 0
            if str(r).isdigit():
                return int(r)

        if "US" in found_ratings:
            us = str(found_ratings["US"]).upper()

            if us in ["G", "TV-Y", "TV-Y7", "TV-G"]:
                return 0
            if us in ["PG", "TV-PG"]:
                return 10
            if us == "PG-13":
                return 12
            if us == "TV-14":
                return 14
            if us in ["R", "TV-MA"]:
                return 16
            if us == "NC-17":
                return 18

        if "DE" in found_ratings:
            de = found_ratings["DE"]
            if str(de).isdigit():
                val = int(de)
                if val == 6:
                    return 10
                return val

        for pais in ["SE", "GB"]:
            if pais in found_ratings:
                r = found_ratings[pais]
                import re

                nums = re.findall(r"\d+", r)
                if nums:
                    val = int(nums[0])
                    if val <= 3:
                        return 0
                    if val == 7:
                        return 10
                    if val == 11:
                        return 12
                    if val == 15:
                        return 16
                    return val

        return None
    except requests.exceptions.RetryError:
        print(f"[{imdb_id}] Falha: Máximo de tentativas excedido.")
        return None
    except requests.exceptions.RequestException as e:
        return None
    except Exception as e:
        print(f"[{imdb_id}] Erro genérico: {e}")
        return None


def get_anime_rating(mal_id):
    url = f"https://api.jikan.moe/v4/anime/{mal_id}"

    rating_map = {
        "G - All Ages": 0,
        "PG - Children": 10,
        "PG-13 - Teens 13 or older": 12,
        "R - 17+ (violence & profanity)": 16,
        "R+ - Mild Nudity": 18,
        "Rx - Hentai": 18,
    }

    try:
        response = requests.get(url)

        if response.status_code == 429:
            return "Rate Limit Excedido (espere um pouco)"

        data = response.json().get("data", {})
        mal_rating = data.get("rating", None)
        return rating_map.get(mal_rating, mal_rating)

    except Exception as e:
        return None


def get_manga_rating(mal_id):
    url = f"https://api.jikan.moe/v4/manga/{mal_id}"

    try:
        response = requests.get(url)

        if response.status_code == 429:
            return "Rate Limit"

        data = response.json().get("data", {})
        if data.get("rating") == "Rx":
            return 18

        all_tags = set()
        for g in data.get("genres", []):
            all_tags.add(g["name"])
        for t in data.get("themes", []):
            all_tags.add(t["name"])
        for e in data.get("explicit_genres", []):
            all_tags.add(e["name"])

        genres = [g["name"] for g in data.get("genres", [])]
        if "Hentai" in genres or "Erotica" in genres:
            return 18

        if "Ecchi" in all_tags:
            return 16

        if "Gore" in all_tags:
            return 18

        if "Horror" in all_tags:
            return 16

        demographics = [d["name"] for d in data.get("demographics", [])]

        if "Seinen" in demographics or "Josei" in demographics:
            return 16
        elif "Shounen" in demographics or "Shoujo" in demographics:
            return 12
        elif "Kids" in demographics:
            return 0

        return None

    except Exception as e:
        return None


def get_game_rating(igdb_id):
    url = "https://api.igdb.com/v4/games"

    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {IGDB_token}",
    }

    query = f"fields age_ratings.organization , age_ratings.rating_category.rating; where id = {igdb_id};"
    try:
        response = requests.post(url, headers=headers, data=query)
        data = response.json()
        if not data or "age_ratings" not in data[0]:
            return None

        ratings_list = data[0]["age_ratings"]
        ratings_map = {}
        rating = None
        for item in ratings_list:
            org_id = item.get("organization")

            rating_data = item.get("rating_category", {})
            rating_val = rating_data.get("rating")

            if org_id and rating_val:
                ratings_map[org_id] = rating_val

        if 6 in ratings_map:
            rating = ratings_map[6]

        elif 1 in ratings_map:
            us_rating = ratings_map[1]
            esrb_to_br = {
                "EC": "L",
                "E": "L",
                "E10+": "10",
                "T": "12",
                "M": "16",
                "AO": "18",
                "RP": "18",
            }
            rating = esrb_to_br.get(us_rating, None)

        elif 2 in ratings_map:
            pegi_rating = ratings_map[2]
            nums = re.findall(r"\d+", str(pegi_rating))
            if nums:
                val = int(nums[0])
                if val <= 3:
                    rating = 0
                if val == 7:
                    rating = 10
                else:
                    rating = str(val)

        if rating:
            if rating == "L":
                return 0
            elif str(rating).isdigit():
                return int(rating)
        return None

    except Exception as e:
        return None


RATING_FUNCTIONS = {
    "filme": lambda imdb_id: imdb_rating(imdb_id, "filme"),
    "serie": lambda imdb_id: imdb_rating(imdb_id, "serie"),
    "anime": get_anime_rating,
    "manga": get_manga_rating,
    "jogo": get_game_rating,
}

paths = ["manga", "serie"]


def processar_tipo(args):
    tipo, pos = args
    input_path = f"data/{tipo}.csv"
    output_path = f"data/ratings/{tipo}_dataset_com_ratings.csv"
    cols_to_save = ["id", "titulo", "ano_lancamento", "classificacao"]

    if not os.path.exists(input_path):
        return f"[{tipo}] Arquivo de entrada não encontrado."

    dataset = pd.read_csv(input_path)
    processed_ids = set()
    file_exists = os.path.isfile(output_path)

    if file_exists:
        try:
            existing_data = pd.read_csv(output_path)
            total_rows = len(existing_data)
            # valid_data = existing_data.dropna(subset=["classificacao"])
            valid_data = existing_data.copy()
            reprocessed_count = total_rows - len(valid_data)
            if reprocessed_count > 0:
                valid_data.to_csv(output_path, index=False)
            processed_ids = set(valid_data["id"].unique())

        except ValueError:
            processed_ids = set()

    to_process = dataset[~dataset["id"].isin(processed_ids)]

    if to_process.empty:
        return f"[{tipo}] Já estava completo."

    file_exists_now = os.path.isfile(output_path)
    write_header = not file_exists_now

    rating_func = RATING_FUNCTIONS.get(tipo)

    if not rating_func:
        return f"[{tipo}] Função de rating não encontrada."

    for index, row in tqdm(
        to_process.iterrows(),
        desc=tipo,
        total=len(to_process),
        position=pos,
        leave=True,
    ):
        id_value = row["id"]

        try:
            rating = rating_func(id_value)
        except Exception:
            rating = None

        row_to_save = {
            "id": row["id"],
            "titulo": row.get("titulo", ""),
            "ano_lancamento": row.get("ano_lancamento", ""),
            "classificacao": rating,
        }

        pd.DataFrame([row_to_save]).to_csv(
            output_path,
            mode="a",
            header=write_header,
            index=False,
            columns=cols_to_save,
        )
        write_header = False

    return f"[{tipo}] Finalizado com sucesso."


if __name__ == "__main__":
    num_workers = len(paths)
    task_args = [(tipo, i) for i, tipo in enumerate(paths)]
    print(f"Iniciando processamento paralelo com {num_workers} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(processar_tipo, arg): arg[0] for arg in task_args}
        for future in concurrent.futures.as_completed(futures):
            tipo_processado = futures[future]
            try:
                resultado = future.result()
                tqdm.write(resultado)
            except Exception as e:
                tqdm.write(f"[{tipo_processado}] Erro fatal na thread: {e}")

    print("Todo o processamento paralelo foi concluído.")
