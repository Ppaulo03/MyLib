import time
import requests
import csv
import json
import os
from dotenv import load_dotenv

load_dotenv(override=True)
# === CONFIGURAÇÕES ===
API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"
CSV_FILENAME = "data_raw/serie_raw.csv"

ANIMATION_GENRE_ID = 16
BLOCKED_COUNTRIES = {"JP", "CN", "KR", "TW", "HK"}

CSV_COLUMNS = [
    "tmdb_id",
    "titulo",
    "ano_lancamento",
    "generos",
    "rating",
    "num_avaliacoes",
    "imagem",
    "descricao",
    "metadata",
]
header = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json;charset=utf-8",
}


def get_genre_map():
    """Busca a lista de gêneros para mapear ID -> Nome (ex: 16 -> Animation)."""
    url = f"{BASE_URL}/genre/tv/list"
    try:
        response = requests.get(url, params={"language": "pt-BR"}, headers=header)
        print(response.text)
        if response.status_code == 200:
            genres = response.json().get("genres", [])
            return {g["id"]: g["name"] for g in genres}
    except Exception as e:
        print(f"Erro ao buscar gêneros: {e}")
    return {}


def get_existing_ids():
    """Lê o CSV e retorna um set com todos os tmdb_id já salvos."""
    existing_ids = set()
    if os.path.isfile(CSV_FILENAME):
        try:
            with open(CSV_FILENAME, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("tmdb_id"):
                        existing_ids.add(int(row["tmdb_id"]))
            print(f"Encontrados {len(existing_ids)} séries já salvas.")
        except Exception as e:
            print(f"Aviso ao ler arquivo: {e}")
    return existing_ids


def init_csv():
    if not os.path.isfile(CSV_FILENAME):
        with open(CSV_FILENAME, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def fetch_tmdb_series(limit_pages=100):
    init_csv()
    existing_ids = get_existing_ids()
    genre_map = get_genre_map()

    if not genre_map:
        print("Falha: Mapa de gêneros indisponível.")
        return

    print("Iniciando coleta (Filtrando Animações Orientais)...")

    for page in range(1, limit_pages + 1):
        try:
            params = {
                "api_key": API_KEY,
                "language": "pt-BR",
                "sort_by": "popularity.desc",
                "page": page,
                "include_null_first_air_dates": "false",
            }

            response = requests.get(
                f"{BASE_URL}/discover/tv", params=params, headers=header
            )

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                rows_to_save = []
                skipped_count = 0
                anime_filtered_count = 0

                for item in results:
                    t_id = item["id"]

                    # 1. Deduplicação
                    if t_id in existing_ids:
                        skipped_count += 1
                        continue

                    # 2. FILTRO APRIMORADO DE ANIME
                    genre_ids = item.get("genre_ids", [])
                    origin_countries = set(
                        item.get("origin_country", [])
                    )  # Converte pra set pra facilitar

                    if ANIMATION_GENRE_ID in genre_ids and (
                        origin_countries & BLOCKED_COUNTRIES
                    ):
                        anime_filtered_count += 1
                        continue

                    existing_ids.add(t_id)

                    try:
                        ano = item.get("first_air_date", "").split("-")[0]
                    except:
                        ano = None

                    genre_names = [genre_map.get(gid, str(gid)) for gid in genre_ids]

                    meta_obj = {
                        "tmdb_id": t_id,
                        "original_name": item.get("original_name"),
                        "origin_country": list(origin_countries),
                        "original_language": item.get("original_language"),
                        "popularity": item.get("popularity"),
                        "backdrop_path": (
                            f"{IMAGE_BASE_URL}{item.get('backdrop_path')}"
                            if item.get("backdrop_path")
                            else None
                        ),
                    }

                    poster = (
                        f"{IMAGE_BASE_URL}{item.get('poster_path')}"
                        if item.get("poster_path")
                        else None
                    )

                    row = {
                        "tmdb_id": t_id,
                        "titulo": item.get("name"),
                        "ano_lancamento": ano,
                        "generos": json.dumps(genre_names, ensure_ascii=False),
                        "rating": item.get("vote_average"),
                        "num_avaliacoes": item.get("vote_count"),
                        "imagem": poster,
                        "descricao": (item.get("overview") or "")
                        .replace("\n", " ")
                        .replace("\r", " ")
                        .strip(),
                        "metadata": json.dumps(meta_obj, ensure_ascii=False),
                    }
                    rows_to_save.append(row)

                if rows_to_save:
                    with open(
                        CSV_FILENAME, mode="a", newline="", encoding="utf-8"
                    ) as f:
                        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                        writer.writerows(rows_to_save)

                print(
                    f"Pág {page}: Salvos: {len(rows_to_save)} | Já existiam: {skipped_count} | Animes/Donghua filtrados: {anime_filtered_count}"
                )

                if page >= data["total_pages"]:
                    break

                time.sleep(0.2)

            else:
                print(f"Erro {response.status_code}")
                time.sleep(5)

        except Exception as e:
            print(f"Erro: {e}")
            break


if __name__ == "__main__":
    fetch_tmdb_series(limit_pages=500)
