import requests
import os
from dotenv import load_dotenv
import pandas as pd
import csv
from tqdm import tqdm

load_dotenv(override=True)
API_KEY = os.getenv("TMDB_API_KEY")
headers = {"Authorization": f"Bearer {API_KEY}"}


def get_series_metadata(tmdb_id):
    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?language=pt-BR&append_to_response=credits"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        runtimes = data.get("episode_run_time", [])
        avg_runtime = sum(runtimes) / len(runtimes) if runtimes else 0
        creators = [c["name"] for c in data.get("created_by", [])]
        cast = [actor["name"] for actor in data.get("credits", {}).get("cast", [])[:3]]
        return {
            "tmdb_id": tmdb_id,
            "duracao_media": int(avg_runtime),
            "criadores": ", ".join(creators),
            "elenco_principal": ", ".join(cast),
            "total_temporadas": data.get("number_of_seasons"),
        }
    else:
        return {}


csv_output = "data_raw/series_metadata.csv"
COLUNAS = ["id", "titulo", "ano_lancamento", "metadata"]


def get_existing_ids(filepath):
    if not os.path.exists(filepath):
        return set()
    try:
        df = pd.read_csv(filepath, usecols=["id"])
        return set(df["id"].tolist())
    except (ValueError, pd.errors.EmptyDataError):
        return set()


def save_series_row(filepath, data_dict):
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0

    with open(filepath, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUNAS)
        if not file_exists:
            writer.writeheader()
        row_to_save = {k: data_dict.get(k, None) for k in COLUNAS}
        writer.writerow(row_to_save)


ids_processados = get_existing_ids(csv_output)
print(f"IDs já processados: {len(ids_processados)}")

base_df = pd.read_csv("data/serie.csv", usecols=["id", "titulo", "ano_lancamento"])

for item in tqdm(
    base_df.itertuples(index=False), total=len(base_df), desc="Processando séries"
):
    tmdb_id = item.id
    if tmdb_id in ids_processados:
        continue

    dados_serie = get_series_metadata(tmdb_id)
    row = {
        "id": tmdb_id,
        "titulo": item.titulo,
        "ano_lancamento": item.ano_lancamento,
        "metadata": dados_serie,
    }
    save_series_row(csv_output, row)

print("Processo finalizado.")
