import time
import requests
import csv
import json
import os

BASE_URL = "https://api.jikan.moe/v4/manga"
CSV_FILENAME = "ETL/data/raw/manga_raw.csv"


CSV_COLUMNS = [
    "mal_id",
    "titulo",
    "titulos_alternativos",
    "ano_lancamento",
    "generos",
    "rating",
    "num_avaliacoes",
    "imagem",
    "descricao",
    "classificacao",
    "metadata",
]


def get_existing_ids():
    """Lê o CSV e retorna um set com todos os mal_id já salvos."""
    existing_ids = set()
    if os.path.isfile(CSV_FILENAME):
        try:
            with open(CSV_FILENAME, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Garante que pegamos apenas IDs válidos
                    if row.get("mal_id"):
                        existing_ids.add(int(row["mal_id"]))
            print(f"Encontrados {len(existing_ids)} mangás já salvos no arquivo.")
        except Exception as e:
            print(f"Aviso ao ler arquivo existente: {e}")
    return existing_ids


def init_csv():
    """Inicia o CSV com cabeçalho se ele não existir."""
    if not os.path.isfile(CSV_FILENAME):
        with open(CSV_FILENAME, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def get_manga_rating(data):

    try:

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


def fetch_and_save_mangas(limit=25, max_pages=500):
    init_csv()

    # Carrega os IDs existentes na memória para verificação rápida (O(1))
    existing_ids = get_existing_ids()

    for page in range(1, max_pages + 1):
        try:
            print(f"Buscando página {page}...", end=" ")
            response = requests.get(
                f"{BASE_URL}?page={page}&limit={limit}&order_by=popularity"
            )

            if response.status_code == 200:
                data = response.json()
                rows_to_save = []
                skipped_count = 0

                for item in data["data"]:
                    current_id = item["mal_id"]

                    # === LÓGICA DE DEDUPLICAÇÃO ===
                    if current_id in existing_ids:
                        skipped_count += 1
                        continue

                    # Se não existe, adicionamos ao set para não duplicar na mesma execução
                    existing_ids.add(current_id)

                    # 1. Extração do Ano
                    try:
                        ano = (
                            item.get("published", {})
                            .get("prop", {})
                            .get("from", {})
                            .get("year")
                        )
                    except:
                        ano = None

                    # 2. Metadata
                    meta_obj = {
                        "mal_id": current_id,
                        "type": item.get("type"),
                        "status": item.get("status"),
                        "volumes": item.get("volumes"),
                        "chapters": item.get("chapters"),
                        "authors": [a["name"] for a in item.get("authors", [])],
                        "serializations": [
                            s["name"] for s in item.get("serializations", [])
                        ],
                        "url": item.get("url"),
                    }
                    classificacao = get_manga_rating(item)
                    # 3. Linha do CSV
                    manga_row = {
                        "mal_id": current_id,  # Coluna explicita
                        "titulo": item.get("title_english") or item.get("title"),
                        "titulos_alternativos": [
                            i["title"] for i in item.get("titles", [])
                        ],
                        "ano_lancamento": ano,
                        "generos": json.dumps(
                            [g["name"] for g in item.get("genres", [])],
                            ensure_ascii=False,
                        ),
                        "rating": item.get("score"),
                        "num_avaliacoes": item.get("scored_by"),
                        "imagem": item.get("images", {})
                        .get("jpg", {})
                        .get("image_url"),
                        "descricao": (item.get("synopsis", "") or "")
                        .replace("\n", " ")
                        .replace("\r", "")
                        .strip(),
                        "classificacao": classificacao,
                        "metadata": json.dumps(meta_obj, ensure_ascii=False),
                    }

                    rows_to_save.append(manga_row)

                # Feedback visual sobre o que aconteceu na página
                if rows_to_save:
                    with open(
                        CSV_FILENAME, mode="a", newline="", encoding="utf-8"
                    ) as f:
                        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                        writer.writerows(rows_to_save)
                    print(f"Salvos: {len(rows_to_save)}")
                else:
                    print(f"Todos os itens desta página já existem.")

                if not data["pagination"]["has_next_page"]:
                    print("Fim das páginas disponíveis.")
                    break

                time.sleep(1)

            elif response.status_code == 429:
                print("\nRate limit. Aguardando 10s...")
                time.sleep(10)
                continue
            else:
                print(f"\nErro HTTP {response.status_code}")

        except Exception as e:
            print(f"\nErro crítico: {e}")
            break


if __name__ == "__main__":
    fetch_and_save_mangas(max_pages=1000)
