import requests
import time
import csv
import os
from dotenv import load_dotenv

load_dotenv(override=True)
URL_BASE = "https://api.jikan.moe/v4"
NOME_ARQUIVO = "data/animes.csv"
MAX_PAGINAS = 2000


def carregar_ids_ja_salvos():
    """
    Lê o CSV existente e retorna um conjunto (set) com os IDs já processados.
    Isso permite retomar o script sem duplicar dados.
    """
    ids = set()
    if not os.path.exists(NOME_ARQUIVO):
        return ids

    try:
        with open(NOME_ARQUIVO, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=",")
            for row in reader:
                if row.get("id"):
                    ids.add(int(row["id"]))
        print(f"🔄 Retomando: {len(ids)} animes já encontrados no arquivo.")
    except Exception as e:
        print(f"Erro ao ler arquivo existente: {e}. Começando do zero.")

    return ids


def salvar_anime_individual(anime_dict):

    arquivo_existe = os.path.exists(NOME_ARQUIVO)
    colunas = [
        "id",
        "titulo",
        "ano_lancamento",
        "generos",
        "generos_unificados",
        "rating",
        "num_avaliacoes",
        "imagem",
        "descricao",
        "metadados",
    ]
    try:
        with open(NOME_ARQUIVO, mode="a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=colunas,
                delimiter=",",
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
            )

            if not arquivo_existe:
                writer.writeheader()
            writer.writerow(anime_dict)
    except Exception as e:
        print(f"Erro ao salvar no disco: {e}")


def try_request(url, headers=None):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            time.sleep(2)
            return try_request(url, headers=headers)

        if response.status_code != 200:
            print(response.text)
            return {}

        return response.json()

    except Exception as e:
        print(e)
    return {}


def check_antecedents(mal_id):
    url = f"{URL_BASE}/anime/{mal_id}/full"
    try:
        response = try_request(url)
        dados = response["data"]
        if dados["type"] == "TV":
            return False
        for relation in dados["relations"]:
            if relation.get("relation").lower() in ["prequel", "parent story"]:
                for entry in relation.get("entry", []):
                    time.sleep(0.2)
                    if check_antecedents(entry["mal_id"]):
                        continue
                    return False
        return True

    except Exception as e:
        print(e)
    return False


TMDB_API_KEY = os.getenv("TMDB_API_KEY")


def get_dets(title, year):
    url = f"https://api.themoviedb.org/3/search/tv?query={title}&language=pt-BR&year={year}"
    header = {
        "Authorization": f"Bearer {TMDB_API_KEY}",
    }
    try:
        response = try_request(url, headers=header)
        dados = response["results"]
        if not dados:
            return None
        item = dados[0]
        first_year_date = item.get("first_air_date", "").split("-")[0]
        if year and first_year_date and int(first_year_date) != int(year):
            return None
        print(item["name"])
        return item.get("overview", None) if dados else None
    except Exception as e:
        print(e)
    return None


def buscar_animes():

    ids_processados = carregar_ids_ja_salvos()
    pagina = 404
    while True:
        print(f"--- Lendo Página {pagina} ---")
        try:
            resp = requests.get(
                f"{URL_BASE}/top/anime",
                params={"page": pagina},
            )

            if resp.status_code == 429:
                time.sleep(2)
                continue

            if resp.status_code != 200:
                print(f"Fim ou erro na API (Status {resp.status_code})")
                break

            data = resp.json()
            items = data.get("data", [])

            if not items:
                break

            for item in items:

                mal_id = item.get("mal_id")
                titulo = item.get("title_english") or item.get("title")
                ano = (
                    str(item.get("year") or item.get("aired", {}).get("from") or "")
                    .split("-")[0]
                    .strip()
                )

                if not ano:
                    continue

                if mal_id in ids_processados:
                    continue

                dets = get_dets(titulo, ano)
                if not dets:
                    continue

                genres = [g["name"] for g in item.get("genres", [])]
                generes_unificados = []
                anime_data = {
                    "id": mal_id,
                    "titulo": titulo,
                    "ano_lancamento": ano,
                    "generos": genres,
                    "generos_unificados": list(generes_unificados),
                    "rating": item.get("score"),
                    "num_avaliacoes": (
                        item.get("scored_by") / 2 if item.get("scored_by") else 0
                    ),
                    "imagem": item["images"]["jpg"]["image_url"],
                    "descricao": (dets or "").replace("\n", " "),
                    "metadados": {
                        "id_original": mal_id,
                        "episodios": item.get("episodes"),
                    },
                }

                salvar_anime_individual(anime_data)
                ids_processados.add(mal_id)

            pagination = data.get("pagination", {})
            if not pagination.get("has_next_page") or (
                MAX_PAGINAS and pagina >= MAX_PAGINAS
            ):
                print("Fim do processamento.")
                break

            pagina += 1

        except KeyboardInterrupt:
            print("\nScript interrompido pelo usuário.")
            break
        except Exception as e:
            print(f"Erro crítico: {e}")
            time.sleep(10)


if __name__ == "__main__":
    buscar_animes()
