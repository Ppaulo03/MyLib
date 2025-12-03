import time
import requests
from supabase_populate.genre_map import GENRE_MAP
import csv
import os
from dotenv import load_dotenv

load_dotenv(override=True)
URL_BASE = "https://api.jikan.moe/v4/top/anime"
CATEGORIA = "anime"
CSV_PATH = "data/my_animes.csv"
LANGUAGE = "pt-BR"
FIELD_NAMES = [
    "id",
    "titulo",
    "ano_lancamento",
    "generos",
    "generos_unificados",
    "rating",
    "imagem",
    "descricao",
    "metadata",
]
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
tmdb_header = {"Authorization": f"Bearer {TMDB_API_KEY}"}


def carregar_ids_existentes(caminho_arquivo):
    ids_encontrados = set()
    if not os.path.exists(caminho_arquivo):
        return ids_encontrados
    try:
        with open(caminho_arquivo, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=",")
            for row in reader:
                if "id" in row and row["id"]:
                    ids_encontrados.add(str(row["id"]))
    except Exception as e:
        print(f"Aviso: Não foi possível ler o arquivo existente: {e}")
    return ids_encontrados


IDS_SALVOS = carregar_ids_existentes(CSV_PATH)


def buscar_animes(pagina):
    try:
        response = requests.get(f"{URL_BASE}?page={pagina}")
        if response.status_code == 429:
            print("Calma! Atingimos o limite da API. Esperando 5 segundos...")
            time.sleep(5)
            return None

        if response.status_code == 200:
            return response.json()

        print(f"Erro na API: {response.status_code}")
        return []
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return []


def save_to_csv(data):
    arquivo_existe = os.path.exists(CSV_PATH)
    with open(CSV_PATH, mode="a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_NAMES, delimiter=",")
        if not arquivo_existe:
            writer.writeheader()
        writer.writerows(data)


def obter_detalhes_anime(anime_name):
    """Busca duração, diretor, elenco e imdb_id"""
    url = (
        f"https://api.themoviedb.org/3/search/tv?query={anime_name}&language={LANGUAGE}"
    )
    try:
        r = requests.get(url, headers=tmdb_header)
        if r.status_code == 200:
            return r.json()
    except Exception:
        print(r.text)
    return None


def get_anime_info(item):
    mal_id = str(item.get("mal_id"))
    if mal_id in IDS_SALVOS:
        return None

    ano = item.get("year")

    if not ano:
        try:
            ano = int(item["aired"]["prop"]["from"]["year"])
        except Exception:
            ano = 0

    genres = [g["name"].strip().lower() for g in item.get("genres", [])]
    generos_unificados = set()
    for g in genres:
        if g in GENRE_MAP:
            generos_unificados.update(GENRE_MAP[g])
    descricao = item.get("synopsis")
    detalhes_tmdb = obter_detalhes_anime(item.get("title_english") or item.get("title"))
    tmdb_id = None
    if detalhes_tmdb and detalhes_tmdb.get("results"):
        primeiro_resultado = detalhes_tmdb["results"][0]
        descricao = primeiro_resultado.get("overview", descricao)
        tmdb_id = primeiro_resultado.get("id")

    return {
        "id": str(item.get("mal_id")),
        "titulo": item.get("title_english") or item.get("title"),
        "descricao": descricao,
        "ano_lancamento": ano,
        "imagem": item["images"]["jpg"]["image_url"],
        "generos": genres,
        "generos_unificados": list(generos_unificados),
        "rating": (item.get("score") or 0) / 2,
        "metadata": {
            "id_original": item.get("mal_id"),
            "tmdb_id": tmdb_id,
            "episodios": item.get("episodes"),
        },
    }


def buscar_e_salvar_animes():
    pagina_atual = 280

    while True:
        print(f"Página {pagina_atual}...")
        data = buscar_animes(pagina_atual)
        if data is None:
            continue
        animes_raw = data["data"]
        dados_para_inserir = []
        for item in animes_raw:
            if registro := get_anime_info(item):
                dados_para_inserir.append(registro)

        if dados_para_inserir:
            save_to_csv(dados_para_inserir)
            print(f"Sucesso! {len(dados_para_inserir)} novos animes adicionados.")
        else:
            print(f"Nenhum anime novo encontrado na página {pagina_atual}.")

        pagination = data["pagination"]
        if not pagination["has_next_page"]:
            break
        pagina_atual += 1
        if pagina_atual > 1000:
            break


if __name__ == "__main__":
    buscar_e_salvar_animes()
