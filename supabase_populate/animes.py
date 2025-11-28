import time
import requests
from utils import save_to_supabase
from genre_map import GENRE_MAP

URL_BASE = "https://api.jikan.moe/v4/top/anime"
CATEGORIA = "anime"


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


def buscar_e_salvar_animes():
    pagina_atual = 1
    print()
    while True:
        print(f"Página {pagina_atual}")
        data = buscar_animes(pagina_atual)
        if data is None:
            continue
        animes_raw = data["data"]
        dados_para_inserir = []
        for item in animes_raw:
            ano = item.get("year")
            if not ano:
                try:
                    ano = int(item["aired"]["prop"]["from"]["year"])
                except Exception:
                    ano = None
            genres = [g["name"].strip().lower() for g in item.get("genres", [])]
            generos_unificados = set()
            for g in genres:
                if g in GENRE_MAP:
                    generos_unificados.update(GENRE_MAP[g])

            registro = {
                "categoria": CATEGORIA,
                "titulo": item.get("title_english") or item.get("title"),
                "descricao": item.get("synopsis"),
                "ano_lancamento": ano,
                "imagem": item["images"]["jpg"]["image_url"],
                "generos": genres,
                "generos_unificados": list(generos_unificados),
                "rating": (item.get("score") or 0) / 2,
                "metadata": {
                    "id_original": item.get("mal_id"),
                    "episodios": item.get("episodes"),
                },
            }
            dados_para_inserir.append(registro)
        save_to_supabase(dados_para_inserir)
        pagination = data["pagination"]
        if not pagination["has_next_page"]:
            break
        pagina_atual += 1
        if pagina_atual > 1000:
            break


if __name__ == "__main__":
    buscar_e_salvar_animes()
