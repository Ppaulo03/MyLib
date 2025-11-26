import time
import requests
from supabase.utils import save_to_supabase


def buscar_animes(pagina):
    try:
        response = requests.get(f"https://api.jikan.moe/v4/top/anime?page={pagina}")
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
    while True:
        print(f"Página {pagina_atual}")
        print("1. Buscando dados da API externa...")
        data = buscar_animes(pagina_atual)
        if data is None:
            continue
        animes_raw = data["data"]
        dados_para_inserir = []

        print("2. Formatando dados...")
        for item in animes_raw:

            ano = item.get("year")
            if not ano:
                try:
                    ano = int(item["aired"]["prop"]["from"]["year"])
                except Exception:
                    ano = None

            registro = {
                "categoria": "anime",
                "titulo": item.get("title"),
                "descricao": item.get("synopsis"),
                "ano_lancamento": ano,
                "metadata": {
                    "id_original": item.get("mal_id"),
                    "episodios": item.get("episodes"),
                    "score": item.get("score"),
                    "imagem": item["images"]["jpg"]["image_url"],
                    "generos": [g["name"] for g in item.get("genres", [])],
                },
            }
            dados_para_inserir.append(registro)

        save_to_supabase(dados_para_inserir)
        pagination = data["pagination"]
        if not pagination["has_next_page"]:
            break
        pagina_atual += 1


if __name__ == "__main__":
    buscar_e_salvar_animes()
