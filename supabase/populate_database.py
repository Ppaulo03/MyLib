from supabase import create_client, Client
import time
import requests
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

URL_API = "https://api.jikan.moe/v4/top/anime"
TIPO_MIDIA = "anime"

def buscar_animes(pagina):
    try:
        response = requests.get(f'{URL_API}?page={pagina}')
        if response.status_code == 429:
            print("Calma! Atingimos o limite da API. Esperando 5 segundos...")
            time.sleep(5)
            return None

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro na API: {response.status_code}")
            return []
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return []

def buscar_e_salvar():
    pagina_atual = 1
    while True:
        print(f"Página {pagina_atual}")
        print("1. Buscando dados da API externa...")
        data = buscar_animes(pagina_atual)
        if data is None: continue
        animes_raw = data['data']
        dados_para_inserir = []

        print("2. Formatando dados...")
        for item in animes_raw:
            
            ano = item.get('year')
            if not ano:
                try: ano = int(item['aired']['prop']['from']['year'])
                except: ano = None

            registro = {
                "categoria": TIPO_MIDIA,
                "titulo": item.get('title'),
                "descricao": item.get('synopsis'),
                "ano_lancamento": ano,
                "metadata": {
                    "id_original": item.get('mal_id'),
                    "episodios": item.get('episodes'),
                    "score": item.get('score'),
                    "imagem": item['images']['jpg']['image_url'],
                    "generos": [g['name'] for g in item.get('genres', [])]
                }
            }
            dados_para_inserir.append(registro)
        try:
            supabase.table("medias").upsert(
                dados_para_inserir, 
                on_conflict="categoria, titulo, ano_lancamento",
                ignore_duplicates=True
            ).execute()
            print("Sucesso! Dados sincronizados.")
        except Exception as e:
            print(f"Ocorreu um erro ao salvar: {e}")

        pagination = data['pagination']
        if not pagination['has_next_page']:
            break
        pagina_atual += 1

if __name__ == "__main__":
    buscar_e_salvar()