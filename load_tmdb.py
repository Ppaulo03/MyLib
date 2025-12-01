import requests
import time
import csv
import os
from supabase_populate.genre_map import GENRE_MAP
from dotenv import load_dotenv


load_dotenv(override=True)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
LANGUAGE = "pt-BR"
PAGINAS_PARA_BUSCAR = 50
ANO_FINAL = 1980
ANO_INICIAL = 1985
ARQUIVO_CSV = "data/tmdb_movies_clean.csv"
header = {"Authorization": f"Bearer {TMDB_API_KEY}"}


def carregar_ids_existentes(caminho_arquivo):
    """Lê o CSV e retorna um conjunto (set) com os IDs que já temos."""
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


def obter_detalhes_filme(movie_id):
    """Busca duração, diretor, elenco e imdb_id"""
    url = f"{BASE_URL}/movie/{movie_id}?language={LANGUAGE}&append_to_response=credits"
    try:
        r = requests.get(url, headers=header)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None


def extrair_diretor(crew_list):
    """Encontra o diretor na lista de equipe"""
    for person in crew_list:
        if person.get("job") == "Director":
            return person.get("name")
    return ""


def extrair_atores(cast_list):
    """Pega os 3 primeiros atores"""
    atores = [p.get("name") for p in cast_list[:3]]
    return ", ".join(atores)


def main():
    ids_ja_salvos = carregar_ids_existentes(ARQUIVO_CSV)
    print(f"--- Iniciando ---")
    print(f"Filmes já no banco de dados: {len(ids_ja_salvos)}")
    for ano in range(ANO_INICIAL, ANO_FINAL - 1, -1):
        for pagina in range(1, PAGINAS_PARA_BUSCAR + 1):
            filmes_novos = []
            url_lista = f"{BASE_URL}/discover/movie?primary_release_year={ano}&language={LANGUAGE}&page={pagina}"
            resp = requests.get(url_lista, headers=header)
            if resp.status_code == 200:
                resultados = resp.json().get("results", [])
                for item in resultados:
                    movie_id = item.get("id")
                    if str(movie_id) in ids_ja_salvos:
                        continue
                    detalhes = obter_detalhes_filme(movie_id)

                    if detalhes:
                        generos = [g["name"] for g in detalhes.get("genres", [])]
                        generos_unificados = set().union(
                            *[
                                GENRE_MAP[g.strip().lower()]
                                for g in generos
                                if g.strip().lower() in GENRE_MAP
                            ]
                        )
                        if len(generos_unificados) == 0:
                            generos_unificados = []
                        data_full = detalhes.get("release_date", "")
                        year = data_full.split("-")[0] if data_full else ""
                        caminho_poster = item.get("poster_path", "")
                        if caminho_poster:
                            caminho_poster = (
                                f"https://image.tmdb.org/t/p/w500{caminho_poster}"
                            )
                        else:
                            caminho_poster = ""
                        filme_formatado = {
                            "id": movie_id,
                            "titulo": detalhes.get("title"),
                            "ano_lancamento": year,
                            "generos": generos,
                            "generos_unificados": generos_unificados,
                            "rating": detalhes.get("vote_average") / 2,
                            "num_avaliacoes": detalhes.get("vote_count"),
                            "imagem": caminho_poster,
                            "descricao": item.get("overview", ""),
                            "metadados": {
                                "duracao": (
                                    f"{detalhes.get('runtime')} min"
                                    if detalhes.get("runtime")
                                    else ""
                                ),
                                "diretor": extrair_diretor(
                                    detalhes.get("credits", {}).get("crew", [])
                                ),
                                "star": extrair_atores(
                                    detalhes.get("credits", {}).get("cast", [])
                                ),
                                "tmdb_id": movie_id,
                            },
                        }
                        filmes_novos.append(filme_formatado)

            else:
                print(resp.text)
            if filmes_novos:
                arquivo_existe = os.path.exists(ARQUIVO_CSV)
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

                with open(ARQUIVO_CSV, mode="a", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=colunas, delimiter=",")
                    if not arquivo_existe:
                        writer.writeheader()
                    writer.writerows(filmes_novos)
                print(f"\nSucesso! {len(filmes_novos)} novos filmes adicionados.")
            else:
                print(f"\nNenhum filme novo encontrado na página {pagina}.")

            time.sleep(0.2)
            print(f"Página {pagina} concluída.")


if __name__ == "__main__":
    main()
