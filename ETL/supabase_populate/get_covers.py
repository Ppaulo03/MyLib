import os
import requests

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
STEAMGRIDDB_API_KEY = os.getenv("STEAMGRIDDB_API_KEY")


def get_movie_cover(title):
    url_busca = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&query={title}"
    response = requests.get(
        url_busca, headers={"Authorization": f"Bearer {TMDB_API_KEY}"}
    )
    if response.status_code == 200:
        dados = response.json()
        if dados["results"]:
            filme = dados["results"][0]
            caminho_poster = filme["poster_path"]
            if caminho_poster:
                url_imagem = f"https://image.tmdb.org/t/p/w500{caminho_poster}"
                return url_imagem
    return None


def get_game_cover(game_name):

    headers = {"Authorization": f"Bearer {STEAMGRIDDB_API_KEY}"}
    url_busca = f"https://www.steamgriddb.com/api/v2/search/autocomplete/{game_name}"
    response = requests.get(url_busca, headers=headers)
    if response.status_code == 200:
        dados_busca = response.json()

        if dados_busca["data"]:
            game_id = dados_busca["data"][0]["id"]

            url_grid = f"https://www.steamgriddb.com/api/v2/grids/game/{game_id}?dimensions=600x900&styles=alternate,material"

            response_grid = requests.get(url_grid, headers=headers)

            if response_grid.status_code == 200:
                grids = response_grid.json()["data"]

                if grids:
                    capa_url = grids[0]["url"]
                    return capa_url
    return None


def get_book_cover(book_title):
    url_busca = f"https://www.googleapis.com/books/v1/volumes?q={book_title}"
    response = requests.get(url_busca)
    if response.status_code == 200:
        dados = response.json()
        if "items" in dados:
            livro = dados["items"][0]["volumeInfo"]
            if "imageLinks" in livro:
                url_ruim = livro["imageLinks"].get("thumbnail") or livro[
                    "imageLinks"
                ].get("smallThumbnail")
                url_hd = (
                    url_ruim.replace("&zoom=1", "&zoom=0")
                    .replace("&edge=curl", "")
                    .replace("http://", "https://")
                )
                return url_hd
    return None
