import os
import requests
import pandas as pd
from time import sleep
import tqdm
from dotenv import load_dotenv

load_dotenv(override=True)
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
STEAMGRIDDB_API_KEY = os.getenv("STEAMGRIDDB_API_KEY")


def get_game_cover(game_name, tries=0):
    headers = {"Authorization": f"Bearer {STEAMGRIDDB_API_KEY}"}
    url_busca = f"https://www.steamgriddb.com/api/v2/search/autocomplete/{game_name}"
    try:
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
    except Exception:
        if tries < 3:
            sleep(1)
            get_game_cover(game_name, tries + 1)
    return ""


def get_book_cover(book_title, tries=0):
    url_busca = f"https://www.googleapis.com/books/v1/volumes?q={book_title}"
    try:
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

                book_title = livro.get("title", book_title)
    except:
        pass

    if tries < 2:
        sleep(1)
        return get_book_cover(book_title, tries=tries + 1)
    return ""


def get_movie_cover(imdb_id):
    url_busca = f"https://api.themoviedb.org/3/find/{imdb_id}?external_source=imdb_id"
    response = requests.get(
        url_busca, headers={"Authorization": f"Bearer {TMDB_API_KEY}"}
    )

    if response.status_code == 200:
        dados = response.json()
        if dados["movie_results"]:
            filme = dados["movie_results"][0]
            caminho_poster = filme["poster_path"]
            overview = filme["overview"]
            if caminho_poster:
                url_imagem = f"https://image.tmdb.org/t/p/w500{caminho_poster}"
                return url_imagem, overview

    return "", ""


type_map = {
    "filme": ["data/filme_tratados.csv", get_movie_cover, "id", True],
    "livro": ["data/livro_tratados.csv", get_book_cover, "titulo", False],
    "jogo": ["data/jogo_tratados.csv", get_game_cover, "titulo", False],
}


def update_images(type_data, position=0):
    dataset_path, get_cover_func, id_field, has_desc = type_map[type_data]
    dataset = pd.read_csv(dataset_path)
    if "imagem" not in dataset.columns:
        dataset["imagem"] = ""

    if "descricao" not in dataset.columns:
        dataset["descricao"] = ""

    dataset = dataset.sort_values(by="rating", ascending=False).reset_index(drop=True)
    dataset.drop_duplicates(subset=["titulo"], inplace=True)
    updated = 0
    dataset.fillna({"imagem": ""}, inplace=True)
    dataset.fillna({id_field: ""}, inplace=True)
    with open(f"data/logs/{type_data}_sem_capa.txt", "w", encoding="utf-8") as log_file:
        pass

    with tqdm.tqdm(desc=type_data, total=len(dataset), position=position) as pbar:
        for index, row in dataset.iterrows():
            pbar.update(1)
            if row["imagem"]:
                continue

            identifier = row[id_field]
            if identifier:
                cover_url = get_cover_func(identifier)
            else:
                cover_url = None

            if has_desc:
                cover_url, desc = cover_url
                dataset.at[index, "descricao"] = desc

            if not cover_url:
                with open(
                    f"data/logs/{type_data}_sem_capa.txt", "a", encoding="utf-8"
                ) as log_file:
                    log_file.write(f"{row['titulo']} - {identifier}\n")

            dataset.at[index, "imagem"] = cover_url
            sleep(0.25)
            updated += 1

            if updated % 10 == 0:
                dataset.to_csv(dataset_path, index=False)
    dataset.to_csv(dataset_path, index=False)


if __name__ == "__main__":
    from concurrent.futures import ThreadPoolExecutor

    categorias = ["filme", "jogo", "livro"]
    with ThreadPoolExecutor(max_workers=3) as executor:
        for i, cat in enumerate(categorias):
            executor.submit(update_images, cat, i)

    print("\n" * len(categorias))
    print("Processo finalizado.")
