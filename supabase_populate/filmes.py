import time
from utils import save_to_supabase
import pandas as pd
from genre_map import GENRE_MAP


def buscar_e_salvar_filmes():
    dataset = pd.read_csv("data/filmes.csv")

    dataset = dataset.fillna(
        {"year": 0, "duration": "", "genre": "", "director": "", "rating_imdb": 0}
    )
    top = 0
    registros = []
    for index, row in dataset.iterrows():
        genres = [g.strip().lower() for g in row["genre"].split(",")]
        generos_unificados = set()
        for g in genres:
            if g in GENRE_MAP:
                generos_unificados.update(GENRE_MAP[g])

        registros.append(
            {
                "categoria": "filme",
                "titulo": row["title"],
                "descricao": "",
                "ano_lancamento": int(row["year"]),
                "generos": genres,
                "generos_unificados": list(generos_unificados),
                "rating": (row.get("rating_imdb") or 0) / 2,
                "metadata": {
                    "duracao": row["duration"],
                    "diretor": row["director"],
                    "duration": row["duration"],
                },
            }
        )

    while len(registros) >= 100:
        lote = registros[:100]
        save_to_supabase(lote)
        registros = registros[100:]
        time.sleep(1)


if __name__ == "__main__":
    buscar_e_salvar_filmes()
