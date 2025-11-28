import time
from utils import save_to_supabase
import pandas as pd
from genre_map import GENRE_MAP


def buscar_e_salvar_jogos():
    dataset = pd.read_csv("data/jogos.csv")

    dataset = dataset.fillna(
        {"Release_Date": "", "Summary": "", "Rating": 0, "Genres": "", "Platforms": ""}
    )
    registros = []
    for index, row in dataset.iterrows():
        try:
            ano_lancamento = int(row["Release_Date"].split(",")[1].strip())
        except Exception:
            ano_lancamento = 0

        genres = (
            [g.strip() for g in row["Genres"]]
            if isinstance(row["Genres"], list)
            else [row["Genres"]]
        )
        generos_unificados = set()
        for g in genres:
            g_lower = g.lower()
            if g_lower in GENRE_MAP:
                generos_unificados.update(GENRE_MAP[g_lower])
        registros.append(
            {
                "categoria": "jogo",
                "titulo": row["Title"],
                "descricao": row["Summary"],
                "ano_lancamento": ano_lancamento,
                "generos": genres,
                "generos_unificados": list(generos_unificados),
                "rating": row["Rating"],
                "metadata": {
                    "plataformas": row["Platforms"],
                },
            }
        )

    while len(registros) >= 100:
        lote = registros[:100]
        save_to_supabase(lote)
        registros = registros[100:]
        time.sleep(1)


if __name__ == "__main__":
    buscar_e_salvar_jogos()
