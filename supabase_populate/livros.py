import time
from utils import save_to_supabase
import pandas as pd


def buscar_e_salvar_livros():
    dataset = pd.read_csv("data/livros.csv")
    dataset = dataset.fillna(
        {"ano": 0, "paginas": 0, "rating": 0, "autor": "", "editora": "", "genero": ""}
    )
    registros = []
    for index, row in dataset.iterrows():
        genres = [g.strip().lower() for g in row["genero"].split("/")]
        generos_unificados = []

        registros.append(
            {
                "categoria": "livro",
                "titulo": row["titulo"],
                "descricao": "",
                "ano_lancamento": row["ano"],
                "generos": genres,
                "generos_unificados": list(generos_unificados),
                "rating": (row["rating"] or 0) / 2,
                "metadata": {
                    "autor": row["autor"],
                    "editora": row["editora"],
                    "paginas": int(row["paginas"]),
                },
            }
        )

    while len(registros) >= 100:
        lote = registros[:100]
        save_to_supabase(lote)
        registros = registros[100:]
        time.sleep(1)


if __name__ == "__main__":
    buscar_e_salvar_livros()
