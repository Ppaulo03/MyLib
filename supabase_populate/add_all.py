from dotenv import load_dotenv

load_dotenv(override=True)
from animes import buscar_e_salvar_animes
from utils import save_to_supabase
import time
import pandas as pd
import ast


def buscar_e_salvar_dataset(path, cateoria):
    dataset = pd.read_csv(path, date_format={"generos": list})
    registros = []
    dataset["generos"] = dataset["generos"].apply(ast.literal_eval)
    dataset["generos_unificados"] = dataset["generos_unificados"].apply(
        ast.literal_eval
    )
    dataset["metadata"] = dataset["metadata"].apply(ast.literal_eval)
    dataset.fillna(
        {
            "ano_lancamento": 0,
            "descricao": "",
            "rating": 0,
        },
        inplace=True,
    )
    for index, row in dataset.iterrows():
        registros.append(
            {
                "categoria": cateoria,
                "titulo": row["titulo"],
                "descricao": row["descricao"],
                "ano_lancamento": row["ano_lancamento"],
                "generos": row["generos"],
                "generos_unificados": row["generos_unificados"],
                "rating": row["rating"],
                "metadata": row["metadata"],
            }
        )

    while len(registros) >= 100:
        lote = registros[:100]
        save_to_supabase(lote)
        registros = registros[100:]
        time.sleep(1)


if __name__ == "__main__":
    pahts = ["livro", "jogo", "filme"]
    print("Iniciando a população do banco de dados Supabase...")
    for p in pahts:
        print(f"Populando {p}...")
        buscar_e_salvar_dataset(f"data/{p}_tratados.csv", p)
    print("Populando animes...")
    buscar_e_salvar_animes()
    print("População concluída.")
