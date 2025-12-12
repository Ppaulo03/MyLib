from dotenv import load_dotenv

load_dotenv(override=True)
from utils import save_to_supabase
import time
import pandas as pd
import ast
from tqdm import tqdm


def buscar_e_salvar_dataset(path, cateoria):
    dataset = pd.read_csv(path, date_format={"generos": list})
    dataset["generos"] = dataset["generos"].apply(ast.literal_eval)
    dataset["generos_unificados"] = dataset["generos_unificados"].apply(
        ast.literal_eval
    )
    dataset["metadata"] = dataset["metadata"].apply(ast.literal_eval)
    dataset.fillna(
        {"ano_lancamento": 0, "descricao": "", "rating": 0, "imagem": ""},
        inplace=True,
    )
    registros = [
        {
            "categoria": cateoria,
            "titulo": row["titulo"],
            "descricao": row["descricao"],
            "ano_lancamento": int(row["ano_lancamento"]),
            "generos": row["generos"],
            "generos_unificados": list(row["generos_unificados"]),
            "rating": row["rating"],
            "metadata": row["metadata"],
            "imagem": row["imagem"],
        }
        for index, row in dataset.iterrows()
    ]
    with tqdm(total=len(registros)) as pbar:
        while registros:
            lote = registros[:100]
            save_to_supabase(lote)
            pbar.update(len(lote))
            registros = registros[100:]
            time.sleep(1)


if __name__ == "__main__":
    pahts = ["filme", "jogo", "livro", "anime"]
    paths = []
    print("Iniciando a população do banco de dados Supabase...")
    for p in pahts:
        print(f"Populando {p}...")
        buscar_e_salvar_dataset(f"data/{p}.csv", p)
