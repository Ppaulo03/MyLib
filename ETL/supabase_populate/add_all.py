from dotenv import load_dotenv

load_dotenv(override=True)
from utils import save_to_supabase
import time
import pandas as pd
import ast
from tqdm import tqdm
import json


def parse_mixed_data(text):
    if pd.isna(text) or text == "":
        return None

    try:
        clean_text = text.replace('""', '"')
        return json.loads(clean_text)
    except json.JSONDecodeError:
        try:
            py_friendly = (
                clean_text.replace("null", "None")
                .replace("true", "True")
                .replace("false", "False")
            )
            return ast.literal_eval(py_friendly)
        except (ValueError, SyntaxError):
            return text


def buscar_e_salvar_dataset(path, cateoria):
    dataset = pd.read_csv(path, date_format={"generos": list})
    dataset["generos"] = dataset["generos"].apply(parse_mixed_data)
    if "generos_unificados" not in dataset.columns:
        dataset["generos_unificados"] = [[] for _index in range(len(dataset))]

    else:
        dataset["generos_unificados"] = dataset["generos_unificados"].apply(
            parse_mixed_data
        )

    if "titulos_alternativos" not in dataset.columns:
        dataset["titulos_alternativos"] = [[] for _index in range(len(dataset))]
    else:
        dataset["titulos_alternativos"] = dataset["titulos_alternativos"].apply(
            parse_mixed_data
        )

    dataset["metadata"] = dataset["metadata"].apply(parse_mixed_data)
    dataset.fillna(
        {"ano_lancamento": 0, "descricao": "", "rating": 0, "imagem": ""},
        inplace=True,
    )
    if "classificacao" in dataset.columns:
        dataset["classificacao"] = dataset["classificacao"].fillna(14).astype(int)

    registros = [
        {
            "categoria": cateoria,
            "titulo": row["titulo"],
            "titulos_alternativos": row.get("titulos_alternativos", []),
            "descricao": row["descricao"],
            "ano_lancamento": int(row["ano_lancamento"]),
            "generos": row["generos"],
            "generos_unificados": list(row["generos_unificados"]),
            "rating": row["rating"],
            "classificacao": row.get("classificacao", 14),
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
    # paths = ["filme", "jogo", "livro", "anime", "manga", "serie"]
    paths = ["manga"]
    print("Iniciando a população do banco de dados Supabase...")
    for p in paths:
        print(f"Populando {p}...")
        buscar_e_salvar_dataset(f"ETL/data/{p}.csv", p)
