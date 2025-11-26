import time
from utils import save_to_supabase
import pandas as pd

dataset = pd.read_csv("data/livros.csv")
dataset = dataset.fillna(
    {"ano": 0, "paginas": 0, "rating": 0, "autor": "", "editora": ""}
)
registros = []
for index, row in dataset.iterrows():
    registros.append(
        {
            "categoria": "livro",
            "titulo": row["titulo"],
            "descricao": "",
            "ano_lancamento": row["ano"],
            "metadata": {
                "autor": row["autor"],
                "editora": row["editora"],
                "paginas": int(row["paginas"]),
                "rating": row["rating"],
            },
        }
    )

    while len(registros) >= 100:
        lote = registros[:100]
        save_to_supabase(lote)
        registros = registros[100:]
        time.sleep(1)
