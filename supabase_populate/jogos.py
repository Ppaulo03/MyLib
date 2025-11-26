import time
from utils import save_to_supabase
import pandas as pd

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
    registros.append(
        {
            "categoria": "jogo",
            "titulo": row["Title"],
            "descricao": row["Summary"],
            "ano_lancamento": ano_lancamento,
            "metadata": {
                "generos": row["Genres"],
                "plataformas": row["Platforms"],
                "rating": row["Rating"],
            },
        }
    )

    while len(registros) >= 100:
        lote = registros[:100]
        save_to_supabase(lote)
        registros = registros[100:]
        time.sleep(1)
