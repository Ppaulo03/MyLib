import time
from utils import save_to_supabase
import pandas as pd

dataset = pd.read_csv("data/filmes.csv")
dataset = dataset.fillna(
    {"year": 0, "duration": "", "genre": "", "director": "", "rating_imdb": 0}
)
registros = []
for index, row in dataset.iterrows():
    registros.append(
        {
            "categoria": "filme",
            "titulo": row["title"],
            "descricao": "",
            "ano_lancamento": int(row["year"]),
            "metadata": {
                "duracao": row["duration"],
                "genre": row["genre"],
                "diretor": row["director"],
                "rating": row["rating_imdb"],
                "duration": row["duration"],
            },
        }
    )

    while len(registros) >= 100:
        lote = registros[:100]
        save_to_supabase(lote)
        registros = registros[100:]
        time.sleep(1)
