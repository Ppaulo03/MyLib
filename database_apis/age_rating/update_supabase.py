from supabase import create_client, Client
from os import getenv
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm

load_dotenv(override=True)
SUPABASE_URL = getenv("SUPABASE_URL")
SUPABASE_KEY = getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


cats = ["anime", "filme", "serie", "manga", "jogo"]
for cat in cats:
    dataset = pd.read_csv(f"data/ratings/{cat}_dataset_com_ratings.csv")
    dataset["categoria"] = cat
    dataset.drop(columns=["id"], inplace=True)
    dataset.fillna({"ano_lancamento": 0, "classificacao": 16}, inplace=True)
    dataset["classificacao"] = dataset["classificacao"].astype(int)
    dataset["ano_lancamento"] = dataset["ano_lancamento"].astype(int)
    dataset = dataset.to_dict(orient="records")

    for i in tqdm(
        range(0, len(dataset), 1000), total=len(dataset) // 1000 + 1, desc=cat
    ):
        batch = dataset[i : i + 1000]

        try:
            response = supabase.rpc(
                "bulk_update_classificacao", {"payload": batch}
            ).execute()
        except Exception as e:
            print(f"Erro no RPC: {e}")

    print("Atualização concluída.")
