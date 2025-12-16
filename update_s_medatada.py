from supabase import create_client, Client
from os import getenv
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
import json
import ast

load_dotenv(override=True)
SUPABASE_URL = getenv("SUPABASE_URL")
SUPABASE_KEY = getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# 1. Carregar Dataset
df = pd.read_csv("data_raw/series_metadata.csv")


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


df["categoria"] = "serie"
df.fillna({"ano_lancamento": 0}, inplace=True)
df["ano_lancamento"] = df["ano_lancamento"].astype(int)
df["metadata"] = df["metadata"].apply(parse_mixed_data)
print(df.head())
BATCH_SIZE = 1000
records = df[["titulo", "ano_lancamento", "categoria", "metadata"]].to_dict(
    orient="records"
)

print(f"Atualizando metadata de {len(records)} séries...")
for i in tqdm(range(0, len(records), BATCH_SIZE)):
    batch = records[i : i + BATCH_SIZE]

    try:
        # Chama a NOVA função RPC criada
        supabase.rpc("bulk_update_metadata", {"payload": batch}).execute()

    except Exception as e:
        print(f"Erro no lote {i}: {e}")
