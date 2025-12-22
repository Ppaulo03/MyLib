import pandas as pd
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import ast

load_dotenv(override=True)

# --- Configurações ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ARQUIVO_CSV = r"ETL\data\anime_titulos.csv"
NOME_TABELA = "midia"

# Conectar ao Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

df = pd.read_csv(ARQUIVO_CSV)

df["ano_lancamento"] = df["ano_lancamento"].fillna(0).astype(int)
df["categoria"] = "anime"
df["titulos_alternativos"] = df["titulos_alternativos"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else []
)

# Selecionar colunas exatas da tabela temporária
dados = df[["titulo", "ano_lancamento", "titulos_alternativos", "categoria"]].to_dict(
    orient="records"
)

# Upload em Batch
BATCH_SIZE = 2000
print(f"Subindo {len(dados)} registros para 'temp_titulos'...")

for i in range(0, len(dados), BATCH_SIZE):
    batch = dados[i : i + BATCH_SIZE]
    try:
        supabase.table("temp_titulos").insert(batch).execute()
        print(f"Lote {i} ok")
    except Exception as e:
        print(f"Erro no lote {i}: {e}")
