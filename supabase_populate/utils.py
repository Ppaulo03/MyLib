from supabase import create_client, Client
from os import getenv

SUPABASE_URL = getenv("SUPABASE_URL", "https://pwlltrcwgtwzckoirjbq.supabase.co")
SUPABASE_KEY = getenv("SUPABASE_KEY", "sb_secret_sECM5VHspjR4hAR44H_Tkw_PWG-p6qp")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def save_to_supabase(data):
    on_conflict = "categoria, titulo, ano_lancamento"
    try:
        supabase.table("midia").upsert(
            data, on_conflict=on_conflict, ignore_duplicates=True
        ).execute()
        print("Sucesso! Dados sincronizados.")
    except Exception as e:
        print(f"Ocorreu um erro ao salvar: {e}")
