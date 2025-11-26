import json
import os
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def lambda_handler(event, context):
    try:

        params = event.get("queryStringParameters") or {}
        search_term = params.get("q", "")
        year = params.get("year")
        category = params.get("category")

        year = int(year) if year and year.isdigit() else None
        if not category:
            category = None

        response = supabase.rpc(
            "buscar_midias",
            {
                "termo_busca": search_term,
                "filtro_ano": year,
                "filtro_categoria": category,
            },
        ).execute()

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response.data),
        }

    except Exception as e:
        print(f"Erro no Lambda: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
