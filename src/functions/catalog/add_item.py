import json
from datetime import datetime, timezone
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client
from supabase import Client, create_client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


@lambda_wrapper(required_fields=["id", "category", "title"])
def lambda_handler(event, context):
    try:

        body = event["parsed_body"]
        user_id = event["user_id"]

        media_id = str(body["id"])
        category = body["category"].lower()
        sk_value = f"item#{category}#{media_id}"

        response = supabase.table("midia").select("*").eq("id", media_id).execute()
        if response.data:
            db_item = response.data[0]
        else:
            db_item = {}
        item = {
            "user_id": user_id,
            "sk": sk_value,
            "title": body["title"],
            "status": body.get("status", "planned"),
            "rating": body.get("rating", None),
            "progress": body.get("progress", 0),
            "review": body.get("review", ""),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "genres": db_item.get("generos", []),
            "unified_genres": db_item.get("generos_unificados", []),
            "metadata": db_item.get("metadata", {}),
            "cover_url": db_item.get("imagem", ""),
            "release_year": db_item.get("ano_lancamento", None),
            "description": db_item.get("descricao", ""),
        }
        db_client.put_item(item)

        return {
            "statusCode": 201,
            "body": json.dumps(
                {"message": "Item adicionado com sucesso", "item_sk": sk_value}
            ),
        }

    except Exception as e:
        print(f"Erro: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Erro interno ao processar item"}),
        }
