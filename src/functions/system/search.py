import json
import os
from supabase import create_client, Client
from get_covers import get_movie_cover, get_game_cover, get_book_cover

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

image_handlers = {
    "filme": get_movie_cover,
    "jogo": get_game_cover,
    "livro": get_book_cover,
}


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

        to_update = []
        for item in response.data:
            categoria = item.get("categoria")
            if categoria in image_handlers and not item.get("imagem"):
                fetch_function = image_handlers[categoria]
                url_imagem = fetch_function(item.get("titulo", ""))
                if url_imagem:
                    item["imagem"] = url_imagem
                    to_update.append(item)

        if to_update:
            supabase.table("midia").upsert(to_update).execute()

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response.data),
        }

    except Exception as e:
        print(f"Erro no Lambda: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
