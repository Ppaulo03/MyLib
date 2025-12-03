from get_covers import get_movie_cover, get_game_cover, get_book_cover
from common.supabase_funcs import search_midia
import json

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

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(search_midia(search_term, year, category)),
        }

    except Exception as e:
        print(f"Erro no Lambda: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
