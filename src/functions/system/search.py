import json
import os
from supabase import create_client, Client
from get_covers import get_movie_cover, get_game_cover, get_book_cover
from pydantic import BaseModel, Field

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

image_handlers = {
    "filme": get_movie_cover,
    "jogo": get_game_cover,
    "livro": get_book_cover,
}


class ListItemsItem(BaseModel):
    id: int = Field(..., alias="id")
    category: str = Field(..., alias="categoria")
    title: str = Field(..., alias="titulo")
    description: str = Field("", alias="descricao")
    release_year: int = Field(None, alias="ano_lancamento")
    cover_url: str = Field("", alias="imagem")
    genres: list[str] = Field([], alias="generos")
    unified_genres: list[str] = Field([], alias="generos_unificados")
    metadata: dict = Field({}, alias="metadata")


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

        data = [ListItemsItem(**item).model_dump() for item in response.data]
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(data),
        }

    except Exception as e:
        print(f"Erro no Lambda: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
