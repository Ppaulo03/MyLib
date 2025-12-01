import json
import os
from supabase import create_client, Client
from get_covers import get_movie_cover, get_game_cover, get_book_cover
from pydantic import BaseModel, Field, field_validator
from typing import Optional
import ast

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

image_handlers = {
    "filme": get_movie_cover,
    "jogo": get_game_cover,
    "livro": get_book_cover,
}


def parse_stringified_list(v):

    if v is None:
        return None

    if isinstance(v, list):
        return v

    if isinstance(v, str):
        try:
            return ast.literal_eval(v)
        except (ValueError, SyntaxError):
            return [v]
    return v


class metadataItem(BaseModel):
    # books
    author: Optional[str] = Field("", alias="autor")
    pages: Optional[int] = Field(0, alias="paginas")
    editor: Optional[str] = Field("", alias="editora")

    # movies
    duration: Optional[str] = Field("", alias="duracao")
    director: Optional[str] = Field("", alias="diretor")
    star: Optional[str] = Field("", alias="star")

    # games
    platform: Optional[str] = Field("", alias="plataformas")
    developers: Optional[str] = Field("", alias="desenvlovedores")

    # anime
    episodes: Optional[int] = Field(0, alias="episodios")

    @field_validator("platform", "developers", mode="before")
    @classmethod
    def parse_stringified_list(cls, v):
        if v is None:
            return ""
        if isinstance(v, list):
            return ",".join(v)

        if isinstance(v, str):
            v = v.strip()  # Remove espaços extras
            try:
                parsed = ast.literal_eval(v)
                if isinstance(parsed, list):
                    return ",".join(parsed)
                else:
                    return ",".join([parsed])
            except (ValueError, SyntaxError):
                return ",".join([v])
        return v


class ListItemsItem(BaseModel):
    id: int = Field(..., alias="id")
    category: str = Field(..., alias="categoria")
    title: str = Field(..., alias="titulo")
    description: Optional[str] = Field("", alias="descricao")
    release_year: Optional[int] = Field(None, alias="ano_lancamento")
    cover_url: Optional[str] = Field("", alias="imagem")
    genres: list[str] = Field([], alias="generos")
    unified_genres: list[str] = Field([], alias="generos_unificados")
    metadata: metadataItem = Field(metadataItem(), alias="metadata")


def json_encode_item(item: ListItemsItem) -> dict:
    encoded = item.model_dump()
    encoded["metadata"] = {}
    if item.category == "livro":
        encoded["metadata"]["author"] = item.metadata.author
        encoded["metadata"]["pages"] = item.metadata.pages
        encoded["metadata"]["editor"] = item.metadata.editor

    elif item.category == "filme":
        encoded["metadata"]["duration"] = item.metadata.duration
        encoded["metadata"]["director"] = item.metadata.director
        encoded["metadata"]["star"] = item.metadata.star

    elif item.category == "jogo":
        encoded["metadata"]["platform"] = item.metadata.platform
        encoded["metadata"]["developers"] = item.metadata.developers

    elif item.category == "anime":
        encoded["metadata"]["episodes"] = item.metadata.episodes

    return encoded


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

        data = [json_encode_item(ListItemsItem(**item)) for item in response.data]
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(data),
        }

    except Exception as e:
        print(f"Erro no Lambda: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
