import json
from common.decorators import lambda_wrapper
from common.dynamo_client import db_client
from supabase import Client, create_client
import os, ast

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
from pydantic import BaseModel, Field, field_validator
from typing import Optional


class MetadataItem(BaseModel):
    # Books
    author: Optional[str] = Field(None, alias="autor")
    pages: Optional[int] = Field(None, alias="paginas")
    editor: Optional[str] = Field(None, alias="editora")

    # Movies
    duration: Optional[list[str]] = Field(None, alias="duracao")
    director: Optional[list[str]] = Field(None, alias="diretor")
    star: Optional[str] = Field(
        None, alias="star"
    )  # Se o nome não muda, não precisa de alias, mas mal não faz

    # Games
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


def get_db_info(media_id):
    response = supabase.table("midia").select("*").eq("id", media_id).execute()
    if response.data:
        db_item = response.data[0]
    else:
        db_item = {}

    metadata = db_item.get("metadata", {})
    metadata_clean = MetadataItem(**metadata)
    return {
        "id": db_item.get("id"),
        "category": db_item.get("categoria"),
        "genres": db_item.get("generos", []),
        "unified_genres": db_item.get("generos_unificados", []),
        "metadata": metadata_clean.model_dump(
            by_alias=False, exclude_unset=True, exclude_none=True
        ),
        "cover_url": db_item.get("imagem", ""),
        "release_year": db_item.get("ano_lancamento", None),
        "description": db_item.get("descricao", ""),
    }


@lambda_wrapper()
def lambda_handler(event, context):

    user_id = event.get("user_id")
    query_params = event.get("queryStringParameters") or {}

    limit = int(query_params.get("limit", 1000))
    next_token = query_params.get("next_token")

    prefix = "item#"
    if categoria := query_params.get("categoria", ""):
        prefix += f"{str(categoria).lower()}#"

    items = db_client.query_items(
        user_id=user_id,
        sk_prefix=prefix,
        limit=limit,
        next_token=next_token,
    )

    items["items"] = [
        {**it, **get_db_info(it["sk"].split("#")[-1])} for it in items["items"]
    ]

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "items": items["items"],
                "count": items["count"],
                "next_token": items["next_token"],
            }
        ),
    }
