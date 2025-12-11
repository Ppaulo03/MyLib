from supabase import Client, create_client
from pydantic import BaseModel, Field, field_validator
from typing import Optional
import os, ast

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


class MetadataItem(BaseModel):
    # Books
    author: Optional[str] = Field(None, alias="autor")
    pages: Optional[int] = Field(None, alias="paginas")
    editor: Optional[str] = Field(None, alias="editora")

    # Movies
    duration: Optional[str] = Field(None, alias="duracao")
    director: Optional[str] = Field(None, alias="diretor")
    star: Optional[str] = Field(None, alias="star")

    # Games
    platform: Optional[str] = Field("", alias="plataformas")
    developers: Optional[str] = Field("", alias="desenvlovedores")

    # anime
    episodes: Optional[int] = Field(0, alias="episodios")
    mal_id: Optional[str | int] = Field("", alias="id_original")

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
                return (
                    ",".join(parsed) if isinstance(parsed, list) else ",".join([parsed])
                )
            except (ValueError, SyntaxError):
                return ",".join([v])
        return v


class ListItemsItem(BaseModel):
    id: str = Field(..., alias="id")
    title: str = Field(..., alias="titulo")
    category: str = Field(..., alias="categoria")
    genres: list[str] = Field([], alias="generos")
    unified_genres: list[str] = Field([], alias="generos_unificados")
    metadata: MetadataItem = Field(MetadataItem(), alias="metadata")
    cover_url: Optional[str] = Field("", alias="imagem")
    release_year: Optional[int] = Field(None, alias="ano_lancamento")
    description: Optional[str] = Field("", alias="descricao")

    @field_validator("id", mode="before")
    @classmethod
    def parse_stringified_list(cls, v):
        return "" if v is None else str(v)


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
        encoded["metadata"]["mal_id"] = item.metadata.mal_id

    return encoded


def get_midia_info(media_id):
    response = supabase.table("midia").select("*").eq("id", media_id).execute()
    db_item = response.data[0] if response.data else {}
    return json_encode_item(ListItemsItem(**db_item)) if db_item else {}


def get_bulk_midia_info(media_ids):
    response = supabase.table("midia").select("*").in_("id", media_ids).execute()
    midia_dict = {}
    for db_item in response.data:
        processed_item = json_encode_item(ListItemsItem(**db_item))
        midia_dict[processed_item["id"]] = processed_item
    return midia_dict


def search_midia(search_term, year=None, category=None):

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
    return [json_encode_item(ListItemsItem(**item)) for item in response.data]


def get_fallback_recommendations(consumed_ids, top_genres, limit=5):
    rpc_response = supabase.rpc(
        "get_recommendations",
        {
            "p_consumed_ids": consumed_ids,
            "p_top_genres": top_genres,
            "p_limit": limit,
        },
    ).execute()
    return [json_encode_item(ListItemsItem(**item)) for item in rpc_response.data]


def get_item_recommendation(source_id, source_category, target_category=None):
    response = (
        supabase.table("recommendations")
        .select("*")
        .eq("origem_id", source_id)
        .eq("origem_categoria", source_category)
        .order("score", desc=True)
        .execute()
    )

    recommendations = {"filme": [], "jogo": [], "anime": [], "livro": []}
    for item in response.data:
        if target_category and target_category != item["alvo_categoria"]:
            continue
        if m := get_midia_info(item["alvo_id"]):
            recommendations[item["alvo_categoria"]].append(m)
    return recommendations
