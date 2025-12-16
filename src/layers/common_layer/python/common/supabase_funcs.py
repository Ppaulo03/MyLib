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
    developers: Optional[str] = Field("", alias="desenvolvedores")

    # anime
    episodes: Optional[int] = Field(0, alias="episodios")
    mal_id: Optional[str | int] = Field("", alias="id_original")

    # manga
    type: Optional[str] = Field(None, alias="type")
    status: Optional[str] = Field(None, alias="status")
    volumes: Optional[int] = Field(None, alias="volumes")
    chapters: Optional[int] = Field(None, alias="chapters")
    authors: Optional[list[str]] = Field([], alias="authors")
    serializations: Optional[list[str]] = Field([], alias="serializations")

    # serie
    mean_runtime: Optional[int] = Field(0, alias="duracao_media")
    creators: Optional[str] = Field("", alias="criadores")
    main_cast: Optional[str] = Field("", alias="elenco_principal")
    total_seasons: Optional[int] = Field(0, alias="total_temporadas")

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
    age_rating: Optional[int] = Field(None, alias="classificacao")

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

    elif item.category == "manga":
        encoded["metadata"]["type"] = item.metadata.type
        encoded["metadata"]["status"] = item.metadata.status
        encoded["metadata"]["volumes"] = item.metadata.volumes
        encoded["metadata"]["chapters"] = item.metadata.chapters
        encoded["metadata"]["authors"] = item.metadata.authors
        encoded["metadata"]["serializations"] = item.metadata.serializations
    elif item.category == "serie":
        encoded["metadata"]["mean_runtime"] = item.metadata.mean_runtime
        encoded["metadata"]["creators"] = item.metadata.creators
        encoded["metadata"]["main_cast"] = item.metadata.main_cast
        encoded["metadata"]["total_seasons"] = item.metadata.total_seasons
    return encoded


def get_midia_info(media_id):
    response = supabase.table("midia").select("*").eq("id", media_id).execute()
    db_item = response.data[0] if response.data else {}
    return json_encode_item(ListItemsItem(**db_item)) if db_item else {}


def get_bulk_midia_info(media_ids, batch_size=200):
    midia_dict = {}
    if not media_ids:
        return midia_dict

    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    for batch_ids in chunk_list(media_ids, batch_size):
        try:
            response = (
                supabase.table("midia").select("*").in_("id", batch_ids).execute()
            )
            for db_item in response.data:
                processed_item = json_encode_item(ListItemsItem(**db_item))
                midia_dict[processed_item["id"]] = processed_item
        except Exception as e:
            print(f"Erro ao processar lote: {e}")
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

    score_max = 0.0
    if response.data:
        score_max = response.data[0].get("score_similaridade", 0.0)
    return [
        json_encode_item(ListItemsItem(**item)) for item in response.data
    ], score_max


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

    target_ids = []
    for item in response.data:
        if target_category and target_category != item["alvo_categoria"]:
            continue
        target_ids.append(item["alvo_id"])

    all_media_records = get_bulk_midia_info(target_ids)
    recommendations = {}

    for midia in all_media_records.values():
        cat = midia.get("categoria")
        if cat not in recommendations:
            recommendations[cat] = []
        recommendations[cat].append(midia)
    return recommendations
