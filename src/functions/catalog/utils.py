from common.dynamo_client import db_client
from pydantic import BaseModel, Field


class Can6Star(BaseModel):
    filme: bool = Field(True)
    anime: bool = Field(True)
    jogo: bool = Field(True)
    livro: bool = Field(True)


def get_6_star_dict(user_id):
    items = db_client.query_items(
        user_id=user_id,
        sk_prefix="can_6_star",
    )
    return Can6Star(**items["items"][0]) if items["items"] else Can6Star()
