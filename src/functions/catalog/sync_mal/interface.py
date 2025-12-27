from common.decorators import AuthRequest
from pydantic import Field, field_validator
from typing import Optional


class SyncMALRequest(AuthRequest):
    username: str = Field(
        ...,
        description="The username of the MyAnimeList account to sync with.",
    )
    category: str = Field(
        ...,
        description="The category of media to sync (e.g., 'anime' or 'manga').",
    )
    override: Optional[bool] = Field(
        False,
        description="Whether to override existing entries in the database.",
    )

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("mal_username cannot be an empty string.")
        return v.strip()
