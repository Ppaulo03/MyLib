from common.decorators import AuthRequest
from pydantic import Field, field_validator
from typing import Optional


class AddItemRequest(AuthRequest):
    id: str
    category: str
    title: str
    status: Optional[str] = Field(default="planned")
    rating: Optional[float] = None
    progress: Optional[int] = Field(default=0, ge=0)
    review: Optional[str] = Field(default="")

    @field_validator("id", mode="before")
    @classmethod
    def validate_category(cls, v):
        if isinstance(v, str):
            return v.strip()
        elif isinstance(v, int):
            return str(v)
        return v
