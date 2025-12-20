from common.decorators import AuthRequest
from pydantic import Field
from typing import Optional


class ItemRecommendationRequest(AuthRequest):
    id: str = Field(
        ...,
        description="The unique identifier of the item for which recommendations are requested.",
    )
    category: str = Field(
        ..., description="The category of the source item (e.g., 'movie', 'book')."
    )
    target_category: Optional[str] = Field(
        None,
        description="The category for which recommendations are sought. If not provided, recommendations across all categories will be returned.",
    )
