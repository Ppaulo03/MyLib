from common.decorators import AuthRequest
from pydantic import Field
from typing import Optional


class UserRecommendationRequest(AuthRequest):
    target_category: Optional[str] = Field(
        None,
        description="The category for which recommendations are sought. If not provided, recommendations across all categories will be returned.",
    )
