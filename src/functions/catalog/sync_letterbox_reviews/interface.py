from pydantic import Field, field_validator, HttpUrl
from common.decorators import AuthRequest


class SyncLetterboxReviewRequest(AuthRequest):
    sk: str = Field(...)
    review_link: str = Field(...)
    override: bool = Field(False)
