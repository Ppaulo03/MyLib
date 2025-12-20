from common.decorators import AuthRequest, Field
from typing import Optional


class GetLibraryRequest(AuthRequest):
    limit: int = Field(default=1000, gt=0, le=2000)
    next_token: Optional[str] = None
    categoria: Optional[str] = None
