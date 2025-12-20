from common.decorators import AuthRequest
from typing import Optional


class SearchRequest(AuthRequest):
    q: str
    year: Optional[str] = None
    category: Optional[str] = None
