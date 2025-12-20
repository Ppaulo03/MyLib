from common.decorators import AuthRequest


class DeleteItemRequest(AuthRequest):
    id: str
    category: str
