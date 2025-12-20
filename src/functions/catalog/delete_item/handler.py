from common.decorators import lambda_wrapper
from common.responses import success
from common.dynamo_client import db_client
from interface import DeleteItemRequest


@lambda_wrapper(model=DeleteItemRequest)
def lambda_handler(request: DeleteItemRequest, context):
    sk_value = f"item#{request.category.lower()}#{request.id}"
    db_client.delete_item(request.user_id, sk_value)
    return success({"message": "Item removido com sucesso", "deleted_id": request.id})
