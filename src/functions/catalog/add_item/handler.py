from common.decorators import lambda_wrapper
from common.responses import created, not_acceptable
from service import create_item, SuperlikeExhaustedError
from interface import AddItemRequest


@lambda_wrapper(model=AddItemRequest)
def lambda_handler(request: AddItemRequest, context):

    try:
        sk_value = f"item#{request.category.lower()}#{request.id}"
        create_item(request, sk_value)
        return created({"message": "Item adicionado com sucesso", "item_sk": sk_value})

    except SuperlikeExhaustedError as e:
        return not_acceptable(str(e))

    except Exception as e:
        raise e
