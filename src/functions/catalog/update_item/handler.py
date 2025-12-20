from common.decorators import lambda_wrapper
from common.dynamo_client import ClientError
from common.responses import not_found, success, not_acceptable
from interface import UpdateItemRequest
from service import update_item
from loguru import logger
import json


@lambda_wrapper(model=UpdateItemRequest)
def lambda_handler(request: UpdateItemRequest, context):

    update_data = request.model_dump(
        exclude_unset=True, exclude={"user_id", "category", "id"}
    )

    if not update_data:
        return success({"message": "Nenhum dado novo para atualizar."})

    try:
        updated_fields = update_item(
            user_id=request.user_id,
            category=request.category,
            item_id=request.id,
            update_data=update_data,
        )

        return success(
            {
                "message": "Atualizado com sucesso",
                "updated_fields": updated_fields,
            }
        )

    except FileNotFoundError:
        return not_found("Item não encontrado.")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "TransactionCanceledException":
            if "ConditionalCheckFailed" in e.response["Error"]["Message"]:
                return not_acceptable(
                    f"Você não tem um Superlike disponível para {request.category}."
                )
        logger.error(json.dumps(e.response, default=str))
        raise e
