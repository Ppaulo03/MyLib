from botocore.exceptions import ClientError
from interface import AddItemRequest
from datetime import datetime, timezone
from common.dynamo_client import db_client
from loguru import logger
import json


class SuperlikeExhaustedError(Exception):
    """Erro levantado quando o usuário não tem mais superlikes para a categoria."""

    pass


def create_item(request: AddItemRequest, sk_value: str):
    timestamp = datetime.now(timezone.utc).isoformat()
    item = {
        "user_id": request.user_id,
        "sk": sk_value,
        "title": request.title,
        "status": request.status,
        "rating": request.rating,
        "progress": request.progress,
        "review": request.review,
        "created_at": timestamp,
        "updated_at": timestamp,
    }

    if not request.rating or request.rating <= 5:
        db_client.put_item(item)
        return

    category = request.category.lower()
    transact_items = [
        {
            "Put": {
                "TableName": db_client.table_name,
                "Item": db_client.to_dynamo_json(item),
            }
        },
        {
            "Update": {
                "TableName": db_client.table_name,
                "Key": db_client.to_dynamo_json(
                    {"user_id": request.user_id, "sk": "can_6_star"}
                ),
                "UpdateExpression": "SET #cat = :false",
                "ConditionExpression": "attribute_not_exists(#cat) OR #cat = :true",
                "ExpressionAttributeNames": {"#cat": category},
                "ExpressionAttributeValues": db_client.to_dynamo_json(
                    {":false": False, ":true": True}
                ),
            }
        },
    ]

    try:
        db_client.execute_transaction(transact_items)
    except ClientError as e:

        if e.response["Error"]["Code"] == "TransactionCanceledException":
            reasons = e.response.get("CancellationReasons", [])
            if len(reasons) > 1 and "ConditionalCheckFailed" in reasons[1].get(
                "Code", ""
            ):
                raise SuperlikeExhaustedError("Superlike já gasto para esta categoria!")
        logger.error(json.dumps(e.response, default=str))
        raise e
