import json
from common.dynamo_client import db_client
from botocore.exceptions import ClientError
from interface import AddItemRequest
from datetime import datetime, timezone
from loguru import logger


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
    user_id_str = str(request.user_id)
    sk_value_str = str(sk_value)

    # 2. Gera o corpo do item (sem as chaves) usando o serializador
    # Removemos user_id e sk do dict 'item' antes de serializar o resto para evitar duplicidade
    item_body = {k: v for k, v in item.items() if k not in ["user_id", "sk"]}
    item_dynamo_body = db_client.to_dynamo_json(item_body)

    # 3. Monta o Item final manualmente mesclando as chaves hardcoded com o corpo
    final_item = {
        "user_id": {"S": user_id_str},  # <--- Manual e Explícito
        "sk": {"S": sk_value_str},  # <--- Manual e Explícito
    }
    final_item.update(item_dynamo_body)  # Adiciona o resto dos campos

    # 4. Debug final antes de enviar
    print(f"PAYLOAD FINAL PUT: {json.dumps(final_item, default=str)}")

    transact_items = [
        {
            "Put": {
                "TableName": db_client.table_name,
                "Item": final_item,  # Usa o item montado manualmente
            }
        },
        {
            "Update": {
                "TableName": db_client.table_name,
                # Monta a Key manualmente também
                "Key": {"user_id": {"S": user_id_str}, "sk": {"S": "can_6_star"}},
                "UpdateExpression": "SET #cat = :false",
                "ConditionExpression": "attribute_not_exists(#cat) OR #cat = :true",
                "ExpressionAttributeNames": {"#cat": category},
                "ExpressionAttributeValues": {
                    ":false": {"BOOL": False},  # Manual
                    ":true": {"BOOL": True},  # Manual
                },
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
