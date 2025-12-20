from datetime import datetime, timezone
from common.dynamo_client import db_client


def update_item(user_id: str, category: str, item_id: str, update_data: dict) -> list:
    sk_value = f"item#{category.lower()}#{item_id}"
    config_sk = "can_6_star"

    response = db_client.query_items(user_id, sk_value, limit=1)
    if not response["items"]:
        raise FileNotFoundError("Item não encontrado.")

    old_item = response["items"][0]
    old_rating = float(old_item.get("rating", 0))

    transact_items = []
    transact_items.append(
        _build_update_tx(
            db_client=db_client,
            key={"user_id": user_id, "sk": sk_value},
            data=update_data,
        )
    )

    new_rating = update_data.get("rating")
    if new_rating is not None:
        is_becoming_super = new_rating > 5 and old_rating <= 5
        is_losing_super = new_rating <= 5 and old_rating > 5

        if is_becoming_super:
            transact_items.append(
                _build_update_tx(
                    db_client=db_client,
                    key={"user_id": user_id, "sk": config_sk},
                    data={category: False},
                    condition_expr=f"#{category} = :trueVal",
                    condition_values={":trueVal": True},
                )
            )

        elif is_losing_super:
            transact_items.append(
                _build_update_tx(
                    db_client=db_client,
                    key={"user_id": user_id, "sk": config_sk},
                    data={category: True},
                )
            )

    db_client.execute_transaction(transact_items)
    return list(update_data.keys())


def _build_update_tx(db_client, key, data, condition_expr=None, condition_values=None):
    if "updated_at" not in data:
        data["updated_at"] = datetime.now(timezone.utc).isoformat()

    update_parts = []
    attr_names = {}
    raw_values_map = {}

    for k, v in data.items():
        key_placeholder = f"#{k}"
        val_placeholder = f":{k}"

        update_parts.append(f"{key_placeholder} = {val_placeholder}")
        attr_names[key_placeholder] = k
        raw_values_map[val_placeholder] = v

    if condition_values:
        for k, v in condition_values.items():
            raw_values_map[k] = v

    attr_values = db_client.to_dynamo_json(raw_values_map)

    dynamo_key = db_client.to_dynamo_json(key)
    tx_item = {
        "Update": {
            "TableName": db_client.table_name,
            "Key": dynamo_key,
            "UpdateExpression": f"SET {', '.join(update_parts)}",
            "ExpressionAttributeNames": attr_names,
            "ExpressionAttributeValues": attr_values,
        }
    }

    if condition_expr:
        tx_item["Update"]["ConditionExpression"] = condition_expr

    return tx_item
