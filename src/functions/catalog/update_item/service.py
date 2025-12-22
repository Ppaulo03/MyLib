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
        db_client.build_update_tx(
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
                db_client.build_update_tx(
                    key={"user_id": user_id, "sk": config_sk},
                    data={category: False},
                    condition_expr=f"attribute_not_exists(#{category}) OR #{category} = :trueVal",
                    condition_values={":trueVal": True},
                )
            )

        elif is_losing_super:
            transact_items.append(
                db_client.build_update_tx(
                    key={"user_id": user_id, "sk": config_sk},
                    data={category: True},
                )
            )

    db_client.execute_transaction(transact_items)
    return list(update_data.keys())
