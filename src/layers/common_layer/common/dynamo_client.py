import os
import json
import base64
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Optional
from decimal import Decimal
from loguru import logger


class DynamoClient:
    def __init__(self):
        self.table_name = os.environ.get("TABLE_NAME", "TabelaDados")

        self.resource = boto3.resource("dynamodb")
        self.client = boto3.client("dynamodb")
        self.table = self.resource.Table(self.table_name)
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()

    def _replace_decimals(self, obj):
        """Recursively converts Decimal to int/float for JSON serialization."""
        if isinstance(obj, list):
            return [self._replace_decimals(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: self._replace_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            # Check if it's an integer stored as Decimal
            return int(obj) if obj % 1 == 0 else float(obj)
        return obj

    def _sanitize_float(self, obj):
        """Recursively converts float to Decimal for DynamoDB storage."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self._sanitize_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._sanitize_float(i) for i in obj]
        return obj

    def _encode_token(self, key_dict: dict) -> Optional[str]:
        if not key_dict:
            return None
        # Ensure keys in the token are JSON serializable (handle Decimals in PK/SK)
        clean_key = self._replace_decimals(key_dict)
        json_str = json.dumps(clean_key)
        return base64.urlsafe_b64encode(json_str.encode()).decode()

    def _decode_token(self, token_str: str) -> Optional[dict]:
        if not token_str:
            return None
        try:
            json_str = base64.urlsafe_b64decode(token_str.encode()).decode()
            return json.loads(json_str)
        except Exception:
            logger.warning("Failed to decode pagination token.")
            return None

    def to_dynamo_json(self, data: dict) -> dict:
        """Converts standard Dict to DynamoDB JSON format ({"S": "val"} etc)."""
        clean_data = self._sanitize_float(data)
        return self.serializer.serialize(clean_data)["M"]

    def put_item(self, item: dict) -> bool:
        try:
            # DynamoDB requires Floats to be Decimals
            clean_item = self._sanitize_float(item)
            self.table.put_item(Item=clean_item)
            return True
        except ClientError as e:
            logger.error(f"Error putting item: {e.response['Error']['Message']}")
            raise

    def query_items(self, user_id, sk_prefix=None, limit=1000, next_token=None):
        try:
            key_condition = Key("user_id").eq(str(user_id))
            if sk_prefix:
                key_condition &= Key("sk").begins_with(str(sk_prefix))

            query_kwargs = {"KeyConditionExpression": key_condition, "Limit": limit}

            if next_token:
                start_key = self._decode_token(next_token)
                if start_key:
                    query_kwargs["ExclusiveStartKey"] = start_key

            response = self.table.query(**query_kwargs)

            return {
                "items": self._replace_decimals(response.get("Items", [])),
                "next_token": self._encode_token(response.get("LastEvaluatedKey")),
                "count": response["Count"],
            }
        except ClientError as e:
            logger.error(f"Error querying items: {e.response['Error']['Message']}")
            raise

    def delete_item(self, user_id, sk) -> bool:
        try:
            self.table.delete_item(Key={"user_id": user_id, "sk": sk})
            return True
        except ClientError as e:
            logger.error(f"Error deleting item: {e.response['Error']['Message']}")
            raise

    def update_item(self, user_id, sk, update_data: dict) -> bool:
        # Filter out keys that cannot be updated (Keys)
        data = {
            k: v for k, v in update_data.items() if k not in ["user_id", "sk", "id"]
        }

        if not data:
            return False

        # Add metadata
        data["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Sanitize inputs (Float -> Decimal)
        data = self._sanitize_float(data)

        # Build UpdateExpression dynamically
        update_parts = []
        attr_names = {}
        attr_values = {}

        for key, value in data.items():
            key_placeholder = f"#{key}"
            val_placeholder = f":{key}"

            update_parts.append(f"{key_placeholder} = {val_placeholder}")
            attr_names[key_placeholder] = key
            attr_values[val_placeholder] = value

        try:
            self.table.update_item(
                Key={"user_id": user_id, "sk": sk},
                UpdateExpression="SET " + ", ".join(update_parts),
                ExpressionAttributeNames=attr_names,
                ExpressionAttributeValues=attr_values,
                ReturnValues="NONE",  # Saves bandwidth, we don't usually need the old/new values back
            )
            return True
        except ClientError as e:
            logger.error(f"Error updating item: {e.response['Error']['Message']}")
            raise

    def execute_transaction(self, transact_items: list) -> bool:
        try:
            self.client.transact_write_items(TransactItems=transact_items)
            return True
        except ClientError as e:
            logger.error(f"Transaction failed: {e.response['Error']['Message']}")
            raise

    def build_update_tx(self, key, data, condition_expr=None, condition_values=None):
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

        attr_values = self.to_dynamo_json(raw_values_map)

        dynamo_key = self.to_dynamo_json(key)
        tx_item = {
            "Update": {
                "TableName": self.table_name,
                "Key": dynamo_key,
                "UpdateExpression": f"SET {', '.join(update_parts)}",
                "ExpressionAttributeNames": attr_names,
                "ExpressionAttributeValues": attr_values,
            }
        }

        if condition_expr:
            tx_item["Update"]["ConditionExpression"] = condition_expr

        return tx_item


db_client = DynamoClient()
