import os
import json
import base64
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from datetime import datetime, timezone


class DynamoClient:
    def __init__(self):
        table_name = os.environ.get("TABLE_NAME", "TabelaDados")
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)

    def _replace_decimals(self, obj):
        if isinstance(obj, list):
            return [self._replace_decimals(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: self._replace_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return obj

    def _encode_token(self, key_dict):
        """Converte o dict da chave (LastEvaluatedKey) em string Base64"""
        if not key_dict:
            return None
        json_str = json.dumps(key_dict)
        return base64.urlsafe_b64encode(json_str.encode()).decode()

    def _decode_token(self, token_str):
        """Converte string Base64 de volta para dict (ExclusiveStartKey)"""
        if not token_str:
            return None
        try:
            json_str = base64.urlsafe_b64decode(token_str.encode()).decode()
            return json.loads(json_str)
        except Exception:
            return None

    def put_item(self, item):
        try:
            self.table.put_item(Item=item)
            return True
        except Exception as e:
            print(f"Erro ao salvar no DynamoDB: {str(e)}")
            raise e

    def query_items(self, user_id, sk_prefix=None, limit=1000, next_token=None):
        try:
            key_condition = Key("user_id").eq(user_id)

            if sk_prefix:
                key_condition = key_condition & Key("sk").begins_with(sk_prefix)

            query_kwargs = {"KeyConditionExpression": key_condition, "Limit": limit}
            start_key = self._decode_token(next_token)
            if start_key:
                query_kwargs["ExclusiveStartKey"] = start_key
            response = self.table.query(**query_kwargs)
            items = response.get("Items", [])
            last_key = response.get("LastEvaluatedKey", None)
            return {
                "items": self._replace_decimals(items),
                "next_token": self._encode_token(last_key),
                "count": response["Count"],
            }

        except Exception as e:
            print(f"Erro ao buscar no DynamoDB: {str(e)}")
            raise e

    def delete_item(self, user_id, sk):
        """
        Remove um item do banco baseado na PK e SK exatas.
        """
        try:
            self.table.delete_item(Key={"user_id": user_id, "sk": sk})
            return True
        except Exception as e:
            print(f"Erro ao deletar no DynamoDB: {str(e)}")
            raise e

    def update_item(self, user_id, sk, update_data):
        """
        Atualiza campos específicos de um item (PATCH).
        :param update_data: Dict com campos a atualizar (ex: {'rating': 10, 'status': 'completed'})
        """
        try:
            data = {
                k: v
                for k, v in update_data.items()
                if k not in ["id", "user_id", "categoria"]
            }

            if not data:
                return False

            data["updated_at"] = datetime.now(timezone.utc).isoformat()

            update_expr = []
            attr_names = {}
            attr_values = {}

            for key, value in data.items():
                key_placeholder = f"#{key}"
                val_placeholder = f":{key}"

                update_expr.append(f"{key_placeholder} = {val_placeholder}")
                attr_names[key_placeholder] = key
                attr_values[val_placeholder] = value

            expression = "SET " + ", ".join(update_expr)

            self.table.update_item(
                Key={"user_id": user_id, "sk": sk},
                UpdateExpression=expression,
                ExpressionAttributeNames=attr_names,
                ExpressionAttributeValues=attr_values,
            )
            return True
        except Exception as e:
            print(f"Erro ao atualizar no DynamoDB: {str(e)}")
            raise e


db_client = DynamoClient()
