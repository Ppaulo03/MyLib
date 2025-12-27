import boto3
import time
import os
from dotenv import load_dotenv

load_dotenv(override=True)

SOURCE_TABLE = "MyLib-devel-TabelaDados-1CAJC8K7D07W5"
TARGET_TABLE = "MyLib-devel-TabelaDados-1ESAFN085RRYG"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def migrate():
    dynamo = boto3.resource(
        "dynamodb",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    source = dynamo.Table(SOURCE_TABLE)
    target = dynamo.Table(TARGET_TABLE)

    print(f"Lendo de {SOURCE_TABLE}...")
    response = source.scan()
    items = response["Items"]
    print(f"Encontrados {len(items)} itens. Iniciando cópia...")
    with target.batch_writer() as batch:
        for i, item in enumerate(items):
            batch.put_item(Item=item)
            if i % 50 == 0:
                print(f"Copiados {i}...")
    while "LastEvaluatedKey" in response:
        response = source.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items = response["Items"]
        with target.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        print(f"Mais lote copiado...")
    print("Migração concluída com sucesso!")


if __name__ == "__main__":
    migrate()
