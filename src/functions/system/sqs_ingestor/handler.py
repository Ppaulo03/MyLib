import os
import json
import boto3
from common.decorators import AuthRequest, lambda_wrapper
from common.responses import success
from pydantic import Field, field_validator
from typing import Optional

sqs = boto3.client("sqs")
QUEUE_URL = os.environ.get("TARGET_QUEUE_URL")


class IngestRequest(AuthRequest):
    username: str = Field(..., description="Username do serviço externo")
    override: bool = Field(False, description="Forçar atualização")
    category: Optional[str] = Field(None, description="Categoria do conteúdo")

    @field_validator("username")
    @classmethod
    def validate_user(cls, v):
        if not v or not v.strip():
            raise ValueError("Username não pode ser vazio")
        return v.strip()


@lambda_wrapper(model=IngestRequest)
def lambda_handler(request: IngestRequest, context):
    if not QUEUE_URL:
        raise ValueError("TARGET_QUEUE_URL não configurada.")

    payload_dict = (
        request.model_dump() if hasattr(request, "model_dump") else request.model_dump()
    )
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(payload_dict))
    return success({"status": "queued", "message": "Solicitação recebida."})
