from typing import Optional, Dict, List
from collections import defaultdict
from pydantic import BaseModel, Field
from common.decorators import lambda_wrapper, AuthRequest
from common.supabase_funcs import get_bulk_midia_info
from common.dynamo_client import db_client
from common.responses import success
from utils import get_6_star_dict


class GetLibraryRequest(AuthRequest):
    limit: int = Field(default=1000, gt=0, le=2000)
    next_token: Optional[str] = None
    categoria: Optional[str] = None


@lambda_wrapper(model=GetLibraryRequest)
def lambda_handler(request: GetLibraryRequest, context):

    prefix = "item#"
    if request.categoria:
        prefix += f"{request.categoria.lower()}#"

    items_data = db_client.query_items(
        user_id=request.user_id,
        sk_prefix=prefix,
        limit=request.limit,
        next_token=request.next_token,
    )

    raw_items = items_data["items"]
    if raw_items:
        ids_para_buscar = [it["sk"].split("#")[-1] for it in raw_items]
        infos_bulk = get_bulk_midia_info(ids_para_buscar)
        raw_items = [
            {**it, **infos_bulk.get(it["sk"].split("#")[-1], {})} for it in raw_items
        ]

    final_items = raw_items
    if not request.categoria:
        grouped_items = defaultdict(list)
        for it in raw_items:
            category_key = it["sk"].split("#")[-2]
            grouped_items[category_key].append(it)
        final_items = dict(grouped_items)

    return success(
        {
            "items": final_items,
            "count": items_data["count"],
            "next_token": items_data["next_token"],
            "6star": get_6_star_dict(request.user_id).model_dump(),
        }
    )
