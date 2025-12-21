from common.decorators import lambda_wrapper
from common.responses import success
from interface import GetLibraryRequest
from service import fetch_library_data, enrich_and_group_items


@lambda_wrapper(model=GetLibraryRequest)
def lambda_handler(request: GetLibraryRequest, context):
    prefix = f"item#{request.category.lower()}#" if request.category else "item#"
    items_data, six_star_data = fetch_library_data(
        user_id=request.user_id,
        prefix=prefix,
        limit=request.limit,
        next_token=request.next_token,
    )

    final_items = enrich_and_group_items(
        raw_items=items_data.get("items", []),
        group_by_category=not bool(request.category),
    )

    return success(
        {
            "items": final_items,
            "count": items_data.get("count"),
            "next_token": items_data.get("next_token"),
            "6star": six_star_data,
        }
    )
