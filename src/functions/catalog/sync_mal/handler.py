from common.decorators import lambda_wrapper
from common.responses import success
from interface import SyncMALRequest
from service import my_anime_list_getter, sync_database


@lambda_wrapper(model=SyncMALRequest)
def lambda_handler(request: SyncMALRequest, context):
    items = my_anime_list_getter(request.username, request.category)
    sync_database(items, request.category, request.user_id, request.override)
    return success({"message": "Sync completed successfully"})
