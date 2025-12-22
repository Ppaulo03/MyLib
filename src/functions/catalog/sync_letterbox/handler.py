from common.decorators import lambda_wrapper
from common.responses import success
from interface import SyncLetterboxRequest
from service import get_letterboxd_films, match_movies_rpc


@lambda_wrapper(model=SyncLetterboxRequest)
def lambda_handler(request: SyncLetterboxRequest, context):
    items = get_letterboxd_films(request.username)
    matches = match_movies_rpc(items)
    return success({"matched_films": len(matches)})
