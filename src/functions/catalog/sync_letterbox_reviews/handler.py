from common.decorators import lambda_wrapper
from common.responses import success
from interface import SyncLetterboxReviewRequest
from service import process_review


@lambda_wrapper(model=SyncLetterboxReviewRequest)
def lambda_handler(request: SyncLetterboxReviewRequest, context):
    updated = process_review(request)
    return success({"status": "processed", "updated": updated, "sk": request.sk})
