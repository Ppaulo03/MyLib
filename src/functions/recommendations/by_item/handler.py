from common.decorators import lambda_wrapper
from common.responses import success, not_found
from interface import ItemRecommendationRequest
from service import process_recommendations, MediaNotFoundError


@lambda_wrapper(model=ItemRecommendationRequest)
def lambda_handler(request: ItemRecommendationRequest, context):
    try:
        results = process_recommendations(
            user_id=request.user_id,
            source_id=int(request.id),
            source_category=request.category,
            target_category=request.target_category,
            limit=5,
        )

        if request.target_category:
            return success(
                {"recommendations": results.get(request.target_category, [])}
            )

        return success({"recommendations": results})

    except MediaNotFoundError:
        return not_found()
