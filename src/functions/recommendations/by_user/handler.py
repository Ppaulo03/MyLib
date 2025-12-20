from common.decorators import lambda_wrapper
from common.responses import success
from interface import UserRecommendationRequest
from service import process_user_recommendations


@lambda_wrapper(model=UserRecommendationRequest)
def lambda_handler(request: UserRecommendationRequest, context):

    recommendations = process_user_recommendations(
        user_id=request.user_id, target_category=request.target_category, limit=24
    )

    return success({"recommendations": recommendations})
