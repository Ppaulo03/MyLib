from common.decorators import lambda_wrapper
from common.supabase_funcs import search_midia
from common.responses import success
from interface import SearchRequest


@lambda_wrapper(model=SearchRequest)
def lambda_handler(request: SearchRequest, context):
    resultados, _ = search_midia(request.q, request.year, request.category)
    return success(resultados)
