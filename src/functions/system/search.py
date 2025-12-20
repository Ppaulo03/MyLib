from common.decorators import lambda_wrapper
from common.supabase_funcs import search_midia
from common.responses import success


@lambda_wrapper()
def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    search_term = params.get("q", "")
    year = params.get("year")
    category = params.get("category")
    resultados, _ = search_midia(search_term, year, category)
    return success(resultados)
