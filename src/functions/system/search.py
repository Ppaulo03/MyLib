from common.decorators import lambda_wrapper
from common.supabase_funcs import search_midia
import json


@lambda_wrapper()
def lambda_handler(event, context):
    try:
        params = event.get("queryStringParameters") or {}
        search_term = params.get("q", "")
        year = params.get("year")
        category = params.get("category")
        resultados, score_max = search_midia(search_term, year, category)
        print(event.get("user_age"))

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(resultados),
        }

    except Exception as e:
        print(f"Erro no Lambda: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
