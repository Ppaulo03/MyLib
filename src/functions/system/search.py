from common.supabase_funcs import search_midia
import json


def lambda_handler(event, context):
    try:
        params = event.get("queryStringParameters") or {}
        search_term = params.get("q", "")
        year = params.get("year")
        category = params.get("category")

        SIMILARIDADE_MINIMA = 0.6
        match_encontrado = False
        resultados, score_max = search_midia(search_term, year, category)
        if score_max >= SIMILARIDADE_MINIMA:
            match_encontrado = True
            print(f"Match encontrado com score {score_max}")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(resultados),
        }

    except Exception as e:
        print(f"Erro no Lambda: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
