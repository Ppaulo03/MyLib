import os
import boto3
from boto3.dynamodb.conditions import Key
from collections import defaultdict
from supabase import create_client, Client

from dotenv import load_dotenv

load_dotenv(override=True)
dynamodb = boto3.resource("dynamodb")


def get_table_name_from_ssm():
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name="/mylib/prod/tabela_dados")
    return response["Parameter"]["Value"]


table = dynamodb.Table(get_table_name_from_ssm())


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all_cognito_users():
    client = boto3.client("cognito-idp")
    users = []
    paginator = client.get_paginator("list_users")

    pages = paginator.paginate(
        UserPoolId=os.getenv("MY_USER_POOL_ID"), AttributesToGet=["sub"]
    )

    for page in pages:
        users.extend(user["Username"] for user in page["Users"])

    return users


def get_profile(user_id):
    key_condition = Key("user_id").eq(str(user_id)) & Key("sk").begins_with("item#")
    response = table.query(KeyConditionExpression=key_condition)
    items = response.get("Items", [])

    consumed_ids = []
    genre_scores = defaultdict(float)

    for item in items:
        midia_id = int(item["sk"].split("#")[-1])
        consumed_ids.append(midia_id)

        rating = float(item["rating"]) if item.get("rating") is not None else 0
        status = str(item["status"]) if item.get("status") else "plan_to_watch"
        if rating <= 0 or status in ["plan_to_watch", "abandoned"]:
            continue

        genres = item.get("unified_genres", [])

        weight = 0
        if rating >= 4:
            weight = rating
        elif rating == 3:
            weight = 1
        else:
            weight = 0.1

        for genre in genres:
            genre_scores[genre] += weight

    sorted_genres = sorted(genre_scores.items(), key=lambda x: x[1], reverse=True)
    top_genres_list = [g[0] for g in sorted_genres]
    return consumed_ids, top_genres_list


def get_recomendations(consumed_ids, top_genres_list):
    try:
        response = supabase.rpc(
            "get_recommendations",
            {
                "p_consumed_ids": consumed_ids,
                "p_top_genres": top_genres_list,
                "p_limit": 10,
            },
        ).execute()

        return response.data

    except Exception as e:
        print(f"Erro ao buscar recomendações: {e}")


def save_to_dynamo(user_id, recomendacoes):
    sk_value = "recommendations"
    item = {
        "user_id": user_id,
        "sk": sk_value,
        "recommendations": recomendacoes,
    }
    table.put_item(Item=item)


def main():
    cognito_users = fetch_all_cognito_users()
    for user_id in cognito_users:
        consumed_ids, top_genres_list = get_profile(user_id)
        recomendacoes = get_recomendations(consumed_ids, top_genres_list)
        save_to_dynamo(user_id, recomendacoes)


if __name__ == "__main__":
    main()
