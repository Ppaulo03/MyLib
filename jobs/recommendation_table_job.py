import os
import boto3
from boto3.dynamodb.conditions import Key
from supabase import create_client, Client
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd

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

CATEGORIAS_ALVO = ["anime", "filme", "jogo", "livro"]
LIMIT_PER_CATEGORY = 10


def fetch_all_cognito_users():
    client = boto3.client("cognito-idp")
    users = []
    paginator = client.get_paginator("list_users")

    pages = paginator.paginate(
        UserPoolId=os.getenv("MY_USER_POOL_ID"), AttributesToGet=["sub"]
    )

    for page in pages:
        for user in page["Users"]:
            sub = next(
                (attr["Value"] for attr in user["Attributes"] if attr["Name"] == "sub"),
                None,
            )
            if sub:
                users.append(sub)
    return users


def get_profile(user_id):
    key_condition = Key("user_id").eq(str(user_id)) & Key("sk").begins_with("item#")
    response = table.query(KeyConditionExpression=key_condition)
    items = response.get("Items", [])
    profile = []
    for item in items:
        sk = item.get("sk").split("#")
        item_id = sk[-1]
        categoria = sk[-2]
        rating = float(item["rating"]) if item.get("rating") is not None else 0
        status = str(item["status"]) if item.get("status") else "planned"

        if rating <= 0 or status in ["planned", "abandoned"]:
            continue
        profile.append(
            {
                "user_id": user_id,
                "categoria": categoria,
                "item_id": item_id,
                "rating": rating,
            }
        )
    return profile


def calculate_recomendations(raw_data):
    df = pd.DataFrame(raw_data)
    df["unique_id"] = df["categoria"] + "_" + df["item_id"].astype(str)
    meta_lookup = (
        df[["unique_id", "categoria", "item_id"]]
        .drop_duplicates()
        .set_index("unique_id")
        .to_dict("index")
    )

    pivot_table = df.pivot_table(
        index="user_id", columns="unique_id", values="rating"
    ).fillna(0)

    item_matrix = pivot_table.T
    similarity_matrix = cosine_similarity(item_matrix)

    similarity_df = pd.DataFrame(
        similarity_matrix, index=item_matrix.index, columns=item_matrix.index
    )

    recommends_list = []

    for source_unique_id in similarity_df.columns:

        source_meta = meta_lookup[source_unique_id]
        scores = similarity_df[source_unique_id].sort_values(ascending=False)
        counts = {cat: 0 for cat in CATEGORIAS_ALVO}

        for target_unique_id, score in scores.items():
            if target_unique_id == source_unique_id:
                continue
            if score < 0.1:
                break

            target_meta = meta_lookup[target_unique_id]
            target_cat = target_meta["categoria"]

            if (
                target_cat in CATEGORIAS_ALVO
                and counts[target_cat] < LIMIT_PER_CATEGORY
            ):

                recommends_list.append(
                    {
                        "origem_id": source_meta["item_id"],
                        "origem_categoria": source_meta["categoria"],
                        "alvo_id": target_meta["item_id"],
                        "alvo_categoria": target_cat,
                        "score": round(score, 4),
                    }
                )
                counts[target_cat] += 1
            if all(c >= LIMIT_PER_CATEGORY for c in counts.values()):
                break
    return recommends_list


def upload_to_supabase(data_list, batch_size=1000):
    total = len(data_list)
    for i in range(0, total, batch_size):
        batch = data_list[i : i + batch_size]
        try:
            response = (
                supabase.table("recommendations")
                .upsert(
                    batch,
                    on_conflict="origem_id, origem_categoria, alvo_id, alvo_categoria",
                    ignore_duplicates=False,
                )
                .execute()
            )
            print(f"Lote {i} a {i+len(batch)} enviado com sucesso!")
        except Exception as e:
            print(f"Erro no lote {i}: {e}")


def main():
    cognito_users = fetch_all_cognito_users()
    raw_data = []
    for user_id in cognito_users:
        raw_data.extend(get_profile(user_id))
    recommends_list = calculate_recomendations(raw_data)
    upload_to_supabase(recommends_list)


if __name__ == "__main__":
    main()
