import boto3, os
from supabase import create_client, Client
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
import ast
import re

load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = "midia"
BATCH_SIZE = 1000
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_table_name_from_ssm():
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name="/mylib/prod/tabela_dados")
    return response["Parameter"]["Value"]


dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(get_table_name_from_ssm())
CSV_PATH = "games_data.csv"
ITEM_CATEGORY = "jogo"


def parse_string_to_obj(valor, tipo_padrao):
    if pd.isna(valor) or valor == "" or valor is None:
        return tipo_padrao

    if isinstance(valor, (list, dict)):
        return valor

    try:
        return ast.literal_eval(valor)
    except (ValueError, SyntaxError):
        try:
            return (
                [item.strip() for item in re.findall(r"(?:[^,(]|\([^)]*\))+", valor)]
                if isinstance(tipo_padrao, list)
                else tipo_padrao
            )
        except Exception:
            return tipo_padrao


def buscar_candidatos_exclusao(sync_time):
    candidatos_exclusao = []
    batch_size = 1000
    start = 0

    while True:
        end = start + batch_size - 1
        response = (
            supabase.table(SUPABASE_TABLE)
            .select("id, titulo")
            .eq("categoria", ITEM_CATEGORY)
            .lt("updated_at", sync_time)
            .range(start, end)
            .execute()
        )

        pagina_atual = response.data
        if not pagina_atual:
            break

        candidatos_exclusao.extend(pagina_atual)
        if len(pagina_atual) < batch_size:
            break

        start += batch_size

    return candidatos_exclusao


def preview_upsert(dataset_local, amostra_qtd=5):
    print(f"--- INICIANDO SIMULAÇÃO (Amostra de {amostra_qtd} itens) ---")

    amostra = dataset_local[:amostra_qtd]
    for item in amostra:
        titulo = item["titulo"]
        ano_lancamento = item["ano_lancamento"]

        response = (
            supabase.table(SUPABASE_TABLE)
            .select("id, titulo, ano_lancamento, categoria")
            .eq("titulo", titulo)
            .eq("ano_lancamento", ano_lancamento)
            .eq("categoria", ITEM_CATEGORY)
            .execute()
        )

        dados_banco = response.data

        print(f"\nItem Local: '{titulo}' ({ano_lancamento})")

        if dados_banco:
            item_existente = dados_banco[0]
            print(f"STATUS: UPDATE (Já existe no ID: {item_existente['id']})")
        else:
            print(f"STATUS: CREATE (Será criado um novo registro)")


def excluir_itens_em_lotes(lista_de_ids):
    if not lista_de_ids:
        print("Nenhum item para excluir.")
        return

    TOTAL = len(lista_de_ids)
    BATCH_SIZE = 500

    print(f"Iniciando exclusão de {TOTAL} itens...")
    with tqdm(total=TOTAL, desc="Deletando") as pbar:
        for i in range(0, TOTAL, BATCH_SIZE):
            lote_ids = lista_de_ids[i : i + BATCH_SIZE]
            try:
                res = supabase.table("midia").delete().in_("id", lote_ids).execute()
                pbar.update(len(lote_ids))
                print(f"Lote {i} a {i+len(lote_ids)} excluído.")

            except Exception as e:
                print(f"Erro ao excluir lote {i}: {e}")

    print("Limpeza concluída!")


def sync_and_check_references():

    sync_time = datetime.now(timezone.utc).isoformat()
    df = pd.read_csv(CSV_PATH)

    if "num_avaliacoes" in df.columns:
        df = df.drop(columns=["num_avaliacoes"])

    df["generos_unificados"] = [[] for _ in range(len(df))]

    if "id" in df.columns:
        df = df.drop(columns=["id"])

    df["generos"] = df["generos"].apply(lambda x: parse_string_to_obj(x, []))
    df["metadata"] = df["metadata"].apply(lambda x: parse_string_to_obj(x, {}))
    df["ano_lancamento"] = df["ano_lancamento"].fillna(0).astype(int)
    df.fillna(
        {"descricao": "", "imagem": "", "ano_lancamento": 0, "rating": 0},
        inplace=True,
    )
    df = df.drop_duplicates(subset=["titulo", "ano_lancamento"])
    records = df.to_dict(orient="records")

    dados_completos = []
    for item in records:
        dados_completos.append(
            {
                **item,
                "categoria": ITEM_CATEGORY,
                "updated_at": sync_time,
            }
        )

    total_items = len(dados_completos)

    print(f"Total de itens para processar: {total_items}")

    with tqdm(total=total_items, unit="docs", desc="Enviando Upsert") as pbar:
        for i in range(0, total_items, BATCH_SIZE):
            batch = dados_completos[i : i + BATCH_SIZE]
            try:
                res = (
                    supabase.table(SUPABASE_TABLE)
                    .upsert(batch, on_conflict="titulo, ano_lancamento, categoria")
                    .execute()
                )

                pbar.update(len(batch))

            except Exception as e:
                print(f"Erro no lote {i}: {e}")
                return

    print("Sincronização de Upsert finalizada!")

    print("Buscando itens obsoletos no Supabase...")
    candidatos_exclusao = buscar_candidatos_exclusao(sync_time)
    if not candidatos_exclusao:
        print("Nenhum item para excluir. Sincronização limpa!")
        return

    ids_para_excluir = {item["id"]: item["titulo"] for item in candidatos_exclusao}
    print(
        f"Encontrados {len(ids_para_excluir)} itens no Supabase que não existem mais no local."
    )

    print("Verificando referências no DynamoDB...")
    filter_expression = Attr("sk").begins_with(f"item#{ITEM_CATEGORY}#")
    response = table.scan(FilterExpression=filter_expression)
    items = response.get("Items", [])
    items_ids = [int(item["sk"].split("#")[-1]) for item in items]

    can_delete_safely = True
    for i in items_ids:
        if i in ids_para_excluir:
            print(f"Item ID {i} está referenciado no DynamoDB - {ids_para_excluir[i]}")
            can_delete_safely = False

    if can_delete_safely:
        print("Nenhuma referência encontrada. Procedendo com exclusão...")
        excluir_itens_em_lotes(list(ids_para_excluir.keys()))


sync_and_check_references()
