import pandas as pd
import asyncio
import aiohttp
import os
import math

# A API Jikan (Não oficial) é pública e não precisa de chave (.env)
# Mas mantemos os imports para não quebrar compatibilidade
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Configurações ---
input_path = r"ETL\data\anime.csv"
output_path = r"ETL\data\anime_titulos.csv"

# JIKAN API LIMITS: ~3 requests/second.
# Se aumentar isso, você tomará erro 429 constantemente.
MAX_CONCURRENT_REQUESTS = 1
BATCH_SIZE = 50


async def get_anime_titles_async(session, mal_id, semaphore):
    """
    Busca dados na Jikan API (v4).
    """
    url = f"https://api.jikan.moe/v4/anime/{mal_id}"

    async with semaphore:
        # Pausa de segurança para respeitar o rate limit da Jikan
        await asyncio.sleep(0.5)

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data_json = await response.json()
                    data = data_json.get("data", {})

                    # --- Lógica de Títulos ---
                    titles_list = []

                    # 1. Pega o Título em Inglês (Prioridade solicitada)
                    english_title = data.get("title_english")
                    if english_title:
                        titles_list.append(english_title)

                    # 2. Pega o Título Padrão (Geralmente Romaji)
                    default_title = data.get("title")
                    if default_title:
                        titles_list.append(default_title)

                    # 3. Pega a lista completa de alternativos (Japonês, Sinônimos, etc)
                    # A Jikan v4 retorna uma lista de objetos em 'titles'
                    raw_titles = data.get("titles", [])
                    for item in raw_titles:
                        t = item.get("title")
                        if t:
                            titles_list.append(t)

                    # Remove duplicatas mantendo a lista limpa
                    unique_titles = list(set(titles_list))

                    return {
                        "id": mal_id,
                        "titulos_alternativos": str(unique_titles),
                        "status": "ok",
                    }

                elif response.status == 429:
                    print(f"ID {mal_id}: Rate limit (429). Pausando 4s...")
                    # Backoff maior para a Jikan recuperar o fôlego
                    await asyncio.sleep(4)
                    return await get_anime_titles_async(session, mal_id, semaphore)

                elif response.status == 404:
                    # Anime não existe ou foi deletado do MAL
                    return {
                        "id": mal_id,
                        "titulos_alternativos": "[]",
                        "status": "not_found",
                    }

                else:
                    return {
                        "id": mal_id,
                        "titulos_alternativos": "[]",
                        "status": "error",
                    }

        except Exception as e:
            print(f"Erro ID {mal_id}: {e}")
            return {"id": mal_id, "titulos_alternativos": "[]", "status": "error"}


async def process_batch(ids_batch, df_original):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = [
            get_anime_titles_async(session, id_val, semaphore) for id_val in ids_batch
        ]
        results = await asyncio.gather(*tasks)

    valid_results = [r for r in results if r is not None]
    df_results = pd.DataFrame(valid_results)

    df_batch_original = df_original[df_original["id"].isin(ids_batch)].copy()

    if not df_results.empty:
        # Ajuste aqui as colunas que existem no seu CSV original de animes.
        # Assumindo 'id' e 'titulo' (ou 'nome')
        cols_to_merge = ["id", "titulo"]
        existing_cols = [c for c in cols_to_merge if c in df_batch_original.columns]

        df_final = pd.merge(
            df_batch_original[existing_cols],
            df_results[["id", "titulos_alternativos"]],
            on="id",
            how="left",
        )
        return df_final
    return pd.DataFrame()


def main():
    print(f"Lendo CSV de entrada: {input_path}...")

    if not os.path.exists(input_path):
        print(f"Erro: Arquivo {input_path} não encontrado.")
        return

    df_input = pd.read_csv(input_path)
    df_input = df_input.dropna(subset=["id"])
    df_input["id"] = df_input["id"].astype(int)

    ids_processados = set()
    if os.path.isfile(output_path):
        try:
            df_existing = pd.read_csv(output_path, usecols=["id"])
            ids_processados = set(df_existing["id"].tolist())
            print(f"Resume: {len(ids_processados)} já processados.")
        except:
            print("Arquivo de saída vazio ou erro de leitura. Iniciando do zero.")

    df_to_process = df_input[~df_input["id"].isin(ids_processados)].copy()
    all_ids = df_to_process["id"].tolist()
    total_ids = len(all_ids)

    print(f"Total a processar: {total_ids}")

    if total_ids == 0:
        return

    num_batches = math.ceil(total_ids / BATCH_SIZE)

    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch_ids = all_ids[start_idx:end_idx]

        print(f"Processando lote {i+1}/{num_batches} ({len(batch_ids)} animes)...")

        # Jikan é lenta, tenha paciência neste passo
        df_batch_result = asyncio.run(process_batch(batch_ids, df_to_process))

        write_header = not os.path.isfile(output_path)
        df_batch_result.to_csv(output_path, mode="a", header=write_header, index=False)


if __name__ == "__main__":
    import time

    start = time.time()
    main()
    end = time.time()
    print(f"Tempo total: {end - start:.2f} segundos")
