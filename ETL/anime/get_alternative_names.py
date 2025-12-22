import pandas as pd
import asyncio
import aiohttp
import os
import math
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Configurações ---
input_path = r"ETL\data\anime.csv"
output_path = r"ETL\data\anime_titulos.csv"

# --- MUDANÇA CRÍTICA ---
# Definimos como 1 para garantir que as requisições sejam feitas em fila indiana.
# A Jikan tolera mal requisições paralelas na API pública.
MAX_CONCURRENT_REQUESTS = 1
BATCH_SIZE = 20  # Reduzido para salvar mais frequentemente no disco


async def get_anime_titles_async(session, mal_id, semaphore):
    """
    Busca dados na Jikan API com lógica robusta de retry (tentativa) e backoff.
    """
    url = f"https://api.jikan.moe/v4/anime/{mal_id}"

    # Número máximo de tentativas por ID antes de desistir
    max_retries = 5

    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(url) as response:

                    if response.status == 200:
                        data_json = await response.json()
                        data = data_json.get("data", {})

                        titles_list = []

                        # 1. Inglês
                        if data.get("title_english"):
                            titles_list.append(data.get("title_english"))

                        # 2. Padrão
                        if data.get("title"):
                            titles_list.append(data.get("title"))

                        # 3. Alternativos
                        for item in data.get("titles", []):
                            if item.get("title"):
                                titles_list.append(item.get("title"))

                        unique_titles = list(set(titles_list))

                        # --- DELAY OBRIGATÓRIO DE SUCESSO ---
                        # Mesmo com sucesso, esperamos 1.5s antes de liberar o semáforo
                        # para a próxima requisição não bater imediatamente.
                        await asyncio.sleep(1.5)

                        return {
                            "id": mal_id,
                            "titulos_alternativos": str(unique_titles),
                            "status": "ok",
                        }

                    elif response.status == 429:
                        # Cálculo do tempo de espera: 2 elevado à tentativa (2, 4, 8, 16s...)
                        wait_time = (2 ** (attempt + 1)) + 1
                        print(
                            f"ID {mal_id}: Erro 429. Tentativa {attempt+1}/{max_retries}. Esperando {wait_time}s..."
                        )
                        await asyncio.sleep(wait_time)
                        # O loop continua e tenta de novo

                    elif response.status == 404:
                        print(f"ID {mal_id}: Não encontrado (404).")
                        await asyncio.sleep(1)  # Pequeno delay mesmo no erro
                        return {
                            "id": mal_id,
                            "titulos_alternativos": "[]",
                            "status": "not_found",
                        }

                    else:
                        print(f"ID {mal_id}: Erro desconhecido {response.status}")
                        return {
                            "id": mal_id,
                            "titulos_alternativos": "[]",
                            "status": "error",
                        }

            except Exception as e:
                print(f"Erro de conexão ID {mal_id}: {e}")
                await asyncio.sleep(2)

        # Se saiu do loop, falhou todas as tentativas
        print(f"ID {mal_id}: Falhou após {max_retries} tentativas.")
        return {
            "id": mal_id,
            "titulos_alternativos": "[]",
            "status": "max_retries_exceeded",
        }


async def process_batch(ids_batch, df_original):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # Timeout total maior para evitar desconexões em esperas longas
    timeout = aiohttp.ClientTimeout(total=600)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            get_anime_titles_async(session, id_val, semaphore) for id_val in ids_batch
        ]
        results = await asyncio.gather(*tasks)

    valid_results = [r for r in results if r is not None]
    df_results = pd.DataFrame(valid_results)

    df_batch_original = df_original[df_original["id"].isin(ids_batch)].copy()

    if not df_results.empty:
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
            print("Iniciando do zero.")

    df_to_process = df_input[~df_input["id"].isin(ids_processados)].copy()
    all_ids = df_to_process["id"].tolist()
    total_ids = len(all_ids)

    print(f"Total a processar: {total_ids}")
    print(
        "AVISO: Modo de segurança ativado (1 req/1.5s) para evitar bloqueio da Jikan."
    )

    if total_ids == 0:
        return

    num_batches = math.ceil(total_ids / BATCH_SIZE)

    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch_ids = all_ids[start_idx:end_idx]

        print(f"Processando lote {i+1}/{num_batches} ({len(batch_ids)} animes)...")

        df_batch_result = asyncio.run(process_batch(batch_ids, df_to_process))

        write_header = not os.path.isfile(output_path)
        df_batch_result.to_csv(output_path, mode="a", header=write_header, index=False)


if __name__ == "__main__":
    import time

    start = time.time()
    main()
    end = time.time()
    # Mostra a média de tempo por item
    print(f"Tempo total: {end - start:.2f} segundos")
