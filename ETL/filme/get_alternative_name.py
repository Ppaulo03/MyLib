import pandas as pd
import asyncio
import aiohttp
import os
import math
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Configurações ---
API_KEY = os.getenv("TMDB_API_KEY")
input_path = r"ETL\data\filme.csv"
output_path = r"ETL\data\filme_titulos.csv"


MAX_CONCURRENT_REQUESTS = 30
BATCH_SIZE = 100  # Salva no disco a cada 100 filmes processados


async def get_alternative_titles_async(session, tmdb_id, semaphore):
    """
    Função assíncrona para buscar dados de um único ID.
    """
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {
        "append_to_response": "alternative_titles",
    }
    headers = {
        "Authorization": f"Bearer {API_KEY}",
    }

    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()

                    original_title = data.get("original_title")
                    original_lang = data.get("original_language")
                    titles_list = [original_title]

                    english_title = None

                    if original_lang == "en":
                        english_title = original_title
                    else:
                        alt_titles = data.get("alternative_titles", {}).get(
                            "titles", []
                        )
                        for item in alt_titles:
                            if (
                                item.get("iso_3166_1") == "US"
                                or item.get("iso_639_1") == "en"
                            ):
                                english_title = item["title"]
                                break

                        if not english_title:
                            english_title = data.get("title")

                    if english_title and english_title != original_title:
                        titles_list.append(english_title)

                    # Retorna o dicionário pronto para o DataFrame
                    return {
                        "id": tmdb_id,
                        "titulos_alternativos": str(list(set(titles_list))),
                        "status": "ok",
                    }

                elif response.status == 429:
                    print(f"ID {tmdb_id}: Rate limit. Retrying...")
                    await asyncio.sleep(2)  # Espera um pouco mais no async
                    return await get_alternative_titles_async(
                        session, tmdb_id, semaphore
                    )

                else:
                    return {
                        "id": tmdb_id,
                        "titulos_alternativos": "[]",
                        "status": "error",
                    }

        except Exception as e:
            print(f"Erro ID {tmdb_id}: {e}")
            return {"id": tmdb_id, "titulos_alternativos": "[]", "status": "error"}


async def process_batch(ids_batch, df_original):
    """
    Processa um lote de IDs simultaneamente.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = [
            get_alternative_titles_async(session, id_val, semaphore)
            for id_val in ids_batch
        ]
        results = await asyncio.gather(*tasks)

    # Filtra resultados válidos
    valid_results = [r for r in results if r is not None]

    # Transforma em DataFrame
    df_results = pd.DataFrame(valid_results)

    # Merge com os dados originais (titulo, ano) usando o ID
    # Precisamos garantir que os tipos batem
    df_batch_original = df_original[df_original["id"].isin(ids_batch)].copy()

    if not df_results.empty:
        df_final = pd.merge(
            df_batch_original[["id", "titulo", "ano_lancamento"]],
            df_results[["id", "titulos_alternativos"]],
            on="id",
            how="left",
        )
        return df_final
    return pd.DataFrame()


def main():
    print("Lendo CSV de entrada...")
    df_input = pd.read_csv(input_path)
    df_input = df_input.dropna(subset=["id"])
    df_input["id"] = df_input["id"].astype(int)

    # 1. Lógica de Retomada (Resume)
    ids_processados = set()
    if os.path.isfile(output_path):
        try:
            df_existing = pd.read_csv(output_path, usecols=["id"])
            ids_processados = set(df_existing["id"].tolist())
            print(f"Resume: {len(ids_processados)} já processados.")
        except:
            print("Arquivo de saída vazio ou erro de leitura. Iniciando do zero.")

    # 2. Filtrar o que falta
    df_to_process = df_input[~df_input["id"].isin(ids_processados)].copy()
    all_ids = df_to_process["id"].tolist()
    total_ids = len(all_ids)

    print(f"Total a processar: {total_ids}")

    if total_ids == 0:
        return

    # 3. Loop de Lotes (Batches)
    # Dividimos a lista de IDs em pedaços de tamanho BATCH_SIZE
    num_batches = math.ceil(total_ids / BATCH_SIZE)

    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch_ids = all_ids[start_idx:end_idx]

        print(f"Processando lote {i+1}/{num_batches} ({len(batch_ids)} filmes)...")

        # Roda o processamento assíncrono para este lote
        # (O loop do Python "espera" o lote acabar para salvar, mas dentro do lote é paralelo)
        df_batch_result = asyncio.run(process_batch(batch_ids, df_to_process))

        # Salva no disco
        write_header = not os.path.isfile(output_path)
        df_batch_result.to_csv(output_path, mode="a", header=write_header, index=False)


if __name__ == "__main__":
    import time

    start = time.time()
    main()
    end = time.time()
    print(f"Tempo total: {end - start:.2f} segundos")
