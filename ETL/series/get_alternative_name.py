import pandas as pd
import asyncio
import aiohttp
import os
import math
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Configurações ---
API_KEY = os.getenv("TMDB_API_KEY")
# Ajustei os caminhos para refletir que agora são séries
input_path = r"ETL\data\serie.csv"
output_path = r"ETL\data\serie_titulos.csv"

MAX_CONCURRENT_REQUESTS = 30
BATCH_SIZE = 100


async def get_alternative_titles_async(session, tmdb_id, semaphore):
    """
    Função assíncrona para buscar dados de um único ID de SÉRIE.
    """
    # MUDANÇA 1: Endpoint mudou de /movie/ para /tv/
    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
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

                    # MUDANÇA 2: 'original_title' vira 'original_name' para séries
                    original_name = data.get("original_name")
                    original_lang = data.get("original_language")
                    titles_list = [original_name]

                    english_title = None

                    if original_lang == "en":
                        english_title = original_name
                    else:
                        # MUDANÇA 3: A chave de lista para séries é 'results', não 'titles'
                        alt_titles = data.get("alternative_titles", {}).get(
                            "results", []
                        )
                        for item in alt_titles:
                            # A API retorna 'title' dentro do objeto, mesmo sendo série
                            if (
                                item.get("iso_3166_1") == "US"
                                or item.get("iso_639_1") == "en"
                            ):
                                english_title = item["title"]
                                break

                        if not english_title:
                            # MUDANÇA 4: Fallback é 'name', não 'title'
                            english_title = data.get("name")

                    if english_title and english_title != original_name:
                        titles_list.append(english_title)

                    return {
                        "id": tmdb_id,
                        "titulos_alternativos": str(list(set(titles_list))),
                        "status": "ok",
                    }

                elif response.status == 429:
                    print(f"ID {tmdb_id}: Rate limit. Retrying...")
                    await asyncio.sleep(2)
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
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = [
            get_alternative_titles_async(session, id_val, semaphore)
            for id_val in ids_batch
        ]
        results = await asyncio.gather(*tasks)

    valid_results = [r for r in results if r is not None]
    df_results = pd.DataFrame(valid_results)

    df_batch_original = df_original[df_original["id"].isin(ids_batch)].copy()

    if not df_results.empty:
        # Nota: Mantive as colunas 'titulo' e 'ano_lancamento' assumindo
        # que seu CSV de séries segue o mesmo padrão de colunas do de filmes.
        # Se for 'nome' ou 'ano_inicio', ajuste aqui.
        cols_to_merge = ["id", "titulo", "ano_lancamento"]

        # Verificação de segurança caso as colunas não existam no CSV de séries
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

        print(f"Processando lote {i+1}/{num_batches} ({len(batch_ids)} séries)...")

        df_batch_result = asyncio.run(process_batch(batch_ids, df_to_process))

        write_header = not os.path.isfile(output_path)
        df_batch_result.to_csv(output_path, mode="a", header=write_header, index=False)


if __name__ == "__main__":
    import time

    start = time.time()
    main()
    end = time.time()
    print(f"Tempo total: {end - start:.2f} segundos")
