from dotenv import load_dotenv

load_dotenv(override=True)

import time
from supabase import create_client, Client
from os import getenv
from get_covers import get_movie_cover, get_game_cover, get_book_cover

SUPABASE_URL = getenv("SUPABASE_URL")
SUPABASE_KEY = getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def update_item_image(item_id, url):
    """Atualiza o registro no Supabase"""
    data = supabase.table("midia").update({"imagem": url}).eq("id", item_id).execute()
    return data


def main(batch_size=500):
    # offset = 9500
    offset = 0
    total_processados = 0
    total_sem_capa = 0

    print("🚀 Iniciando processamento em lotes...")

    while True:
        sem_capa = 0
        start = offset
        end = offset + batch_size - 1

        print(f"\n📥 Buscando lote: {start} até {end}...")

        response = (
            supabase.table("midia")
            .select("*")
            .in_("categoria", ["livro"])
            .order("rating", desc=True)
            .range(start, end)
            .execute()
        )

        items = response.data

        if not items:
            print("🏁 Nenhum item retornado. Fim da tabela!")
            break
        print(f"   Processando {len(items)} itens neste lote...")

        for item in items:
            titulo = item.get("titulo")
            categoria = item.get("categoria")
            item_id = item.get("id")
            if categoria not in ["filme", "jogo", "livro"] or item.get("imagem"):
                continue

            print(f"Processando: {titulo} ({categoria})...")
            print(f"Rating: {item.get("rating")}, ID: {item_id}")
            nova_capa = None

            if categoria == "filme":
                nova_capa = get_movie_cover(titulo)
            elif categoria == "jogo":
                nova_capa = get_game_cover(titulo)
            elif categoria == "livro":
                nova_capa = get_book_cover(titulo)

            if nova_capa:
                print(f"Capa encontrada: {nova_capa}")
                update_item_image(item_id, nova_capa)
            else:
                print(f"Capa não encontrada.")
                sem_capa += 1
                total_sem_capa += 1

            total_processados += 1
            time.sleep(1.0)

        print(f"Lote finalizado. Itens sem capa neste lote: {sem_capa}")
        offset += batch_size
    print(f"\nProcesso finalizado! Total de itens processados: {total_processados}")


if __name__ == "__main__":
    main()
