import os
from supabase import create_client, Client
import unicodedata
from dotenv import load_dotenv

load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


MOVIE_MAPPING = {
    # --- AÇÃO E AVENTURA ---
    "Ação": ["Ação"],
    "Aventura": ["Aventura"],
    "Faroeste": ["Ação", "Aventura"],  # Western
    "Guerra": ["Ação", "Drama", "Realidade / Educação"],  # Frequentemente histórico
    # --- EMOÇÃO E DRAMA ---
    "Drama": ["Drama"],
    "Romance": ["Romance"],
    "Comédia": ["Comédia"],
    "Cinema TV": [
        "Drama",
        "Romance",
    ],  # Geralmente telefilmes são dramas ou comédias românticas
    "Família": ["Infantil / Família"],
    # --- FANTASIA E IMAGINAÇÃO ---
    "Fantasia": ["Fantasia"],
    "Ficção científica": ["Ficção Científica"],
    "Animação": [
        "Infantil / Família",
        "Fantasia",
    ],  # Animação é meio, mas quase sempre cai aqui
    # --- TENSÃO ---
    "Terror": ["Terror / Suspense"],
    "Thriller": ["Terror / Suspense"],  # Suspense puro
    "Mistério": [
        "Terror / Suspense",
        "Estratégia / Raciocínio",
    ],  # Detetive/Investigation
    "Crime": ["Terror / Suspense", "Drama"],
    # --- REALIDADE E CULTURA ---
    "Documentário": ["Realidade / Educação"],
    "História": ["Realidade / Educação", "Drama"],
    "Música": ["Música"],  # Musicais ou Biografias musicais
}


def unify_genres(specific_genres):
    if not specific_genres:
        return []

    unified_set = set()

    for g in specific_genres:
        key = g.strip()

        if key in MOVIE_MAPPING:
            for super_g in MOVIE_MAPPING[key]:
                unified_set.add(super_g)
        else:
            print(f"Não mapeado: {key}")
    return list(unified_set)


def run_anime_unification():
    print("Iniciando unificação de gêneros de ANIME...")

    page_size = 1000
    page = 0
    total_updated = 0

    while True:
        print(f"Processando página {page}...")

        # Filtra apenas ANIME
        response = (
            supabase.table("midia")
            .select("id, titulo, generos, generos_unificados")
            .eq("categoria", "filme")
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )

        rows = response.data
        if not rows:
            break

        for row in rows:
            current_specific = row["generos"]
            if not current_specific:
                continue

            # Gera os unificados
            new_unified = unify_genres(current_specific)

            # Atualiza se mudou ou se estava vazio
            if new_unified != row["generos_unificados"]:
                try:
                    supabase.table("midia").update(
                        {"generos_unificados": new_unified}
                    ).eq("id", row["id"]).execute()
                    total_updated += 1
                except Exception as e:
                    print({"generos_unificados": new_unified})
                    print(f"Erro ao atualizar {row['titulo']} - {row["id"]}: {e}")

        page += 1

    print(f"Unificação de Animes concluída! Total atualizado: {total_updated}")


if __name__ == "__main__":
    run_anime_unification()
