import requests
import json
import csv
import time
from datetime import datetime, timezone

# --- CONFIGURAÇÃO ---
CLIENT_ID = ""
CLIENT_SECRET = ""

BATCH_SIZE = 500
MIN_VOTES = 20
FILENAME_CSV = "dataset_completo_igdb.csv"


def get_access_token(client_id, client_secret):
    url = "https://id.twitch.tv/oauth2/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def process_image_url(url):
    if not url:
        return None
    url = f"https:{url}" if url.startswith("//") else url
    return url.replace("t_thumb", "t_cover_big")  # Alta qualidade


def save_batch_to_csv(data_batch, is_first_batch):
    """Salva um lote de dados no CSV. Se for o primeiro, escreve o cabeçalho."""
    fieldnames = [
        "id",
        "titulo",
        "ano_lancamento",
        "generos",
        "rating",
        "num_avaliacoes",
        "imagem",
        "descricao",
        "metadata",
    ]
    mode = "w" if is_first_batch else "a"

    with open(FILENAME_CSV, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if is_first_batch:
            writer.writeheader()
        writer.writerows(data_batch)


def build_dataset_loop():
    print("Autenticando...")
    try:
        token = get_access_token(CLIENT_ID, CLIENT_SECRET)
    except Exception as e:
        print(f"Erro de autenticação: {e}")
        return

    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }

    offset = 0
    total_coletado = 0
    keep_running = True

    print(
        f"Iniciando coleta. Lote de {BATCH_SIZE} por vez. Salvando em '{FILENAME_CSV}'..."
    )

    while keep_running:
        # Query Dinâmica com Offset
        body = f"""
            fields name, first_release_date, summary, genres.name, themes.name, total_rating, total_rating_count, cover.url, slug, url, platforms.name;
            where game_type = 0;
            sort total_rating_count desc;
            limit {BATCH_SIZE};
            offset {offset};
        """

        try:
            response = requests.post(url, headers=headers, data=body)

            if response.status_code == 429:
                print("Limite de requisições atingido. Esperando 5 segundos...")
                time.sleep(5)
                continue

            if response.status_code != 200:
                print(f"Erro na API: {response.status_code} - {response.text}")
                break

            games = response.json()

            if not games:
                print("Fim dos dados encontrados.")
                break

            processed_batch = []

            for game in games:
                ts = game.get("first_release_date")
                ano = datetime.fromtimestamp(ts, timezone.utc).year if ts else None
                genres_list = [g["name"] for g in game.get("genres", [])]
                themes_list = [t["name"] for t in game.get("themes", [])]
                genres_list = list(set(genres_list + themes_list))

                metadata = {
                    "slug": game.get("slug"),
                    "igdb_url": game.get("url"),
                    "plataformas": [p["name"] for p in game.get("platforms", [])],
                    "raw_genres": genres_list,
                }

                entry = {
                    "id": game.get("id"),
                    "titulo": game.get("name"),
                    "ano_lancamento": ano,
                    "descricao": game.get("summary", "")
                    .replace("\n", " ")
                    .replace("\r", ""),
                    "generos": ", ".join(genres_list),
                    "rating": (
                        round(game.get("total_rating", 0), 2)
                        if game.get("total_rating")
                        else None
                    ),
                    "num_avaliacoes": game.get("total_rating_count"),
                    "metadata": json.dumps(metadata, ensure_ascii=False),
                    "imagem": process_image_url(game.get("cover", {}).get("url")),
                }
                processed_batch.append(entry)

            # Salva o lote atual
            save_batch_to_csv(processed_batch, is_first_batch=(offset == 0))

            count = len(processed_batch)
            total_coletado += count
            print(
                f"Lote processado: Jogos {offset} a {offset + count}. Total até agora: {total_coletado}"
            )

            # Prepara o próximo lote
            offset += BATCH_SIZE

            # Pausa ética para não sobrecarregar a API (Rate Limit)
            time.sleep(0.3)

        except KeyboardInterrupt:
            print("\nParando script manualmente...")
            break
        except Exception as e:
            print(f"Erro inesperado: {e}")
            break

    print(f"\n--- Processo finalizado ---")
    print(f"Total de jogos salvos: {total_coletado}")


if __name__ == "__main__":
    build_dataset_loop()
