from dotenv import load_dotenv
import requests
import pandas as pd
import os, time
from datetime import datetime, timezone

load_dotenv(override=True)

IGBD_CLIENT_ID = os.getenv("IGBD_CLIENT_ID")
IGBD_CLIENT_SECRET = os.getenv("IGBD_CLIENT_SECRET")
CSV_PATH = "games_data.csv"


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


df = pd.read_csv(CSV_PATH)
jogos_sem_ano = df[df["ano_lancamento"].isna()].copy()
if jogos_sem_ano.empty:
    print("Nenhum jogo com ano faltando!")
else:
    print(f"Encontrados {len(jogos_sem_ano)} jogos sem ano. Buscando no IGDB...")

    print("Autenticando...")
    try:
        token = get_access_token(IGBD_CLIENT_ID, IGBD_CLIENT_SECRET)
    except Exception as e:
        print(f"Erro de autenticação: {e}")

    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": IGBD_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    ids_para_buscar = jogos_sem_ano["id"].tolist()
    novos_anos = {}

    for chunk_ids in chunks(ids_para_buscar, 50):
        ids_str = ",".join(map(str, chunk_ids))
        body = f"fields name, first_release_date, release_dates.y; where id = ({ids_str}); limit 50;"

        response = requests.post(
            "https://api.igdb.com/v4/games", headers=headers, data=body
        )

        if response.status_code == 200:
            dados = response.json()
            for game in dados:
                ano_final = None
                if "release_dates" in game:
                    anos_lista = [r["y"] for r in game["release_dates"] if "y" in r]
                    if anos_lista:
                        ano_final = min(anos_lista)
                        metodo = "release_dates.y"

                if not ano_final and "first_release_date" in game:
                    ts = game["first_release_date"]
                    try:
                        dt = datetime.fromtimestamp(ts, timezone.utc)
                        ano_final = dt.year
                        metodo = "timestamp_convertido"
                    except (OSError, ValueError):
                        print(
                            f"[ERRO DATA] {game['name']} tem timestamp inválido: {ts}"
                        )
                if ano_final:
                    novos_anos[game["id"]] = ano_final
                    print(f"[OK] {game['name']}: {ano_final} ({metodo})")
                else:
                    print(f"[SEM DATA] {game.get('name')}")

        else:
            print(f"Erro na API: {response.status_code} - {response.text}")
        time.sleep(0.25)
        break

    print("\n--- Aplicando mudanças no DataFrame ---")
    for game_id, ano in novos_anos.items():
        df.loc[df["id"] == game_id, "ano_lancamento"] = ano

    print(df.loc[df["id"].isin(novos_anos.keys()), ["id", "titulo", "ano_lancamento"]])
    df.to_csv(CSV_PATH, index=False)
