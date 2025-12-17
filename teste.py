def gerar_lista_diversificada():
    print("\n🌍 Iniciando Crawler de Diversidade Musical...")

    # Dicionário de "Sementes" (Playlists Oficiais do Spotify)
    # Você pode adicionar mais IDs pegando da URL da playlist
    seeds = {
        "Top Global 50": "37i9dQZEVXbMDoHDwVN2tF",
        "Top Brasil 50": "37i9dQZEVXbMXbN3EUUhlg",
        "Rock Classics": "37i9dQZF1DWXRqgorJj26U",
        "RapCaviar (Hip-Hop)": "37i9dQZF1DX0XUsuxWHRQd",
        "Viva Latino": "37i9dQZF1DX10zKzsJ2j87",
        "Metal Essentials": "37i9dQZF1DWWOaP4H0wKns",
        "Indie Pop": "37i9dQZF1DWWECt2rOqu33",
        "Mega Hit Mix (Pop)": "37i9dQZF1DXbYM3nMM0oPk",
        "MPB Clássica": "37i9dQZF1DX889U0CL85jj",
        "Sertanejo 100%": "37i9dQZF1DX4PT7Bq2Nn8w",
        "Electronic Circus": "37i9dQZF1DX0r3x8OtiwEM",
    }

    artistas_unicos = set()

    for categoria, playlist_id in seeds.items():
        print(f"   ↳ Colhendo artistas de: {categoria}...")
        try:
            # Pega as primeiras 100 faixas da playlist
            results = sp.playlist_tracks(playlist_id, limit=100)

            for item in results["items"]:
                if item["track"] and item["track"]["artists"]:
                    # Pega o artista principal da faixa
                    nome_artista = item["track"]["artists"][0]["name"]
                    artistas_unicos.add(nome_artista)

        except Exception as e:
            print(f"     ⚠️ Erro na playlist {categoria}: {e}")

    lista_final = sorted(list(artistas_unicos))
    print(
        f"✅ Censo concluído! {len(lista_final)} artistas únicos encontrados para importação."
    )
    return lista_final


# --- NO FINAL DO SCRIPT (MAIN) ---
if __name__ == "__main__":

    # Gera a lista automaticamente baseada nas playlists semente
    artistas_para_importar = gerar_lista_diversificada()

    # Remove arquivo CSV antigo para começar limpo
    if os.path.exists(CSV_FILENAME):
        os.remove(CSV_FILENAME)

    # Processa a lista gerada
    for artista in artistas_para_importar:
        processar_artista_para_csv(artista)
