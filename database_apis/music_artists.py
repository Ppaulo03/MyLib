import spotipy
from spotipy.oauth2 import SpotifyOAuth
import csv
import json
import os
from dotenv import load_dotenv

load_dotenv(override=True)

SPOTIPY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
CSV_FILENAME = "data_raw/musical.csv"

SCOPE = "playlist-read-private user-follow-read"
REDIRECT_URI = "http://127.0.0.1:80"

# Inicialização do Cliente com OAuth (Simula um usuário real)
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=SPOTIPY_CLIENT_ID,
        client_secret=SPOTIPY_CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
    )
)


def calcular_rating_1_to_5(popularidade_0_100):
    if popularidade_0_100 is None:
        return 0
    return round(1 + (popularidade_0_100 * 0.04), 1)


def calcular_classificacao_inteligente(is_explicit, lista_generos):
    if is_explicit:
        return 16

    contexto_genero = " ".join(lista_generos).lower() if lista_generos else ""

    keywords_livre = [
        "children",
        "infantil",
        "kids",
        "lullaby",
        "nursery",
        "disney",
        "cartoon",
        "anime score",
        "soundtrack",
        "classical",
        "instrumental",
        "piano",
        "sleep",
        "ambient",
    ]

    if any(k in contexto_genero for k in keywords_livre):
        return 0

    keywords_leve = [
        "christian",
        "gospel",
        "louvor",
        "ccm",  # Religioso
        "jazz",
        "blues",
        "bossa nova",
        "mpb",  # Culturais/Leves
        "orchestral",
        "symphonic",
    ]

    if any(k in contexto_genero for k in keywords_leve):
        return 10

    keywords_pesado_clean = [
        "death metal",
        "black metal",
        "doom",
        "grindcore",
        "horror",
        "dark ambient",
        "thrash",
    ]

    if any(k in contexto_genero for k in keywords_pesado_clean):
        return 14
    return 12


def salvar_no_csv(dados_lista, filename):

    file_exists = os.path.isfile(filename)
    fieldnames = [
        "titulo",
        "ano_lancamento",
        "categoria",
        "generos",
        "generos_unificados",
        "rating",
        "num_avaliacoes",
        "imagem",
        "descricao",
        "metadata",
        "classificacao",
    ]

    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for item in dados_lista:
            row = item.copy()
            row["generos"] = json.dumps(item["generos"], ensure_ascii=False)
            row["generos_unificados"] = json.dumps(
                item["generos_unificados"], ensure_ascii=False
            )
            row["metadata"] = json.dumps(item["metadata"], ensure_ascii=False)
            writer.writerow(row)


def processar_artista_para_csv(nome_artista_busca):

    results = sp.search(q=nome_artista_busca, type="artist", limit=1)
    if not results["artists"]["items"]:
        print(f"'{nome_artista_busca}' não encontrado.")
        return

    artist_data = results["artists"]["items"][0]
    artist_id = artist_data["id"]
    artist_name = artist_data["name"]
    img_url = artist_data["images"][0]["url"] if artist_data["images"] else None

    albums_results = sp.artist_albums(
        artist_id, album_type="album", limit=50, country="BR"
    )
    albuns_lista = []
    titulos_processados = set()
    contagem_explicit = 0

    for album_simple in albums_results["items"]:
        nome_limpo = album_simple["name"].lower().split("(")[0].strip()
        if nome_limpo in titulos_processados:
            continue
        titulos_processados.add(nome_limpo)
        try:
            album_full = sp.album(album_simple["id"])
        except Exception:
            continue
        tracks = album_full.get("tracks", {}).get("items", [])
        any_explicit = any(t.get("explicit", False) for t in tracks)
        if any_explicit:
            contagem_explicit += 1

        any_explicit = any(t.get("explicit", False) for t in tracks)
        classificacao_final = calcular_classificacao_inteligente(
            any_explicit, artist_data["genres"]
        )

        album_obj = {
            "titulo": album_simple["name"],
            "ano_lancamento": (
                int(album_simple["release_date"][:4])
                if album_simple["release_date"]
                else 0
            ),
            "categoria": "album_musical",
            "generos": artist_data["genres"],
            "generos_unificados": [],
            "rating": calcular_rating_1_to_5(album_full["popularity"]),
            "num_avaliacoes": 0,
            "classificacao": classificacao_final,
            "imagem": (
                album_simple["images"][0]["url"] if album_simple["images"] else None
            ),
            "descricao": f"Álbum de {artist_name}",
            "metadata": {
                "id_artista_spotify": artist_id,
                "nome_artista": artist_name,
                "spotify_id": album_simple["id"],
                "total_faixas": album_simple["total_tracks"],
                "gravadora": album_full.get("label"),
                "tipo": album_simple["album_type"],
            },
        }

        albuns_lista.append(album_obj)

    classificacao_base = calcular_classificacao_inteligente(
        False, artist_data["genres"]
    )
    qtde_albuns_explicit = sum(1 for a in albuns_lista if a["classificacao"] >= 16)
    if albuns_lista:
        ratio_explicit = qtde_albuns_explicit / len(albuns_lista)
        if ratio_explicit > 0.20:
            classificacao_artista = 16
        else:
            classificacao_artista = classificacao_base
    else:
        classificacao_artista = classificacao_base

    artista_obj = {
        "titulo": artist_name,
        "ano_lancamento": 0,
        "categoria": "artista_musical",
        "generos": artist_data["genres"],
        "generos_unificados": [],
        "rating": calcular_rating_1_to_5(artist_data["popularity"]),
        "num_avaliacoes": artist_data["followers"]["total"],
        "classificacao": classificacao_artista,
        "imagem": img_url,
        "descricao": "Artista importado via Spotify API.",
        "metadata": {
            "spotify_id": artist_id,
            "popularidade_raw": artist_data["popularity"],
            "tipo": "artista",
        },
    }

    batch_completo = [artista_obj] + albuns_lista
    salvar_no_csv(batch_completo, CSV_FILENAME)
    print(f"{artist_name} + {len(albuns_lista)} álbuns salvos em '{CSV_FILENAME}'")


def gerar_lista_diversificada():
    print("\n🌍 Iniciando Crawler via Discovery (Scanner de Categorias)...")

    artistas_unicos = set()
    playlists_processadas = 0

    try:
        # PASSO 1: Descobrir quais categorias existem no Brasil
        # Isso retorna a lista real (ex: 'toplists', '00s', 'sertanejo', 'pop')
        # Sem risco de 404 por ID errado.
        print("   ↳ Buscando categorias disponíveis...")
        response_cats = sp.categories(country="BR", locale="pt_BR", limit=50)
        categorias_items = response_cats["categories"]["items"]

        # Palavras-chave para IGNORAR (Não queremos Podcasts ou Audiobooks)
        blacklist_termos = [
            "podcast",
            "audiobooks",
            "made for you",
            "charts",
            "paradas",
        ]
        # Nota: Se quiser as 'Top 50', remova 'charts'/'paradas' da blacklist

        for cat in categorias_items:
            cat_id = cat["id"]
            cat_name = cat["name"]

            # Filtro básico de qualidade
            if any(termo in cat_name.lower() for termo in blacklist_termos):
                continue

            print(f"\n📂 Processando Categoria: {cat_name} (ID: {cat_id})")

            try:
                # PASSO 2: Pegar playlists desta categoria
                playlists_resp = sp.category_playlists(
                    category_id=cat_id, country="BR", limit=3
                )

                for pl in playlists_resp["playlists"]["items"]:
                    if not pl:
                        continue

                    # print(f"     ↳ Extraindo: {pl['name']}")

                    try:
                        # PASSO 3: Pegar artistas da playlist
                        tracks_resp = sp.playlist_tracks(
                            pl["id"], limit=20, market="BR"
                        )

                        for item in tracks_resp["items"]:
                            if item["track"] and item["track"]["artists"]:
                                nome = item["track"]["artists"][0]["name"]
                                artistas_unicos.add(nome)

                        playlists_processadas += 1
                        print(
                            f"       ✅ +{len(tracks_resp['items'])} faixas de '{pl['name']}'"
                        )

                    except Exception as e:
                        # Erros pontuais em playlists não param o script
                        pass

            except Exception as e:
                print(f"     ⚠️ Categoria vazia ou erro: {e}")

    except Exception as e:
        print(f"❌ Erro fatal ao buscar categorias: {e}")

    lista_final = sorted(list(artistas_unicos))
    print(f"\n✅ Censo concluído!")
    print(f"📊 {playlists_processadas} playlists varridas.")
    print(f"🎵 {len(lista_final)} artistas únicos encontrados.")

    return lista_final


def obter_artistas_ja_processados(filename):
    processed = set()

    if not os.path.exists(filename):
        return processed

    try:
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("categoria") == "artista_musical":
                    processed.add(row["titulo"].lower().strip())
    except Exception as e:
        print(f"⚠️ Aviso: Não foi possível ler o histórico ({e}). Iniciando zerado.")
    return processed


SEMENTES_MASTER = [
    # === ROCK & METAL CLÁSSICO/MODERNO ===
    "The Beatles",
    "The Rolling Stones",
    "Led Zeppelin",
    "Pink Floyd",
    "Queen",
    "AC/DC",
    "Black Sabbath",
    "Deep Purple",
    "The Who",
    "Jimi Hendrix",
    "The Doors",
    "Cream",
    "Aerosmith",
    "Kiss",
    "Van Halen",
    "Guns N' Roses",
    "Scorpions",
    "Bon Jovi",
    "U2",
    "The Police",
    "Dire Straits",
    "Bruce Springsteen",
    "David Bowie",
    "Eric Clapton",
    "Fleetwood Mac",
    "Eagles",
    "Lynyrd Skynyrd",
    "Creedence Clearwater Revival",
    "Santana",
    "Journey",
    "Boston",
    "Toto",
    "Foreigner",
    "Heart",
    "Joan Jett",
    "Alice Cooper",
    "Metallica",
    "Iron Maiden",
    "Judas Priest",
    "Megadeth",
    "Slayer",
    "Pantera",
    "Motorhead",
    "Anthrax",
    "Sepultura",
    "System of a Down",
    "Slipknot",
    "Korn",
    "Limp Bizkit",
    "Linkin Park",
    "Rage Against the Machine",
    "Deftones",
    "Disturbed",
    "Avenged Sevenfold",
    "Evanescence",
    "Nightwish",
    "Within Temptation",
    "Epica",
    "Lacuna Coil",
    "Ghost",
    "Volbeat",
    "Five Finger Death Punch",
    "Bullet for My Valentine",
    "Bring Me The Horizon",
    "Rammstein",
    "Gojira",
    "Mastodon",
    "Opeth",
    "Dream Theater",
    "Tool",
    "A Perfect Circle",
    # === GRUNGE, PUNK & INDIE/ALT ===
    "Nirvana",
    "Pearl Jam",
    "Soundgarden",
    "Alice in Chains",
    "Stone Temple Pilots",
    "Foo Fighters",
    "Red Hot Chili Peppers",
    "The Smashing Pumpkins",
    "Green Day",
    "The Offspring",
    "Blink-182",
    "Sum 41",
    "Ramones",
    "The Clash",
    "Sex Pistols",
    "Iggy Pop",
    "The Stooges",
    "The Cure",
    "The Smiths",
    "Joy Division",
    "New Order",
    "Depeche Mode",
    "Radiohead",
    "Oasis",
    "Blur",
    "The Verve",
    "Coldplay",
    "Muse",
    "The Killers",
    "Franz Ferdinand",
    "Arctic Monkeys",
    "The Strokes",
    "The White Stripes",
    "The Black Keys",
    "Kings of Leon",
    "Imagine Dragons",
    "OneRepublic",
    "Twenty One Pilots",
    "Florence + The Machine",
    "Lana Del Rey",
    "Lorde",
    "Hozier",
    "Tame Impala",
    "Gorillaz",
    "Vampire Weekend",
    "Arcade Fire",
    "Bon Iver",
    "The 1975",
    "Paramore",
    "Fall Out Boy",
    "My Chemical Romance",
    "Panic! At The Disco",
    "Weezer",
    "Beck",
    "Pixies",
    "Sonic Youth",
    # === POP & R&B (DIVAS & ICONS) ===
    "Michael Jackson",
    "Madonna",
    "Prince",
    "Whitney Houston",
    "Mariah Carey",
    "Celine Dion",
    "George Michael",
    "Elton John",
    "Phil Collins",
    "Stevie Wonder",
    "Tina Turner",
    "Cher",
    "Britney Spears",
    "Christina Aguilera",
    "Justin Timberlake",
    "Beyoncé",
    "Rihanna",
    "Lady Gaga",
    "Katy Perry",
    "Taylor Swift",
    "Adele",
    "Bruno Mars",
    "Ed Sheeran",
    "Justin Bieber",
    "Ariana Grande",
    "Dua Lipa",
    "The Weeknd",
    "Harry Styles",
    "Miley Cyrus",
    "Selena Gomez",
    "Demi Lovato",
    "Shawn Mendes",
    "Sam Smith",
    "Sia",
    "P!nk",
    "Shakira",
    "Jennifer Lopez",
    "Alicia Keys",
    "John Legend",
    "Usher",
    "Ne-Yo",
    "Chris Brown",
    "Frank Ocean",
    "SZA",
    "Doja Cat",
    "Billie Eilish",
    "Olivia Rodrigo",
    "Post Malone",
    "Halsey",
    "Khalid",
    "Lizzo",
    "Janet Jackson",
    "TLC",
    "Destiny's Child",
    "Spice Girls",
    "Backstreet Boys",
    "NSYNC",
    "ABBA",
    "Bee Gees",
    "Carpenters",
    # === RAP & HIP-HOP ===
    "Tupac",
    "The Notorious B.I.G.",
    "Dr. Dre",
    "Snoop Dogg",
    "Eminem",
    "Jay-Z",
    "Nas",
    "Wu-Tang Clan",
    "N.W.A",
    "Ice Cube",
    "50 Cent",
    "Kanye West",
    "Drake",
    "Kendrick Lamar",
    "J. Cole",
    "Travis Scott",
    "Future",
    "Lil Wayne",
    "Nicki Minaj",
    "Cardi B",
    "Megan Thee Stallion",
    "Tyler, The Creator",
    "A$AP Rocky",
    "Kid Cudi",
    "Mac Miller",
    "XXXTentacion",
    "Juice WRLD",
    "Lil Peep",
    "Post Malone",
    "The Black Eyed Peas",
    "OutKast",
    "Missy Elliott",
    "Lauryn Hill",
    "Fugees",
    "Public Enemy",
    "Run-D.M.C.",
    "Beastie Boys",
    "Cypress Hill",
    "Migos",
    "21 Savage",
    # === BRASIL: MPB, SAMBA & CLÁSSICOS ===
    "Roberto Carlos",
    "Erasmo Carlos",
    "Tim Maia",
    "Jorge Ben Jor",
    "Wilson Simonal",
    "Caetano Veloso",
    "Gilberto Gil",
    "Chico Buarque",
    "Milton Nascimento",
    "Gal Costa",
    "Maria Bethânia",
    "Elis Regina",
    "Rita Lee",
    "Ney Matogrosso",
    "Djavan",
    "Alceu Valença",
    "Zé Ramalho",
    "Elba Ramalho",
    "Luiz Gonzaga",
    "Dominguinhos",
    "Cartola",
    "Adoniran Barbosa",
    "Paulinho da Viola",
    "Martinho da Vila",
    "Alcione",
    "Beth Carvalho",
    "Zeca Pagodinho",
    "Jorge Aragão",
    "Fundo de Quintal",
    "Raça Negra",
    "Só Pra Contrariar",
    "Exaltasamba",
    "Sorriso Maroto",
    "Thiaguinho",
    "Péricles",
    "Ferrugem",
    "Ludmilla",
    "Seu Jorge",
    "Tribalistas",
    "Marisa Monte",
    "Cássia Eller",
    "Nando Reis",
    "Ana Carolina",
    "Vanessa da Mata",
    "Tiago Iorc",
    "Anavitória",
    "Silva",
    "Duda Beat",
    "Liniker",
    "Gilsons",
    # === BRASIL: ROCK, POP & RAP NACIONAL ===
    "Legião Urbana",
    "Paralamas do Sucesso",
    "Titãs",
    "Barão Vermelho",
    "Cazuza",
    "Engenheiros do Hawaii",
    "Capital Inicial",
    "Biquini Cavadão",
    "Ultraje a Rigor",
    "Raimundos",
    "Charlie Brown Jr.",
    "CPM 22",
    "Detonautas",
    "Pitty",
    "NX Zero",
    "Fresno",
    "Skank",
    "Jota Quest",
    "O Rappa",
    "Planet Hemp",
    "Mamonas Assassinas",
    "Los Hermanos",
    "Sepultura",
    "Angra",
    "Shaman",
    "Krisiun",
    "Ratos de Porão",
    "Matanza",
    "Racionais MC's",
    "Sabotage",
    "Facção Central",
    "Marcelo D2",
    "Gabriel O Pensador",
    "Emicida",
    "Criolo",
    "Mano Brown",
    "Djonga",
    "Baco Exu do Blues",
    "Filipe Ret",
    "Xamã",
    "Matuê",
    "L7NNON",
    "Orochi",
    "Poesia Acústica",
    # === BRASIL: SERTANEJO & FUNK ===
    "Chitãozinho & Xororó",
    "Zezé Di Camargo & Luciano",
    "Leandro & Leonardo",
    "Bruno & Marrone",
    "João Paulo & Daniel",
    "Milionário & José Rico",
    "Sérgio Reis",
    "Almir Sater",
    "Jorge & Mateus",
    "Henrique & Juliano",
    "Marília Mendonça",
    "Gusttavo Lima",
    "Luan Santana",
    "Michel Teló",
    "Zé Neto & Cristiano",
    "Maiara & Maraisa",
    "Simone & Simaria",
    "Matheus & Kauan",
    "Wesley Safadão",
    "Xand Avião",
    "João Gomes",
    "Anitta",
    "Pabllo Vittar",
    "Gloria Groove",
    "Luísa Sonza",
    "Lexa",
    "IZA",
    "Pedro Sampaio",
    "Dennis DJ",
    "Kevinho",
    "MC Kevin o Chris",
    "MC Hariel",
    "MC Ryan SP",
    "MC Cabelinho",
    "MC Poze do Rodo",
    "Claudinho & Buchecha",
    # === JAZZ, BLUES & INSTRUMENTAL ===
    "Louis Armstrong",
    "Duke Ellington",
    "Miles Davis",
    "John Coltrane",
    "Charlie Parker",
    "Nina Simone",
    "Ella Fitzgerald",
    "Billie Holiday",
    "Aretha Franklin",
    "Ray Charles",
    "Etta James",
    "B.B. King",
    "Muddy Waters",
    "Eric Clapton",
    "Stevie Ray Vaughan",
    "Gary Moore",
    "Jimi Hendrix",
    "Frank Sinatra",
    "Tony Bennett",
    "Nat King Cole",
    "Amy Winehouse",
    "Norah Jones",
    "Diana Krall",
    "Michael Bublé",
    "Kenny G",
    "Hans Zimmer",
    "John Williams",
    "Ennio Morricone",
    "Ludwig van Beethoven",
    "Wolfgang Amadeus Mozart",
    "Johann Sebastian Bach",
    "Frédéric Chopin",
    "Antonio Vivaldi",
    "Pyotr Ilyich Tchaikovsky",
    # === ELETRÔNICA, REGGAE & K-POP ===
    "Daft Punk",
    "Kraftwerk",
    "The Chemical Brothers",
    "The Prodigy",
    "Fatboy Slim",
    "Moby",
    "Avicii",
    "Calvin Harris",
    "David Guetta",
    "Tiësto",
    "Armin van Buuren",
    "Martin Garrix",
    "Zedd",
    "The Chainsmokers",
    "Marshmello",
    "Skrillex",
    "Diplo",
    "Major Lazer",
    "DJ Snake",
    "Alok",
    "Vintage Culture",
    "Bob Marley",
    "The Wailers",
    "Peter Tosh",
    "Jimmy Cliff",
    "UB40",
    "Natiruts",
    "Maneva",
    "Ponto de Equilíbrio",
    "BTS",
    "BLACKPINK",
    "TWICE",
    "EXO",
    "Stray Kids",
    "NewJeans",
    "Seventeen",
    "PSY",
    "BigBang",
]


# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":

    ja_processados = obter_artistas_ja_processados(CSV_FILENAME)
    total = len(SEMENTES_MASTER)

    for i, artista in enumerate(SEMENTES_MASTER):

        if artista.lower().strip() in ja_processados:
            print(f"[{i+1}/{total}] ⏩ Pulando {artista} (Já existe)")
            continue
        print(f"[{i+1}/{total}] 🎸 Processando: {artista}")
        try:
            processar_artista_para_csv(artista)
            ja_processados.add(artista.lower().strip())
        except Exception as e:
            print(f"   ⚠️ Erro inesperado em {artista}: {e}")

    print("\nFIM! Todos os artistas da lista foram processados.")
