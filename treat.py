import pandas as pd
from dotenv import load_dotenv
import ast

load_dotenv(override=True)
from supabase_populate.genre_map import GENRE_MAP


def tratar_livros():
    dataset = pd.read_csv("data/livros.csv")
    dataset = dataset.fillna(
        {"ano": 0, "paginas": 0, "rating": 0, "autor": "", "editora": "", "genero": ""}
    )

    dataset.drop(
        columns=[
            "ISBN_10",
            "idioma",
            "male",
            "female",
            "querem_ler",
            "lendo",
            "resenha",
            "abandonos",
            "relendo",
            "leram",
        ],
        inplace=True,
    )

    dataset["titulo"] = dataset["titulo"].apply(
        lambda x: x if isinstance(x, str) else ""
    )
    dataset["descricao"] = dataset["descricao"].apply(
        lambda x: x if isinstance(x, str) else ""
    )
    dataset["ano"] = dataset["ano"].apply(lambda x: int(x) if x > 0 else 0)
    dataset["genero"] = dataset["genero"].apply(
        lambda x: x.lower().split(" / ") if isinstance(x, str) else []
    )
    dataset["genero"] = dataset["genero"].apply(
        lambda genres: [
            g.lower().strip() for g in genres if len(g.split()) <= 4 and g.strip()
        ]
    )
    dataset["generos_unificados"] = dataset["genero"].apply(
        lambda genres: list(
            set().union(
                *[
                    GENRE_MAP[g.strip().lower()]
                    for g in genres
                    if g.strip().lower() in GENRE_MAP
                ]
            )
        )
    )

    dataset["rating"] = dataset["rating"].apply(lambda x: min(max(x, 0), 5))
    dataset["avaliacao"] = dataset["avaliacao"].apply(lambda x: int(x) if x > 0 else 0)

    C = dataset["rating"].mean()
    m = 50

    dataset["rating"] = ((dataset["rating"] * dataset["avaliacao"]) + (C * m)) / (
        dataset["avaliacao"] + m
    )

    dataset["autor"] = dataset["autor"].apply(lambda x: x if isinstance(x, str) else "")
    dataset["editora"] = dataset["editora"].apply(
        lambda x: x if isinstance(x, str) else ""
    )
    dataset["paginas"] = dataset["paginas"].apply(lambda x: int(x) if x > 0 else 0)
    dataset["metadata"] = dataset.apply(
        lambda row: {
            "autor": row["autor"] if isinstance(row["autor"], str) else "",
            "editora": row["editora"] if isinstance(row["editora"], str) else "",
            "paginas": int(row["paginas"]) if row["paginas"] > 0 else 0,
            "isbn13": (
                str(row["ISBN_13"]) if isinstance(row["ISBN_13"], (str, int)) else ""
            ),
        },
        axis=1,
    )
    dataset["ISBN_13"] = dataset["ISBN_13"].apply(
        lambda x: str(int(x)) if not pd.isna(x) else ""
    )
    dataset = dataset[dataset["ISBN_13"] != ""]
    dataset.drop(columns=["autor", "editora", "paginas"], inplace=True)

    dataset.rename(
        columns={
            "ano": "ano_lancamento",
            "genero": "generos",
            "avaliacao": "num_avaliacoes",
        },
        inplace=True,
    )
    dataset.drop_duplicates(subset=["ISBN_13"])
    dataset.to_csv("data/livros_tratados.csv", index=False)


def tratar_filmes():
    dataset = pd.read_csv("data/filmes.csv")
    dataset.drop(
        columns=[
            "budget",
            "gross_world_wide",
            "writer",
            "gross_us_canada",
            "gross_opening_weekend",
            "nomination",
            "oscar",
            "language",
            "production_company",
            "country_origin",
            "rating_mpa",
            "link",
            "filming_location",
            "win",
        ],
        inplace=True,
    )

    dataset["year"] = dataset["year"].apply(lambda x: int(x) if x > 0 else 0)
    dataset["title"] = dataset["title"].apply(lambda x: x if isinstance(x, str) else "")
    dataset["genre"] = dataset["genre"].apply(
        lambda x: x.strip().lower().split(",") if isinstance(x, str) else []
    )
    dataset["generos_unificados"] = dataset["genre"].apply(
        lambda genres: list(
            set().union(
                *[
                    GENRE_MAP[g.strip().lower()]
                    for g in genres
                    if g.strip().lower() in GENRE_MAP
                ]
            )
        )
    )
    dataset["duration"] = dataset["duration"].apply(
        lambda x: x if isinstance(x, str) else ""
    )
    dataset["director"] = dataset["director"].apply(
        lambda x: x if isinstance(x, str) else ""
    )
    dataset["star"] = dataset["star"].apply(lambda x: x if isinstance(x, str) else "")
    dataset["rating_imdb"] = dataset["rating_imdb"].apply(
        lambda x: min(max(x, 0), 10) / 2
    )
    dataset["metadata"] = dataset.apply(
        lambda row: {
            "duracao": row["duration"] if isinstance(row["duration"], str) else "",
            "diretor": row["director"] if isinstance(row["director"], str) else "",
            "star": row["star"] if isinstance(row["star"], str) else "",
            "imdb_id": row["id"] if isinstance(row["id"], str) else "",
        },
        axis=1,
    )
    dataset["vote"] = dataset["vote"].apply(lambda x: int(x) if x > 0 else 0)
    dataset.drop(columns=["duration", "director", "star"], inplace=True)

    dataset.rename(
        columns={
            "year": "ano_lancamento",
            "title": "titulo",
            "genre": "generos",
            "rating_imdb": "rating",
            "vote": "num_avaliacoes",
        },
        inplace=True,
    )
    dataset.to_csv("data/filmes_tratados.csv", index=False)


def tratar_jogos():
    dataset = pd.read_csv("data/jogos.csv")

    dataset.drop(
        columns=["Unnamed: 0", "Backlogs", "Wishlist", "Lists", "Reviews", "Playing"],
        inplace=True,
    )

    def str_to_int(x):
        if not isinstance(x, str):
            return 0
        x = x.lower().strip()
        multipliers = {
            "k": 1_000,
            "m": 1_000_000,
            "b": 1_000_000_000,
            "t": 1_000_000_000_000,
        }
        if x[-1] in multipliers:
            number_part = float(x[:-1])
            multiplier = multipliers[x[-1]]
            return int(number_part * multiplier)
        return int(float(x))

    dataset["Plays"] = dataset["Plays"].apply(lambda x: str_to_int(x))
    dataset["Rating"] = dataset["Rating"].apply(
        lambda x: min(max(x, 0), 5) if not pd.isna(x) else 0
    )

    C = dataset["Rating"].mean()
    m = 50

    dataset["Rating"] = ((dataset["Rating"] * dataset["Plays"]) + (C * m)) / (
        dataset["Plays"] + m
    )
    dataset["Release_Date"] = dataset["Release_Date"].apply(
        lambda x: int(x.split(" ")[-1]) if (not pd.isna(x) and " " in x) else 0
    )
    dataset["Developers"] = dataset["Developers"].apply(
        lambda x: str(x) if isinstance(x, str) else ""
    )
    dataset["Summary"] = dataset["Summary"].apply(
        lambda x: str(x) if isinstance(x, str) else ""
    )
    dataset["Platforms"] = dataset["Platforms"].apply(
        lambda x: str(x) if isinstance(x, str) else ""
    )
    dataset["Title"] = dataset["Title"].apply(
        lambda x: str(x) if isinstance(x, str) else ""
    )

    def clean_genres(x):
        if isinstance(x, str) and x.startswith("[") and x.endswith("]"):
            try:
                x = ast.literal_eval(x)
            except (ValueError, SyntaxError):
                pass

        if isinstance(x, list):
            return [g.strip() for g in x]

        return [str(x).strip()]

    dataset["Genres"] = dataset["Genres"].apply(clean_genres)
    dataset["generos_unificados"] = dataset["Genres"].apply(
        lambda genres: list(
            set().union(
                *[
                    GENRE_MAP[g.strip().lower()]
                    for g in genres
                    if g.strip().lower() in GENRE_MAP
                ]
            )
        )
    )
    dataset.rename(
        columns={
            "Release_Date": "ano_lancamento",
            "Title": "titulo",
            "Genres": "generos",
            "Rating": "rating",
            "Summary": "descricao",
            "Plays": "num_avaliacoes",
        },
        inplace=True,
    )
    dataset["metadata"] = dataset.apply(
        lambda row: {
            "desenvlovedores": (
                row["Developers"] if isinstance(row["Developers"], str) else ""
            ),
            "plataformas": (
                row["Platforms"] if isinstance(row["Platforms"], str) else ""
            ),
        },
        axis=1,
    )
    dataset.drop(columns=["Developers", "Platforms"], inplace=True)
    dataset.to_csv("data/jogos_tratados.csv", index=False)


if __name__ == "__main__":
    # tratar_jogos()
    # tratar_filmes()
    tratar_livros()
