import pandas as pd

NOME_ARQUIVO = "animes_validated.csv"

df = pd.read_csv(NOME_ARQUIVO)

# drop rows with ano_lancamento equal to missing

df = df[df["ano_lancamento"].notna()]
df["num_avaliacoes"] = df["num_avaliacoes"] * 2
df["rating"] = df["rating"] / 2
C = df["rating"].mean()
m = df["num_avaliacoes"].mean()
df["rating"] = ((df["rating"] * df["num_avaliacoes"]) + (C * m)) / (
    df["num_avaliacoes"] + m
)

df.drop_duplicates(subset=["id"], inplace=True)
df.to_csv("ETL/data/anime.csv", index=False)
