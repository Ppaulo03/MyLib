import pandas as pd

dataset = pd.read_csv("data/tmdb_movies.csv")
dataset = dataset.drop_duplicates(subset=["id"])
C = dataset["rating"].mean()
m = dataset["num_avaliacoes"].mean()
dataset["rating"] = ((dataset["rating"] * dataset["num_avaliacoes"]) + (C * m)) / (
    dataset["num_avaliacoes"] + m
)
dataset.to_csv("data/tmdb_movies_treated.csv", index=False)
print(dataset.head()[["rating", "num_avaliacoes"]])
