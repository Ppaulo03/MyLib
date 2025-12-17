import pandas as pd
from time import time


def clean_csv(csv, timestamp):
    df = pd.read_csv(f"ETL/data/raw/{csv}_raw.csv")
    df = df.drop_duplicates(subset=["titulo"])
    # get the range of ratings
    while df["rating"].max() > 5:
        df["rating"] = df["rating"] / 2

    C = df["rating"].mean()
    m = df["num_avaliacoes"].mean()
    df["rating"] = ((df["rating"] * df["num_avaliacoes"]) + (C * m)) / (
        df["num_avaliacoes"] + m
    )
    df.to_csv(f"ETL/data/{csv}_{timestamp}.csv", index=False)


if __name__ == "__main__":
    csvs = ["manga"]
    timestamp = int(time())
    for csv in csvs:
        clean_csv(csv, timestamp)
