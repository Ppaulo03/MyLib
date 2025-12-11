import pandas as pd

csv = "dataset_completo_igdb"
df = pd.read_csv(f"{csv}.csv")

# get outer wilds
item = df[df["titulo"] == "Outer Wilds"].index
print(df.loc[item])
# df = df[df["num_avaliacoes"] > 0]

# C = df["rating"].mean()
# m = df["num_avaliacoes"].quantile(0.90)

# df["rating"] = ((df["rating"] * df["num_avaliacoes"]) + (C * m)) / (
#     df["num_avaliacoes"] + m
# )

# df.to_csv(f"{csv}_tratado.csv", index=False)
