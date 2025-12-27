import pandas as pd
import os

# --- Configurações ---
original_path = r"ETL\data\anime.csv"  # The source of the missing column
target_path = r"ETL\data\anime_titulos.csv"  # The file you want to fix


def main():
    print("Reading files...")

    # 1. Read the original file (we only need 'id' and the missing column)
    # Check if the column name is exactly 'ano_lancamento' in your CSV
    df_original = pd.read_csv(original_path)

    # Select only the columns we need to merge
    # If the column name is different (e.g., 'start_year'), change it here
    cols_to_add = ["id", "ano_lancamento"]

    if "ano_lancamento" not in df_original.columns:
        print(f"Error: Column 'ano_lancamento' not found in {original_path}")
        print(f"Available columns: {list(df_original.columns)}")
        return

    df_source = df_original[cols_to_add].dropna(subset=["id"])
    df_source["id"] = df_source["id"].astype(int)

    # 2. Read the target file (the one you scraped)
    if not os.path.exists(target_path):
        print(f"Error: Target file {target_path} not found.")
        return

    df_target = pd.read_csv(target_path)

    # Ensure ID is int for proper matching
    df_target["id"] = df_target["id"].astype(int)

    print(f"Original rows: {len(df_source)}")
    print(f"Target rows to update: {len(df_target)}")

    # 3. Merge
    # We use 'left' to keep everything in target, just adding the info from source
    print("Merging data...")
    df_merged = pd.merge(df_target, df_source, on="id", how="left")

    # 4. Save
    # We overwrite the target file. If you want a backup, change the filename below.
    output_filename = r"ETL\data\anime_titulos_fixed.csv"

    df_merged.to_csv(output_filename, index=False)

    print("------------------------------------------------")
    print(f"Done! File saved as: {output_filename}")
    print(f"Total rows: {len(df_merged)}")
    print("Sample:")
    print(df_merged[["id", "titulo", "ano_lancamento"]].head())


if __name__ == "__main__":
    main()
