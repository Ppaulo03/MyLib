import pandas as pd
from langdetect import detect, LangDetectException

INPUT_FILE = "skoop_scrapping_raw.csv"


def verificar_idioma(texto):

    if pd.isna(texto) or str(texto).strip() == "":
        return "indefinido"

    texto = str(texto)

    if len(texto) < 25:
        return "indefinido"

    try:
        lang = detect(texto)
        if lang == "pt":
            return "pt"
        else:
            return "gringo"
    except LangDetectException:
        return "indefinido"


print("Carregando CSV...")
try:
    df = pd.read_csv(INPUT_FILE)
except FileNotFoundError:
    print(f"Erro: Arquivo {INPUT_FILE} não encontrado.")
    exit()

print(f"Total de linhas carregadas: {len(df)}")
print("Analisando idiomas das descrições...")

df["analise_lang"] = df["descricao"].apply(verificar_idioma)


df_pt = df[df["analise_lang"].isin(["pt", "indefinido"])].copy()
df_gringo = df[df["analise_lang"] == "gringo"].copy()

df_pt.drop(columns=["analise_lang"], inplace=True)
df_gringo_view = df_gringo[["titulo", "descricao", "analise_lang"]]

print("-" * 30)
print(f"Livros Mantidos (PT-BR ou Sem Texto): {len(df_pt)}")
print(f"Livros Rejeitados (Outros idiomas): {len(df_gringo)}")
print("-" * 30)

if not df_gringo.empty:
    print("Exemplos de rejeitados:")
    print(df_gringo[["titulo", "descricao"]].head(3))

df_pt.to_csv("livros_pt_br.csv", index=False)

print("\nProcesso concluído.")
print("-> 'livros_pt_br.csv' pronto para a próxima etapa.")
