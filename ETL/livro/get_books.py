import pandas as pd
import requests
import time
import csv
import os

# --- CONFIGURAÇÕES ---
INPUT_CSV = r"ETL\livro\books.csv"  # Seu arquivo de entrada
OUTPUT_CSV = r"ETL\livro\dataset_livros_pt_br.csv"  # Arquivo final
GOOGLE_API_KEY = None  # Se tiver uma chave, coloque aqui: "SUA_CHAVE"


def get_portuguese_version(original_title, author):
    clean_title = original_title.split("(")[0].strip()

    # Busca relaxada para permitir tradução + palavras-chave de reforço
    query = f"{clean_title} portugues inauthor:{author}"

    url = f"https://www.googleapis.com/books/v1/volumes?q={query}&langRestrict=pt&maxResults=10"
    if GOOGLE_API_KEY:
        url += f"&key={GOOGLE_API_KEY}"

    try:
        resp = requests.get(url)

        # Tratamento de Rate Limit (Erro 429)
        if resp.status_code == 429:
            print(f"⏳ Rate limit atingido. Aguardando 5s...")
            time.sleep(5)
            return get_portuguese_version(original_title, author)

        data = resp.json()
        if "items" not in data:
            return None

        # Loop de validação (os filtros que discutimos)
        for item in data["items"]:
            vol = item["volumeInfo"]

            # 1. Idioma deve ser PT
            if vol.get("language") not in ["pt", "pt-BR", "por"]:
                continue

            # 2. Autor deve bater (fuzzy match simples pelo sobrenome)
            found_authors = " ".join(vol.get("authors", [])).lower()
            author_surname = author.split(" ")[-1].lower()
            if author_surname not in found_authors:
                continue

            # 3. Tem que ter ISBN
            isbn = _extract_isbn(vol.get("industryIdentifiers", []))
            if not isbn:
                continue

            # 4. Tem que ter Descrição (essencial para seu MyLib)
            description = vol.get("description", "")
            if not description or len(description) < 10:
                continue

            # Sucesso! Retorna formato limpo
            return {
                "isbn": isbn,
                "titulo_pt": vol.get("title"),
                "titulo_original": original_title,  # Título vindo do CSV de entrada
                "descricao": description,
                "imagem_url": vol.get("imageLinks", {}).get("thumbnail", ""),
                "autores": ", ".join(vol.get("authors", [])),
                "paginas": vol.get("pageCount", 0),
                "categorias": ", ".join(vol.get("categories", [])),
            }

        return None

    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
        return None


def _extract_isbn(identifiers):
    if not identifiers:
        return None
    # Preferência ISBN-13
    for i in identifiers:
        if i["type"] == "ISBN_13":
            return i["identifier"]
    # Fallback ISBN-10
    for i in identifiers:
        if i["type"] == "ISBN_10":
            return i["identifier"]
    return None


# --- EXECUÇÃO ---

# 1. Carrega dados de entrada (com tratamento de erro de leitura)
try:
    df_input = pd.read_csv(INPUT_CSV, on_bad_lines="skip")
except Exception as e:
    print(f"Erro ao ler CSV de entrada: {e}")
    # Cria dataframe dummy pra teste se não achar arquivo
    df_input = pd.DataFrame(
        [
            {
                "title": "Harry Potter and the Half-Blood Prince",
                "authors": "J.K. Rowling",
            },
            {"title": "The Pragmatic Programmer", "authors": "Andy Hunt"},
            {"title": "Clean Code", "authors": "Robert C. Martin"},
        ]
    )

# Limpeza básica na coluna de autores
if "authors" in df_input.columns:
    df_input["authors"] = df_input["authors"].apply(
        lambda x: str(x).split("/")[0].strip()
    )

# 2. Prepara o CSV de Saída
# Usamos o modo 'a' (append) se o arquivo já existir, ou 'w' (write) se for novo
file_exists = os.path.isfile(OUTPUT_CSV)
fieldnames = [
    "isbn",
    "titulo_pt",
    "titulo_original",
    "descricao",
    "imagem_url",
    "autores",
    "paginas",
    "categorias",
]

print(f"Iniciando processamento de {len(df_input)} livros...")
print(f"Salvando em: {OUTPUT_CSV}\n")

with open(
    OUTPUT_CSV, mode="a" if file_exists else "w", newline="", encoding="utf-8"
) as f_out:
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)

    if not file_exists:
        writer.writeheader()  # Escreve cabeçalho apenas se arquivo é novo

    success_count = 0

    # Itera sobre o DataFrame de entrada
    for index, row in df_input.iterrows():
        title_in = row.get("title")
        author_in = row.get("authors")

        print(f"Processando [{index+1}/{len(df_input)}]: {title_in}...", end="\r")

        # Chama a função de busca
        result = get_portuguese_version(title_in, author_in)

        if result:
            # Escreve no CSV imediatamente
            writer.writerow(result)
            success_count += 1
            print(f"✅ FOUND: {result['titulo_pt']} ({result['isbn']})            ")
        else:
            print(f"🔻 SKIP: Versão PT-BR não encontrada ou incompleta.        ")
            # Opcional: Se quiser salvar o livro em inglês como fallback,
            # adicione a lógica aqui (mas ficará sem descrição/capa a não ser que busque na API em EN)

        # Delay para não tomar Ban do Google
        time.sleep(1.0)

print(f"\n\n🏁 Concluído! {success_count} livros salvos com sucesso em '{OUTPUT_CSV}'.")
