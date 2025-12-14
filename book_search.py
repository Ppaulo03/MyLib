import requests
import time
import pandas as pd
import os
import ast
import re


def fetch_open_library_data(isbn, dados_skoob):
    if not isbn:
        return None

    # Endpoint Direto (Bibliographic Data) - Muito mais confiável que o Search
    url = (
        f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&jscmd=data&format=json"
    )

    try:
        response = requests.get(url)
        if response.status_code != 200:
            return None

        data_raw = response.json()

        # A chave do JSON retornado é "ISBN:seu_numero"
        key = f"ISBN:{isbn}"

        if key not in data_raw:
            # Tenta sem o prefixo ISBN caso a API devolva diferente (raro, mas acontece)
            if isbn in data_raw:
                item = data_raw[isbn]
            else:
                return None  # Realmente não encontrou
        else:
            item = data_raw[key]

        # --- Extração de Dados ---

        # 1. Título
        titulo = item.get("title", dados_skoob.get("titulo"))

        # 2. Ano de Lançamento
        # A API retorna strings como "2002", "Nov 2002", "November 5, 2002".
        # Vamos extrair apenas os 4 dígitos do ano.
        data_pub = item.get("publish_date", "")
        ano_match = re.search(r"\d{4}", data_pub)
        ano = int(ano_match.group(0)) if ano_match else None

        # 3. Autores
        # Diferente do Search, aqui vem uma lista de objetos: [{'name': 'Rowling', 'url': ...}]
        lista_autores = item.get("authors", [])
        nome_autor = lista_autores[0]["name"] if lista_autores else "Desconhecido"

        # 4. Gêneros (Subjects)
        subjects = []
        if "subjects" in item:
            # Pegamos os top 5, mas filtramos para ter apenas strings (as vezes vem objetos)
            subjects = [
                s["name"] if isinstance(s, dict) else s for s in item["subjects"]
            ][:5]

        # 5. Editora
        lista_editoras = item.get("publishers", [])
        nome_editora = lista_editoras[0]["name"] if lista_editoras else "Desconhecida"

        # 6. Imagem
        # O jscmd=data geralmente retorna um objeto 'cover' com urls
        imagem_url = None
        if "cover" in item and "large" in item["cover"]:
            imagem_url = item["cover"]["large"]
        elif "cover" in item and "medium" in item["cover"]:
            imagem_url = item["cover"]["medium"]

        # 7. Descrição e Ratings (Geralmente fracos nessa API, mantemos Skoob/Zero)
        # Rating não vem no jscmd=data, teríamos que fazer outra call.
        # Sugiro manter 0 ou None para preencher depois se for crítico.
        rating = None
        num_avaliacoes = 0

        # Montagem Final
        metadata = {"autor": nome_autor, "isbn": isbn, "editora": nome_editora}

        return {
            "titulo": titulo,
            "ano_lancamento": ano,
            "generos": subjects,
            "rating": rating,  # API jscmd=data não traz rating
            "num_avaliacoes": num_avaliacoes,
            "imagem": imagem_url,
            "descricao": dados_skoob.get("descricao"),  # Prioriza Skoob
            "metadata": metadata,
        }

    except Exception as e:
        print(f"Erro no ISBN {isbn}: {e}")
        return None


# --- Exemplo de Uso ---

csv = "livros_pt_br.csv"
dados_skoob = pd.read_csv(csv)
dados_skoob_list = dados_skoob.to_dict(orient="records")

output_file = "dados_livros_enriquecidos.csv"
colunas_finais = [
    "titulo",
    "ano_lancamento",
    "generos",
    "rating",
    "num_avaliacoes",
    "imagem",
    "descricao",
    "metadata",
]

isbns_processados = set()

if os.path.exists(output_file):
    try:
        df_existente = pd.read_csv(output_file, usecols=["metadata"])
        for meta_str in df_existente["metadata"]:
            print(meta_str)
            try:
                meta_dict = ast.literal_eval(meta_str)
                if "isbn" in meta_dict:
                    isbns_processados.add(str(meta_dict["isbn"]))
            except:
                continue
        print(f"Retomando: {len(isbns_processados)} livros já encontrados no arquivo.")
    except Exception as e:
        print(f"Arquivo existe mas houve erro ao ler (pode estar vazio): {e}")

print("Iniciando enriquecimento de dados...")

for item in dados_skoob_list:
    isbn_clean = str(item["isbn"]).replace("-", "").strip()

    if isbn_clean in isbns_processados:
        continue

    resultado = fetch_open_library_data(isbn_clean, item)
    item_final = {}
    if resultado:
        item_final = resultado
    else:
        continue

    df_temp = pd.DataFrame([item_final])
    if not os.path.exists(output_file):
        df_temp.to_csv(output_file, index=False, mode="w", header=True)
    else:
        df_temp.to_csv(output_file, index=False, mode="a", header=False)
    isbns_processados.add(isbn_clean)
    time.sleep(1)
