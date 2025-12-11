import requests
import csv
import time

# --- CONFIGURAÇÃO ---

# Lista de ASSUNTOS oficiais para varrer o banco de dados
# Isso garante variedade: ficção, não-ficção, técnico, arte, etc.
CATEGORIAS_PARA_MINERAR = [
    "subject:ficção",
    "subject:romance",
    "subject:história",
    # "subject:romance",
    # "subject:history",
    # "subject:science",
    # "subject:thriller",
    # "subject:horror",
    # "subject:biography",
    # "subject:computers",
    # "subject:psychology",
    # "subject:religion",
    # "subject:art",
    # "subject:cooking",
    # "subject:business",
    # "subject:poetry",
]

# Quantos livros tentar pegar por categoria? (Google limita a aprox 500-800 por termo)
LIVROS_POR_CATEGORIA = 10
FILENAME = "catalogo_livros_pt.csv"


def limpar_html(texto):
    """Remove tags HTML básicas que o Google às vezes manda"""
    if not texto:
        return ""
    return (
        texto.replace("<p>", "")
        .replace("</p>", "\n")
        .replace("<br>", "\n")
        .replace("<b>", "")
        .replace("</b>", "")
    )


def minerar_livros():
    livros_unicos = {}  # Dicionário para evitar duplicatas (Key = ID Google)
    total_coletado = 0

    print(f"--- INICIANDO MINERAÇÃO DE CATÁLOGO ---")
    print(
        f"Metas: {len(CATEGORIAS_PARA_MINERAR)} categorias x {LIVROS_POR_CATEGORIA} livros"
    )

    # Prepara o arquivo CSV (cria cabeçalho)
    colunas = [
        "id",
        "titulo",
        "subtitulo",
        "autores",
        "data",
        "paginas",
        "categorias",
        "idioma",
        "descricao",
        "imagem",
    ]
    with open(FILENAME, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()

    for categoria in CATEGORIAS_PARA_MINERAR:
        print(f"\n>>> Minerando categoria: {categoria} ...")
        start_index = 0
        livros_nessa_categoria = 0
        erros_consecutivos = 0

        while livros_nessa_categoria < LIVROS_POR_CATEGORIA:
            url = "https://www.googleapis.com/books/v1/volumes"
            params = {
                "q": categoria,  # Busca pelo Assunto
                "langRestrict": "pt",  # Pede em Português
                "printType": "books",  # Só livros (sem revistas)
                "maxResults": 40,  # Máximo por página
                "startIndex": start_index,
                "orderBy": "relevance",  # 'newest' também é bom para catálogo
            }

            try:
                res = requests.get(url, params=params)

                if res.status_code == 429:
                    print("Limite de requisições! Esperando 10s...")
                    time.sleep(10)
                    continue

                if res.status_code != 200:
                    print(f"Erro API: {res.status_code}")
                    break

                dados = res.json()
                if "items" not in dados:
                    print("Fim dos resultados para esta categoria.")
                    break

                lote_atual = []

                for item in dados["items"]:
                    info = item.get("volumeInfo", {})
                    book_id = item.get("id")

                    # 1. Checagem de Idioma (Hard Filter)
                    # Às vezes o langRestrict falha, então conferimos manualmente
                    idioma = info.get("language", "")
                    if idioma not in ["pt", "pt-BR", "pt-PT"]:
                        continue

                    # 2. Checagem de Duplicata Global
                    if book_id in livros_unicos:
                        continue

                    # 3. Processamento
                    titulo = info.get("title")
                    if not titulo:
                        continue  # Livro sem título não serve

                    autores = ", ".join(info.get("authors", []))
                    desc = limpar_html(info.get("description", ""))

                    img_dict = info.get("imageLinks", {})
                    imagem = (
                        img_dict.get("extraLarge")
                        or img_dict.get("large")
                        or img_dict.get("thumbnail")
                        or ""
                    )

                    # Adiciona ao dicionário global e ao lote de salvamento
                    entry = {
                        "id": book_id,
                        "titulo": titulo,
                        "subtitulo": info.get("subtitle", ""),
                        "autores": autores,
                        "data": info.get("publishedDate", ""),
                        "paginas": info.get("pageCount", 0),
                        "categorias": ", ".join(info.get("categories", [])),
                        "idioma": idioma,
                        "descricao": desc,  # Texto original do Google
                        "imagem": imagem,
                    }

                    livros_unicos[book_id] = True  # Marca como já pego
                    lote_atual.append(entry)

                # Salva o lote no CSV imediatamente (append mode)
                if lote_atual:
                    with open(FILENAME, "a", newline="", encoding="utf-8-sig") as f:
                        writer = csv.DictWriter(
                            f, fieldnames=colunas, extrasaction="ignore"
                        )
                        writer.writerows(lote_atual)

                    count = len(lote_atual)
                    livros_nessa_categoria += count
                    total_coletado += count
                    print(
                        f"   + {count} livros salvos (Total Cat: {livros_nessa_categoria})"
                    )
                else:
                    erros_consecutivos += 1
                    if (
                        erros_consecutivos > 3
                    ):  # Se 3 páginas vierem vazias (filtradas), para categoria
                        print("Muitos livros descartados, pulando categoria.")
                        break

                start_index += 40
                time.sleep(0.5)  # Respeita a API

            except Exception as e:
                print(f"Erro inesperado: {e}")
                break

    print(f"\n--- PROCESSO FINALIZADO ---")
    print(f"Total de livros no catálogo: {total_coletado}")
    print(f"Arquivo salvo em: {FILENAME}")


if __name__ == "__main__":
    minerar_livros()
