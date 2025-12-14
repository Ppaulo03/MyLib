import csv
import os
import re
import html
import time
import random
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import threading

# ================= CONFIG =================
ARQUIVO_SAIDA = "skoop_scrapping_raw.csv"
THREADS = 1
ID_INICIAL = 1
ID_FINAL = 10
BATCH_SIZE = 10
TIMEOUT = 10

ua = UserAgent()
_lock_log = threading.Lock()


# ================= UTIL =================
def limpar_texto(texto: str | None) -> str | None:
    if not texto:
        return None

    texto = html.unescape(texto)
    texto = re.sub(r"<[^>]+>", "", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip().rstrip("-").strip()


def carregar_ids_salvos() -> set[int]:
    if not os.path.isfile(ARQUIVO_SAIDA):
        return set()

    ids = set()
    with open(ARQUIVO_SAIDA, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                ids.add(int(row["id"]))
            except Exception:
                pass
    return ids


def extrair_isbn(soup: BeautifulSoup) -> str | None:
    texto = soup.get_text(" ", strip=True)
    match = re.search(
        r"ISBN(?:\s*-?\s*(?:10|13))?\s*[:\-]?\s*([0-9Xx\-]{10,17})",
        texto,
    )

    if not match:
        return None

    isbn = match.group(1).replace("-", "").upper()
    if len(isbn) == 10 or len(isbn) == 13:
        return isbn

    return None


def log_falha(book_id, motivo, extra=None):

    linha = f"ID={book_id} | {motivo}"
    if extra:
        linha += f" | {extra}"

    with _lock_log:
        with open("falhas.log", "a", encoding="utf-8") as f:
            f.write(linha + "\n")


# ================= SCRAPER =================
def processar_livro(book_id):
    url = f"https://www.skoob.com.br/book/{book_id}"
    headers = {
        "User-Agent": ua.random,
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    try:

        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        if resp.status_code != 200 or "ops!" in resp.url:
            log_falha(
                book_id,
                "Página não encontrada ou erro HTTP",
                f"Status: {resp.status_code}",
            )
            return None

        soup = BeautifulSoup(resp.content, "html.parser")

        # 1. Título
        titulo_tag = soup.find("h1")
        if not titulo_tag:
            print(resp.text)
            print(resp.url)
            log_falha(book_id, "Título não encontrado")
            return None

        titulo = limpar_texto(titulo_tag.get_text())
        descricao_tag = soup.find("p", itemprop="description")
        descricao = limpar_texto(
            descricao_tag.get_text()
            if descricao_tag
            else soup.find("meta", attrs={"name": "description"}).get("content")
        )

        isbn = extrair_isbn(soup)
        if not isbn or not isbn.isdigit() or int(isbn) == 0:
            log_falha(book_id, "ISBN inválido ou não encontrado")
            return None
        # ---------- OpenLibrary ----------
        ol = (
            requests.get(
                f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&jscmd=data&format=json",
                timeout=TIMEOUT,
            )
            .json()
            .get(f"ISBN:{isbn}")
        )

        if not ol:
            log_falha(book_id, "Livro não encontrado na OpenLibrary", f"ISBN: {isbn}")
            return None

        edition = requests.get(
            f"https://openlibrary.org/isbn/{isbn}.json",
            timeout=TIMEOUT,
        ).json()

        work_id = edition.get("works", [{}])[0].get("key", "").split("/")[-1]
        if not work_id:
            log_falha(book_id, "Work ID não encontrado na edição", f"ISBN: {isbn}")
            return None

        work_id = edition.get("works", [{}])[0].get("key", "").split("/")[-1]
        if not work_id:
            log_falha(book_id, "Work ID não encontrado na edição", f"ISBN: {isbn}")
            return None

        work = (
            requests.get(
                f"https://openlibrary.org/search.json?q=key:/works/{work_id}",
                timeout=TIMEOUT,
            )
            .json()
            .get("docs", [{}])[0]
        )

        ratings = (
            requests.get(
                f"https://openlibrary.org/works/{work_id}/ratings.json",
                timeout=TIMEOUT,
            )
            .json()
            .get("summary", {})
        )

        livro = {
            "id": book_id,
            "titulo": ol.get("title"),
            "descricao": descricao,
            "isbn": isbn,
        }

        print(f"[OK] ID {book_id} - {titulo}")
        return livro

    except Exception as e:
        log_falha(book_id, "Erro ao processar livro", str(e))
        return None


# ================= CSV =================
def salvar_lote(livros: list[dict]):
    if not livros:
        return

    existe = os.path.isfile(ARQUIVO_SAIDA)
    with open(ARQUIVO_SAIDA, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=livros[0].keys())
        if not existe:
            writer.writeheader()
        writer.writerows(livros)


# ================= MAIN =================
def main():
    ids_salvos = carregar_ids_salvos()
    ids = [i for i in range(ID_INICIAL, ID_FINAL + 1) if i not in ids_salvos]
    print(f"IDs para processar: {len(ids)}")
    buffer = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(processar_livro, i): i for i in ids}

        for future in concurrent.futures.as_completed(futures):
            resultado = future.result()
            if resultado:
                buffer.append(resultado)

            if len(buffer) >= BATCH_SIZE:
                salvar_lote(buffer)
                buffer.clear()
    salvar_lote(buffer)
    print("Finalizado!")


if __name__ == "__main__":
    main()
