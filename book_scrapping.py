import asyncio
import aiohttp
import csv
import os
import re
import html
import random
from bs4 import BeautifulSoup
from tqdm import tqdm

# ================= CONFIG =================
ARQUIVO_SAIDA = "skoop_scrapping_raw.csv"
ARQUIVO_FALHAS = "falhas.log"
ID_INICIAL = 1
ID_FINAL = 1_000_000

CONCURRENCY = 10
BATCH_SIZE = 25
TIMEOUT = aiohttp.ClientTimeout(total=15)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
]

HEADERS_BASE = {
    "Accept-Language": "pt-BR,pt;q=0.9",
}


# ================= UTIL =================
def limpar_texto(texto: str | None) -> str | None:
    if not texto:
        return None
    texto = html.unescape(texto)
    texto = re.sub(r"<[^>]+>", "", texto)
    return re.sub(r"\s+", " ", texto).strip(" -")


def carregar_ids_salvos() -> set[int]:
    if not os.path.isfile(ARQUIVO_SAIDA):
        return set()

    with open(ARQUIVO_SAIDA, encoding="utf-8") as f:
        return {
            int(row["id"]) for row in csv.DictReader(f) if row.get("id", "").isdigit()
        }


def carregar_ids_falhos() -> set[int]:
    if not os.path.isfile(ARQUIVO_FALHAS):
        return set()

    ids = set()
    with open(ARQUIVO_FALHAS, encoding="utf-8") as f:
        for linha in f:
            match = re.search(r"ID=(\d+)", linha)
            if match:
                ids.add(int(match.group(1)))
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


def log_falha(book_id, motivo):
    with open(ARQUIVO_FALHAS, "a", encoding="utf-8") as f:
        f.write(f"ID={book_id} | {motivo}\n")


# ================= HTTP =================
async def fetch(session: aiohttp.ClientSession, book_id: int) -> str | None:
    url = f"https://www.skoob.com.br/book/{book_id}"

    headers = {
        **HEADERS_BASE,
        "User-Agent": random.choice(USER_AGENTS),
    }

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200 or "ops!" in str(resp.url):
                log_falha(book_id, f"HTTP {resp.status}")
                return None
            return await resp.text()

    except asyncio.TimeoutError:
        log_falha(book_id, "Timeout")
    except aiohttp.ClientError as e:
        log_falha(book_id, f"ClientError {e}")

    return None


# ================= PARSER =================
def parse(book_id: int, html_text: str) -> dict | None:
    soup = BeautifulSoup(html_text, "lxml")

    titulo_tag = soup.find("h1")
    if not titulo_tag:
        log_falha(book_id, "Título não encontrado")
        return None

    descricao_tag = soup.find("p", itemprop="description")
    descricao = descricao_tag.get_text(strip=True) if descricao_tag else None
    if not descricao:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        descricao = meta_desc["content"] if meta_desc else ""

    isbn = extrair_isbn(soup)
    if not isbn or not isbn.isdigit():
        log_falha(book_id, "ISBN inválido")
        return None

    return {
        "id": book_id,
        "isbn": isbn,
        "titulo": limpar_texto(titulo_tag.text),
        "descricao": limpar_texto(descricao),
    }


# ================= WORKER =================
async def worker(queue, session, semaphore, buffer, pbar):
    while True:
        book_id = await queue.get()
        if book_id is None:
            queue.task_done()
            break

        async with semaphore:
            html_text = await fetch(session, book_id)
            if html_text:
                resultado = parse(book_id, html_text)
                if resultado:
                    buffer.append(resultado)

        pbar.update(1)

        if len(buffer) >= BATCH_SIZE:
            salvar_lote(buffer)
            buffer.clear()

        queue.task_done()


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
async def main():
    ids_salvos = carregar_ids_salvos()
    ids_falhos = carregar_ids_falhos()
    ids = [
        i
        for i in range(ID_INICIAL, ID_FINAL + 1)
        if i not in ids_salvos and i not in ids_falhos
    ]

    total = ID_FINAL - ID_INICIAL + 1 - len(ids_salvos) - len(ids_falhos)
    print(f"IDs para processar: {total}")

    queue = asyncio.Queue(maxsize=CONCURRENCY * 3)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    buffer = []

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        with tqdm(total=total, desc="Scraping", unit="livro") as pbar:

            workers = [
                asyncio.create_task(worker(queue, session, semaphore, buffer, pbar))
                for _ in range(CONCURRENCY)
            ]

            for book_id in ids:
                await queue.put(book_id)

            # sinaliza fim
            for _ in workers:
                await queue.put(None)

            await queue.join()

            for w in workers:
                await w

    salvar_lote(buffer)
    print("Finalizado!")


if __name__ == "__main__":
    asyncio.run(main())
