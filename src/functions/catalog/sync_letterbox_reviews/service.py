import time
import random
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from loguru import logger
from common.dynamo_client import db_client
from datetime import datetime, timezone

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "https://letterboxd.com/",
}

session = requests.Session()
retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)


def _scrape_review_text(review_url: str) -> str | None:
    try:
        # Sleep para evitar rate limit do Letterboxd
        time.sleep(random.uniform(0.5, 1.5))

        response = session.get(review_url, headers=HEADERS, timeout=10)

        if response.status_code == 404:
            logger.warning(f"Review não encontrada (404): {review_url}")
            return None

        response.raise_for_status()  # Lança erro para 5xx (SQS Retry)

        soup = BeautifulSoup(response.text, "html.parser")

        # Tentativa de seletores em ordem de prioridade
        selectors = ["div.review.body-text", ".review-body", ".body-text"]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(separator="\n\n", strip=True)

        logger.warning(f"Seletor de texto falhou para: {review_url}")
        return None

    except requests.RequestException as e:
        logger.error(f"Erro de rede ao acessar Letterboxd: {e}")
        raise e  # Relança para o handler pegar e o SQS tentar de novo


def process_review(message) -> bool:
    """
    Orquestra a verificação e atualização da review.
    Retorna True se processou/atualizou, False se pulou.
    """
    if not message.override:
        try:
            item_atual = db_client.get_item(message.user_id, message.sk)
            if item_atual and len(item_atual.get("review", "")) > 10:
                logger.info(f"Review já existe para {message.sk}. Pulando.")
                return False
        except Exception as e:
            logger.warning(f"Erro ao checar item existente: {e}")

    logger.info(f"Baixando review: {message.review_link}")
    review_text = _scrape_review_text(message.review_link)
    if review_text:
        db_client.update_item(
            message.user_id,
            message.sk,
            {
                "review": review_text,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        logger.info(f"Review salva com sucesso para {message.sk}")
        return True
    return False
