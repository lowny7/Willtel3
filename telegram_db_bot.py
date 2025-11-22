#!/usr/bin/env python3
"""
telegram_db_bot.py - Bot Telegram que consulta um PostgreSQL e retorna resultados.
Comandos:
  /start               - mensagem de boas-vindas
  /search <termo>      - procura termo nas colunas textuais e retorna até 10 resultados
  /count <termo>       - retorna quantos resultados existem para o termo
  /help                - ajuda rápida

Configuração via variáveis de ambiente:
  BOT_TOKEN      - token do bot (obrigatório)
  DATABASE_URL   - string de conexão Postgres (padrão usado se não fornecido)
  RESULTS_LIMIT  - número máximo de resultados por consulta (opcional, default 10)

Uso:
  export BOT_TOKEN="123:ABC..."
  export DATABASE_URL="postgresql://postgres:docker@localhost/telegram"
  python3 telegram_db_bot.py
"""
import os
import html
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telegram import ParseMode
from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram import Update

# Carrega .env se existir
load_dotenv()

# Configurações
BOT_TOKEN = os.environ.get("6158712854:AAF47WpjOgNWwWkthOgRNkKcs3mdoblCWTA")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:docker@localhost/telegram")
RESULTS_LIMIT = int(os.environ.get("RESULTS_LIMIT", "10"))

# Logging básico
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    logger.error("BOT_TOKEN não definido. Defina a variável de ambiente BOT_TOKEN.")
    raise SystemExit("BOT_TOKEN não definido. Ex.: export BOT_TOKEN='seu_token_aqui'")

def get_connection():
    # Cria nova conexão a cada chamada para robustez em ambientes instáveis (Termux)
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def format_result(row):
    # Formata um registro para envio no Telegram (HTML)
    repo = html.escape(row.get("repo") or "")
    path = html.escape(row.get("path") or "")
    url = row.get("html_url") or ""
    snippet = html.escape((row.get("snippet") or "")[:600])
    score = row.get("score")
    found_at = row.get("found_at")
    parts = []
    if repo:
        parts.append(f"<b>Repo:</b> {repo}")
    if path:
        parts.append(f"<b>Path:</b> {path}")
    if url:
        # apresentar link clicável
        parts.append(f"<b>URL:</b> <a href=\"{html.escape(url)}\">link</a>")
    if score is not None:
        parts.append(f"<b>Score:</b> {score}")
    if found_at:
        parts.append(f"<b>Found:</b> {found_at}")
    if snippet:
        parts.append(f"<pre>{snippet}</pre>")
    return "\n".join(parts)

def search_db(term: str, limit: int = RESULTS_LIMIT):
    # Pesquisa segura usando parâmetros; procura em colunas textuais comuns
    q = f"%{term}%"
    sql = """
    SELECT source, repo, path, html_url, score, snippet, found_at
    FROM public.search_results
    WHERE (COALESCE(source,'') ILIKE %s
        OR COALESCE(repo,'') ILIKE %s
        OR COALESCE(path,'') ILIKE %s
        OR COALESCE(html_url,'') ILIKE %s
        OR COALESCE(snippet,'') ILIKE %s)
    ORDER BY found_at DESC
    LIMIT %s;
    """
    params = (q, q, q, q, q, limit)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()

def count_db(term: str):
    q = f"%{term}%"
    sql = """
    SELECT COUNT(*) AS cnt
    FROM public.search_results
    WHERE (COALESCE(source,'') ILIKE %s
        OR COALESCE(repo,'') ILIKE %s
        OR COALESCE(path,'') ILIKE %s
        OR COALESCE(html_url,'') ILIKE %s
        OR COALESCE(snippet,'') ILIKE %s);
    """
    params = (q, q, q, q, q)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            r = cur.fetchone()
            return r["cnt"] if r else 0
    finally:
        conn.close()

# Handlers do bot
def start(update: Update, context: CallbackContext):
    update.message.reply_text(
        "Olá — sou um bot que consulta a base PostgreSQL local.\n"
        "Use /search <termo> para procurar (ex.: /search lowny7) ou /count <termo> para ver quantos resultados."
    )

def help_cmd(update: Update, context: CallbackContext):
    update.message.reply_text(
        "/search <termo>  — procura o termo nas colunas textuais (retorna até {} resultados)\n"
        "/count <termo>   — conta quantos resultados existem para o termo".format(RESULTS_LIMIT)
    )

def search_cmd(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text("Uso: /search <termo>")
        return
    term = " ".join(context.args).strip()
    update.message.reply_text(f"Procurando por: {html.escape(term)} ...")
    try:
        rows = search_db(term)
    except Exception as e:
        logger.exception("Erro ao buscar no banco")
        update.message.reply_text("Erro ao consultar o banco de dados.")
        return
    if not rows:
        update.message.reply_text("Nenhum resultado encontrado.")
        return
    # enviar resultados (um por mensagem para evitar limites de caracteres)
    for row in rows:
        text = format_result(row)
        try:
            update.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception:
            # fallback sem HTML se houver erro
            update.message.reply_text(text)

def count_cmd(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text("Uso: /count <termo>")
        return
    term = " ".join(context.args).strip()
    try:
        cnt = count_db(term)
    except Exception:
        logger.exception("Erro ao contar no banco")
        update.message.reply_text("Erro ao consultar o banco de dados.")
        return
    update.message.reply_text(f"Resultados para '{html.escape(term)}': {cnt}")

def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(CommandHandler("search", search_cmd))
    dp.add_handler(CommandHandler("count", count_cmd))

    logger.info("Bot iniciado. Pressione Ctrl+C para parar.")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
