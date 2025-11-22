#!/usr/bin/env python3
import os
import re
import html
import logging
import asyncio
from urllib.parse import urlparse
from typing import Any, List, Optional

from fastapi import FastAPI, Request, HTTPException, Header, status
from pydantic import BaseModel

import pg8000

# telegram (python-telegram-bot v20+)
from telegram import __version__ as PTB_VERSION
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("app")

DEFAULT_DB_URL = "postgresql://postgres:docker@localhost/telegram"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", None)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", None)

HTTP_API_KEY = os.getenv("HTTP_API_KEY")
TELEGRAM_ALLOWED = os.getenv("TELEGRAM_ALLOWED")

if TELEGRAM_ALLOWED:
    try:
        ALLOWED_CHAT_IDS = set(
            int(x.strip()) for x in TELEGRAM_ALLOWED.split(",") if x.strip()
        )
    except Exception:
        ALLOWED_CHAT_IDS = set()
else:
    ALLOWED_CHAT_IDS = None

PORT = int(os.getenv("PORT", "8000"))

VALID_TABLE_RE = re.compile(r"^[A-Za-z0-9_]{1,63}$")
MAX_QUERY_LENGTH = 3000
FORBIDDEN_WORDS = [
    ";", "drop ", "delete ", "update ", "insert ", "truncate ", "alter ", "create "
]

app = FastAPI(title="Telegram+FastAPI PG (pg8000)")


def parse_database_url(url: str):
    p = urlparse(url)
    if p.scheme not in ("postgres", "postgresql"):
        raise ValueError("URL de banco deve ser postgresql://")
    return dict(
        user=p.username or "postgres",
        password=p.password or "",
        host=p.hostname or "localhost",
        port=p.port or 5432,
        database=p.path.lstrip("/") or "postgres",
    )


DB_PARAMS = parse_database_url(DATABASE_URL)


def _sync_run_query(sql: str, params: tuple = ()):
    conn = pg8000.connect(
        user=DB_PARAMS["user"],
        password=DB_PARAMS["password"],
        host=DB_PARAMS["host"],
        port=DB_PARAMS["port"],
        database=DB_PARAMS["database"],
        timeout=10,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        cur.close()
        return cols, rows
    finally:
        conn.close()


async def run_query(sql: str, params: tuple = ()):
    return await asyncio.to_thread(_sync_run_query, sql, params)


def format_results(cols: List[str], rows: List[tuple], max_rows: int = 200):
    if not cols:
        return "(nenhum resultado)"
    header = " | ".join(cols)
    lines = [header, "-" * len(header)]
    for idx, row in enumerate(rows):
        if idx >= max_rows:
            lines.append(f"... {len(rows) - max_rows} linhas omitidas ...")
            break
        safe = []
        for v in row:
            if isinstance(v, (bytes, bytearray)):
                safe.append(v.decode("utf-8", errors="replace"))
            elif v is None:
                safe.append("NULL")
            else:
                safe.append(str(v))
        lines.append(" | ".join(safe))
    return "\n".join(lines)


def query_is_safe(payload: str) -> bool:
    if not payload:
        return False
    if len(payload) > MAX_QUERY_LENGTH:
        return False
    if ";" in payload:
        return False
    lp = payload.lower()
    if not lp.lstrip().startswith("select"):
        return False
    for f in FORBIDDEN_WORDS:
        if f in lp:
            return False
    return True


class QueryIn(BaseModel):
    sql: str


async def require_api_key(x_api_key: Optional[str] = Header(None)):
    if HTTP_API_KEY and x_api_key != HTTP_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key inválida"
        )


@app.get("/health")
async def health():
    return {"status": "ok", "ptb_version": PTB_VERSION}


@app.get("/table/{table_name}")
async def http_table(
    table_name: str,
    limit: int = 20,
    api_key: Optional[str] = Header(None)
):
    await require_api_key(api_key)
    if not VALID_TABLE_RE.match(table_name):
        raise HTTPException(
            status_code=400,
            detail="Nome de tabela inválido."
        )

    limit = max(1, min(1000, int(limit)))
    sql = f'SELECT * FROM "{table_name}" LIMIT %s'
    cols, rows = await run_query(sql, (limit,))
    return {
        "columns": cols,
        "rows_count": len(rows),
        "rows_preview": [list(r) for r in rows[:limit]],
    }


@app.post("/query")
async def http_query(payload: QueryIn, api_key: Optional[str] = Header(None)):
    await require_api_key(api_key)
    sql = payload.sql.strip()
    if not query_is_safe(sql):
        raise HTTPException(
            status_code=400,
            detail="Query inválida ou não permitida."
        )
    cols, rows = await run_query(sql, ())
    return {
        "columns": cols,
        "rows_count": len(rows),
        "rows_preview": [list(r) for r in rows[:200]],
    }


telegram_app: Optional[Application] = None


async def is_chat_allowed(chat_id: int) -> bool:
    if ALLOWED_CHAT_IDS is None:
        return True
    return chat_id in ALLOWED_CHAT_IDS


async def tg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot ativo. Use /table <nome> [limite] ou /query <SELECT ...>"
    )


async def tg_table(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Uso: /table <nome_da_tabela> [limite]")
        return

    table = args[0]
    if not VALID_TABLE_RE.match(table):
        await update.message.reply_text("Nome de tabela inválido.")
        return

    limit = 20
    if len(args) > 1:
        try:
            limit = int(args[1])
            limit = max(1, min(1000, limit))
        except Exception:
            await update.message.reply_text("Limite inválido.")
            return

    sql = f'SELECT * FROM "{table}" LIMIT %s'
    cols, rows = await run_query(sql, (limit,))
    out = format_results(cols, rows, max_rows=limit)

    chunk = 3800
    for i in range(0, len(out), chunk):
        await update.message.reply_text(
            f"<pre>{html.escape(out[i:i+chunk])}</pre>",
            parse_mode="HTML"
        )


async def tg_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    payload = text.partition(" ")[2].strip()

    if not payload:
        await update.message.reply_text("Uso: /query <SELECT ...>")
        return

    if not query_is_safe(payload):
        await update.message.reply_text("Query inválida ou não permitida.")
        return

    cols, rows = await run_query(payload, ())
    out = format_results(cols, rows, max_rows=200)

    chunk = 3800
    for i in range(0, len(out), chunk):
        await update.message.reply_text(
            f"<pre>{html.escape(out[i:i+chunk])}</pre>",
            parse_mode="HTML"
        )


@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return "OK"


async def start_telegram_bot():
    global telegram_app

    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN não foi definido!")
        return

    telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()
    telegram_app.add_handler(CommandHandler("start", tg_start))
    telegram_app.add_handler(CommandHandler("table", tg_table))
    telegram_app.add_handler(CommandHandler("query", tg_query))

    if WEBHOOK_URL:
        webhook = f"{WEBHOOK_URL}/webhook"
        logger.info(f"Configurando Webhook: {webhook}")
        await telegram_app.bot.set_webhook(webhook)


@app.on_event("startup")
async def on_startup():
    await start_telegram_bot()


@app.on_event("shutdown")
async def on_shutdown():
    global telegram_app
    if telegram_app and telegram_app.running:
        await telegram_app.stop()
        telegram_app = None


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
