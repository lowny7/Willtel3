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
from telegram import __version__ as PTB_VERSION  # for info
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

# Configurações via env
DEFAULT_DB_URL = "postgresql://postgres:docker@localhost/telegram"
DATABASE_URL = os.getenv("postgresql://postgres:docker@localhost/telegram", DEFAULT_DB_URL)
TELEGRAM_TOKEN = os.getenv("6158712854:AAF47WpjOgNWwWkthOgRNkKcs3mdoblCWTA")
HTTP_API_KEY = os.getenv("HTTP_API_KEY")  # se setada, protege endpoints HTTP
TELEGRAM_ALLOWED = os.getenv("6423632852")  # ex: "12345678,987654321"
if TELEGRAM_ALLOWED:
    try:
        ALLOWED_CHAT_IDS = set(int(x.strip()) for x in TELEGRAM_ALLOWED.split(",") if x.strip())
    except Exception:
        ALLOWED_CHAT_IDS = set()
else:
    ALLOWED_CHAT_IDS = None  # None => não usa whitelist

PORT = int(os.getenv("PORT", "8000"))

# validações
VALID_TABLE_RE = re.compile(r"^[A-Za-z0-9_]{1,63}$")
MAX_QUERY_LENGTH = 3000
FORBIDDEN_WORDS = [";","drop ","delete ","update ","insert ","truncate ","alter ", "create "]

app = FastAPI(title="Telegram+FastAPI PG (pg8000)")

def parse_database_url(url: str):
    p = urlparse(url)
    if p.scheme not in ("postgres", "postgresql"):
        raise ValueError("Espera URL com esquema postgresql://")
    user = p.username or "postgres"
    password = p.password or ""
    host = p.hostname or "localhost"
    port = p.port or 5432
    dbname = p.path.lstrip("/") or "postgres"
    return dict(user=user, password=password, host=host, port=port, database=dbname)

DB_PARAMS = parse_database_url(DATABASE_URL)
logger.info("DB params: host=%s port=%s db=%s user=%s", DB_PARAMS["host"], DB_PARAMS["port"], DB_PARAMS["database"], DB_PARAMS["user"])

# -------------------------
# Funções de acesso ao DB
# -------------------------
def _sync_run_query(sql: str, params: tuple = ()):
    """
    Função síncrona que executa a query usando pg8000.
    Será chamada via asyncio.to_thread para não bloquear o loop.
    Retorna (columns, rows)
    """
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
    count = 0
    for row in rows:
        if count >= max_rows:
            lines.append(f"... {len(rows) - max_rows} linhas omitidas ...")
            break
        safe = []
        for v in row:
            if isinstance(v, (bytes, bytearray)):
                try:
                    safe.append(v.decode("utf-8", errors="replace"))
                except Exception:
                    safe.append(str(v))
            elif v is None:
                safe.append("NULL")
            else:
                safe.append(str(v))
        lines.append(" | ".join(safe))
        count += 1
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

# -------------------------
# FastAPI Schemas & Endpoints
# -------------------------
class QueryIn(BaseModel):
    sql: str

async def require_api_key(x_api_key: Optional[str] = Header(None)):
    # Se HTTP_API_KEY não definido => aberto
    if HTTP_API_KEY:
        if x_api_key != HTTP_API_KEY:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key inválida")

@app.get("/health")
async def health():
    return {"status": "ok", "ptb_version": PTB_VERSION}

@app.get("/table/{table_name}")
async def http_table(table_name: str, limit: int = 20, api_key: Optional[str] = Header(None)):
    await require_api_key(api_key)
    if not VALID_TABLE_RE.match(table_name):
        raise HTTPException(status_code=400, detail="Nome de tabela inválido.")
    limit = max(1, min(1000, int(limit)))
    sql = f'SELECT * FROM "{table_name}" LIMIT %s'
    try:
        cols, rows = await run_query(sql, (limit,))
    except Exception as e:
        logger.exception("Erro DB /table")
        raise HTTPException(status_code=500, detail=str(e))
    return {"columns": cols, "rows_count": len(rows), "rows_preview": [list(r) for r in rows[:limit]]}

@app.post("/query")
async def http_query(payload: QueryIn, api_key: Optional[str] = Header(None)):
    await require_api_key(api_key)
    sql = payload.sql.strip()
    if not query_is_safe(sql):
        raise HTTPException(status_code=400, detail="Query inválida ou não permitida.")
    try:
        cols, rows = await run_query(sql, ())
    except Exception as e:
        logger.exception("Erro DB /query")
        raise HTTPException(status_code=500, detail=str(e))
    return {"columns": cols, "rows_count": len(rows), "rows_preview": [list(r) for r in rows[:200]]}

# -------------------------
# Telegram Bot
# -------------------------
telegram_app: Optional[Application] = None

async def is_chat_allowed(chat_id: int) -> bool:
    if ALLOWED_CHAT_IDS is None:
        return True
    return chat_id in ALLOWED_CHAT_IDS

# Handlers
async def tg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not await is_chat_allowed(chat_id):
        await update.message.reply_text("Acesso negado.")
        return
    await update.message.reply_text(
        "Bot ativo. Use /table <nome> [limite] ou /query <SELECT ...> (apenas SELECT)."
    )

async def tg_table(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not await is_chat_allowed(chat_id):
        await update.message.reply_text("Acesso negado.")
        return
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
    try:
        cols, rows = await run_query(sql, (limit,))
    except Exception as e:
        logger.exception("Erro DB (tg_table)")
        await update.message.reply_text(f"Erro ao consultar: {e}")
        return
    out = format_results(cols, rows, max_rows=limit)
    # enviar em chunks respeitando limite do Telegram (~4096)
    chunk_size = 3800
    for i in range(0, len(out), chunk_size):
        await update.message.reply_text(f"<pre>{html.escape(out[i:i+chunk_size])}</pre>", parse_mode="HTML")

async def tg_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not await is_chat_allowed(chat_id):
        await update.message.reply_text("Acesso negado.")
        return
    text = update.message.text or ""
    payload = text.partition(" ")[2].strip()
    if not payload:
        await update.message.reply_text("Uso: /query <SELECT ...>")
        return
    if not query_is_safe(payload):
        await update.message.reply_text("Query inválida ou não permitida.")
        return
    try:
        cols, rows = await run_query(payload, ())
    except Exception as e:
        logger.exception("Erro DB (tg_query)")
        await update.message.reply_text(f"Erro ao executar SELECT: {e}")
        return
    out = format_results(cols, rows, max_rows=200)
    chunk_size = 3800
    for i in range(0, len(out), chunk_size):
        await update.message.reply_text(f"<pre>{html.escape(out[i:i+chunk_size])}</pre>", parse_mode="HTML")

async def start_telegram_bot():
    global telegram_app
    if not TELEGRAM_TOKEN:
        logger.info("TELEGRAM_TOKEN não definido, bot Telegram será desativado.")
        return
    telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()
    telegram_app.add_handler(CommandHandler("start", tg_start))
    telegram_app.add_handler(CommandHandler("table", tg_table))
    telegram_app.add_handler(CommandHandler("query", tg_query))

    # run_polling é um coroutine que mantém o bot em polling; executamos como tarefa de fundo
    logger.info("Iniciando Telegram bot (polling)...")
    # start polling in background and don't await here (FastAPI server continua)
    asyncio.create_task(telegram_app.run_polling())

# Startup event: iniciar bot
@app.on_event("startup")
async def on_startup():
    logger.info("Aplicação iniciando. Versão PTB: %s", PTB_VERSION)
    # iniciar bot em background (se token disponível)
    await start_telegram_bot()

# Shutdown: parar o bot cleanly se estiver rodando
@app.on_event("shutdown")
async def on_shutdown():
    global telegram_app
    if telegram_app:
        logger.info("Encerrando Telegram bot...")
        await telegram_app.stop()
        telegram_app = None

# -------------------------
# CLI: permite executar com python main.py (usa uvicorn programaticamente)
# -------------------------
if __name__ == "__main__":
    import uvicorn
    logger.info("Executando uvicorn app on 0.0.0.0:%s", PORT)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
