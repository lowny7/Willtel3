import os
import logging
import asyncio
import psycopg2
import pandas as pd
import re
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from fastapi import FastAPI
import uvicorn
from threading import Thread

# Configuração
logging.basicConfig(level=logging.INFO)
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", 8000))

# Ajustar DATABASE_URL para Railway
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# FastAPI App
app = FastAPI(title="Telegram Investigator Bot")

@app.get("/")
async def root():
    return {"status": "Bot online", "service": "Telegram Investigator"}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Classe do Bot
class TelegramInvestigatorBot:
    def __init__(self, token):
        self.token = token
        self.application = Application.builder().token(token).build()
        self.setup_handlers()
    
    def get_db_connection(self):
        """Conexão com o banco"""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            logging.error(f"Erro DB: {e}")
            return None
    
    def setup_handlers(self):
        """Configura handlers do bot"""
        handlers = [
            CommandHandler("start", self.start),
            CommandHandler("search", self.search_user),
            CommandHandler("analyze", self.analyze_user),
            CommandHandler("phones", self.search_phones),
            CommandHandler("network", self.analyze_network),
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message),
            CallbackQueryHandler(self.button_handler)
        ]
        
        for handler in handlers:
            self.application.add_handler(handler)
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Comando /start"""
        user = update.effective_user
        await update.message.reply_text(f"""
👮 **BOT DE INVESTIGAÇÃO TELEGRAM**

Olá {user.first_name}! 

**Comandos:**
🔍 `/search @username` - Buscar usuário
📊 `/analyze ID` - Análise completa  
📞 `/phones ID` - Buscar telefones
👥 `/network ID` - Analisar rede

**Exemplos:**
`/search @username`
`/analyze 123456789`
`/phones 123456789` 
`/network 123456789`

💡 *Também funciona digitar @username diretamente*
        """, parse_mode='Markdown')
    
    async def search_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Buscar usuário por @username"""
        if not context.args:
            await update.message.reply_text("❌ Use: `/search @username`", parse_mode='Markdown')
            return
        
        username = context.args[0].replace('@', '')
        await update.message.reply_text(f"🔍 Buscando @{username}...")
        
        conn = self.get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Erro de conexão com o banco")
            return
        
        try:
            query = """
            SELECT id, username, first_name, last_name, phone 
            FROM users 
            WHERE username ILIKE %s
            LIMIT 10
            """
            df = pd.read_sql_query(query, conn, params=[username])
            conn.close()
            
            if df.empty:
                await update.message.reply_text(f"❌ Nenhum usuário encontrado para @{username}")
                return
            
            response = f"✅ **{len(df)} usuário(s) encontrado(s):**\n\n"
            
            for idx, row in df.iterrows():
                user_info = f"""
👤 **Usuário {idx+1}:**
🆔 ID: `{row['id']}`
📛 @{row['username']}
👤 Nome: {row['first_name']} {row['last_name'] or ''}
📞 Telefone: {row['phone'] or 'Não disponível'}
                """
                response += user_info + "\n" + "─" * 20 + "\n"
            
            # Botões de ação
            keyboard = []
            for _, row in df.iterrows():
                keyboard.append([
                    InlineKeyboardButton(
                        f"🔍 Analisar {row['username']}", 
                        callback_data=f"analyze_{row['id']}"
                    )
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                response, 
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            
        except Exception as e:
            conn.close()
            await update.message.reply_text(f"❌ Erro: {str(e)}")
    
    async def analyze_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Análise completa do usuário"""
        if not context.args:
            await update.message.reply_text("❌ Use: `/analyze ID`", parse_mode='Markdown')
            return
        
        user_id = context.args[0]
        await update.message.reply_text(f"📊 Analisando usuário {user_id}...")
        
        conn = self.get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Erro de conexão com o banco")
            return
        
        try:
            # Dados básicos
            user_query = "SELECT * FROM users WHERE id = %s"
            user_df = pd.read_sql_query(user_query, conn, params=[user_id])
            
            if user_df.empty:
                await update.message.reply_text("❌ Usuário não encontrado")
                conn.close()
                return
            
            user_data = user_df.iloc[0]
            
            # Estatísticas
            stats_query = """
            SELECT 
                COUNT(*) as total_messages,
                COUNT(DISTINCT chat_id) as total_chats,
                MIN(date) as first_message,
                MAX(date) as last_message
            FROM messages WHERE user_id = %s
            """
            stats_df = pd.read_sql_query(stats_query, conn, params=[user_id])
            stats = stats_df.iloc[0] if not stats_df.empty else {}
            
            conn.close()
            
            # Montar resposta
            response = f"""
🎯 **RELATÓRIO DE INVESTIGAÇÃO**

👤 **DADOS BÁSICOS:**
🆔 ID: `{user_data['id']}`
📛 Username: @{user_data['username']}
👤 Nome: {user_data['first_name']} {user_data['last_name'] or ''}
📞 Telefone: {user_data['phone'] or 'Não disponível'}

📊 **ATIVIDADE:**
💬 Total de mensagens: {stats.get('total_messages', 0)}
👥 Grupos ativos: {stats.get('total_chats', 0)}
📅 Primeira mensagem: {stats.get('first_message', 'N/A')}
🕒 Última mensagem: {stats.get('last_message', 'N/A')}
            """
            
            # Botões
            keyboard = [
                [InlineKeyboardButton("📞 Buscar Telefones", callback_data=f"phones_{user_id}")],
                [InlineKeyboardButton("👥 Analisar Rede", callback_data=f"network_{user_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                response, 
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            
        except Exception as e:
            conn.close()
            await update.message.reply_text(f"❌ Erro: {str(e)}")
    
    async def search_phones(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Buscar padrões de telefone"""
        if not context.args:
            await update.message.reply_text("❌ Use: `/phones ID`", parse_mode='Markdown')
            return
        
        user_id = context.args[0]
        await update.message.reply_text(f"📞 Buscando telefones...")
        
        conn = self.get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Erro de conexão com o banco")
            return
        
        try:
            query = """
            SELECT m.text, m.date, c.title as chat_title
            FROM messages m
            JOIN chats c ON m.chat_id = c.id
            WHERE m.user_id = %s AND (
                m.text ~ '\+?[0-9]{10,15}'
            )
            ORDER BY m.date DESC
            LIMIT 15
            """
            
            df = pd.read_sql_query(query, conn, params=[user_id])
            conn.close()
            
            if df.empty:
                await update.message.reply_text("❌ Nenhum telefone encontrado")
                return
            
            response = f"📞 **{len(df)} PADRÕES ENCONTRADOS:**\n\n"
            
            for idx, row in df.iterrows():
                phones = re.findall(r'(\+?[0-9]{10,15})', row['text'])
                if phones:
                    phone_info = f"**{idx+1}.** 📱 {phones[0]}\n💬 {row['text'][:60]}...\n📁 {row['chat_title']}\n\n"
                    response += phone_info
            
            await update.message.reply_text(response, parse_mode='Markdown')
            
        except Exception as e:
            conn.close()
            await update.message.reply_text(f"❌ Erro: {str(e)}")
    
    async def analyze_network(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Analisar rede social"""
        if not context.args:
            await update.message.reply_text("❌ Use: `/network ID`", parse_mode='Markdown')
            return
        
        user_id = context.args[0]
        await update.message.reply_text(f"👥 Analisando rede...")
        
        conn = self.get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Erro de conexão com o banco")
            return
        
        try:
            query = """
            SELECT DISTINCT u.username, u.first_name, u.last_name,
                   COUNT(*) as msg_count
            FROM messages m
            JOIN users u ON m.user_id = u.id  
            WHERE m.chat_id IN (
                SELECT DISTINCT chat_id FROM messages WHERE user_id = %s
            )
            AND m.user_id != %s
            GROUP BY u.id, u.username, u.first_name, u.last_name
            HAVING COUNT(*) > 2
            ORDER BY msg_count DESC
            LIMIT 10
            """
            
            df = pd.read_sql_query(query, conn, params=[user_id, user_id])
            conn.close()
            
            if df.empty:
                await update.message.reply_text("❌ Nenhuma conexão encontrada")
                return
            
            response = f"👥 **REDE SOCIAL - {len(df)} CONEXÕES:**\n\n"
            
            for idx, row in df.iterrows():
                response += f"**{idx+1}.** @{row['username']} - {row['msg_count']} msgs\n"
            
            await update.message.reply_text(response, parse_mode='Markdown')
            
        except Exception as e:
            conn.close()
            await update.message.reply_text(f"❌ Erro: {str(e)}")
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Lida com mensagens contendo @username"""
        text = update.message.text
        usernames = re.findall(r'@(\w+)', text)
        
        if usernames:
            for username in usernames[:2]:
                await self.search_user_by_mention(update, username)
        else:
            await update.message.reply_text("💡 Digite @username ou use /search")
    
    async def search_user_by_mention(self, update: Update, username: str):
        """Busca rápida por menção"""
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            query = "SELECT id, username FROM users WHERE username ILIKE %s LIMIT 3"
            df = pd.read_sql_query(query, conn, params=[username])
            conn.close()
            
            if not df.empty:
                response = f"🔍 **@{username} encontrado:**\n\n"
                for _, row in df.iterrows():
                    response += f"👤 @{row['username']}\n🆔 `{row['id']}`\n📊 `/analyze {row['id']}`\n\n"
                await update.message.reply_text(response, parse_mode='Markdown')
        except Exception as e:
            conn.close()
    
    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler de botões inline"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data.startswith('analyze_'):
            user_id = data.split('_')[1]
            fake_update = type('', (), {})()
            fake_update.message = query.message
            fake_context = type('', (), {'args': [user_id]})()
            await self.analyze_user(fake_update, fake_context)
        
        elif data.startswith('phones_'):
            user_id = data.split('_')[1]
            fake_update = type('', (), {})()
            fake_update.message = query.message
            fake_context = type('', (), {'args': [user_id]})()
            await self.search_phones(fake_update, fake_context)
        
        elif data.startswith('network_'):
            user_id = data.split('_')[1]
            fake_update = type('', (), {})()
            fake_update.message = query.message
            fake_context = type('', (), {'args': [user_id]})()
            await self.analyze_network(fake_update, fake_context)
    
    def run(self):
        """Inicia o bot"""
        self.application.run_polling()

# Global bot instance
bot = None

def start_bot():
    """Inicia o bot em thread separada"""
    global bot
    try:
        bot = TelegramInvestigatorBot(BOT_TOKEN)
        logging.info("🤖 Bot iniciado no Railway")
        bot.run()
    except Exception as e:
        logging.error(f"Erro no bot: {e}")

@app.on_event("startup")
async def startup_event():
    """Inicia o bot quando o servidor inicia"""
    if BOT_TOKEN:
        Thread(target=start_bot, daemon=True).start()
        logging.info("🚀 Servidor + Bot iniciados")
    else:
        logging.warning("⚠️ BOT_TOKEN não configurado")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
