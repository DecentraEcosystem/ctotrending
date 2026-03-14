import os
import logging
import sys
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from telegram import BotCommand
import asyncio

from handlers.user_handlers import (
    start_command,
    trending_command,
    buytrending_command,
    disclaimer_command,
    toptrending_command,
    menu_callback,
    handle_text_input,
)
from handlers.admin_handlers import (
    genref_command,
    listrefs_command,
    delref_command,
)
from tasks.token_monitor import TokenMonitor
import utils.db as db

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 30))
TEST_CHANNEL_ID = os.getenv('TEST_CHANNEL_ID', '')  # canale test discovery sperimentale
TEST_DISCOVERY_INTERVAL = int(os.getenv('TEST_DISCOVERY_INTERVAL', 60))

def main():
    logger.info("🤖 Starting Pumptrend Bot...")
    logger.info(f"Channel ID: {CHANNEL_ID}")

    if not BOT_TOKEN or not CHANNEL_ID:
        logger.error("❌ Missing BOT_TOKEN or CHANNEL_ID!")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("trending", trending_command))
    app.add_handler(CommandHandler("buytrending", buytrending_command))
    app.add_handler(CommandHandler("disclaimer", disclaimer_command))
    app.add_handler(CommandHandler("toptrending", toptrending_command))

    # Admin commands
    app.add_handler(CommandHandler("genref", genref_command))
    app.add_handler(CommandHandler("listrefs", listrefs_command))
    app.add_handler(CommandHandler("delref", delref_command))

    # Callbacks
    app.add_handler(CallbackQueryHandler(menu_callback))

    # Text messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    # Token monitor
    token_monitor = TokenMonitor(app.bot, CHANNEL_ID)
    db.init_db()
    db.init_referral_table()

    # Teniamo traccia dei task di background per cancellarli allo shutdown
    _bg_tasks: list = []

    async def start_background_tasks(application):
        logger.info("🔄 Starting token monitor...")
        application.bot_data["monitor"] = token_monitor

        await application.bot.set_my_commands([
            BotCommand("start",        "🏠 Main menu"),
            BotCommand("trending",     "📈 Live trending tokens sorted by gain"),
            BotCommand("buytrending",  "🚀 Promote your token in the channel"),
            BotCommand("toptrending",  "🏆 Top 10 tokens by performance"),
            BotCommand("disclaimer",   "⚠️ Risk disclaimer & important info"),
        ])
        logger.info("✅ Bot commands menu set")

        # asyncio.create_task — l'event loop è già attivo qui dentro post_init
        task = asyncio.create_task(
            token_monitor.start_polling(POLLING_INTERVAL),
            name="token_monitor_main"
        )
        _bg_tasks.append(task)
        logger.info("✅ Token monitor task scheduled")

        # ── TestDiscovery: sistema sperimentale canale test ──────────────────
        # Avviato SOLO se TEST_CHANNEL_ID è impostato nelle env vars.
        # Gira in parallelo — non tocca il monitor principale.
        if TEST_CHANNEL_ID:
            _bg_tasks.append(test_task)
            logger.info(f"🧪 TestDiscovery started on channel {TEST_CHANNEL_ID} (interval={TEST_DISCOVERY_INTERVAL}s)")
        else:
            logger.info("🧪 TestDiscovery disabled (TEST_CHANNEL_ID not set)")

    async def stop_background_tasks(application):
        """Cancella tutti i task di background in modo pulito allo shutdown."""
        logger.info(f"🛑 Stopping {len(_bg_tasks)} background task(s)...")
        for task in _bg_tasks:
            if not task.done():
                task.cancel()
        if _bg_tasks:
            await asyncio.gather(*_bg_tasks, return_exceptions=True)
        logger.info("✅ Background tasks stopped cleanly")

    app.post_init = start_background_tasks
    app.post_shutdown = stop_background_tasks

    logger.info("✅ Bot polling started!")
    app.run_polling(allowed_updates=['message', 'callback_query'])

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
