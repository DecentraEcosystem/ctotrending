import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = int(os.getenv('CHANNEL_ID'))
CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME', '')
ADMIN_CHANNEL_ID = int(os.getenv('ADMIN_CHANNEL_ID', 0))

# Solana & Payments
PAYMENT_WALLET = os.getenv('PAYMENT_WALLET')
SOLANA_RPC_URL = os.getenv('SOLANA_RPC_URL')
HELIUS_API_KEY = os.getenv('HELIUS_API_KEY')
PRICE_SOL = float(os.getenv('PRICE_SOL', 0.5))

# Pump.fun (usato solo per link nei messaggi)
PUMP_PROGRAM_ID = os.getenv('PUMP_PROGRAM_ID', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')

# CTO Bot — nessun filtro MC/età: posta TUTTI i CTO Solana
# POLLING_INTERVAL: quanto spesso controlla l'endpoint DexScreener CTO (secondi)
WHALE_MIN_SOL = float(os.getenv('WHALE_MIN_SOL', 5.0))
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 120))

# Dummy values mantenuti per compatibilità con moduli condivisi (non usati per filtrare)
MIN_MARKET_CAP = 0
MAX_MARKET_CAP = 999_999_999
TOKEN_AGE_HOURS = 9999
STREAK_MIN = int(os.getenv('STREAK_MIN', 2))
STREAK_MULTIPLIER = float(os.getenv('STREAK_MULTIPLIER', 2.0))

# Owner DM alerts
OWNER_TELEGRAM_ID = int(os.getenv('OWNER_TELEGRAM_ID', 0))



