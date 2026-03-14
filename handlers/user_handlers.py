import logging
import asyncio
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils.dexscreener_utils import DexscreenerAPI
from formatters.message_formatter import format_token_message
from utils.security_utils import fetch_rugcheck, fetch_bundle_check
import config
import utils.db as db

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Admin channel notifications
# ─────────────────────────────────────────────

async def _notify_admin(bot, text: str):
    """Invia una notifica silenziosa al canale admin privato. No-op se ADMIN_CHANNEL_ID non configurato."""
    if not config.ADMIN_CHANNEL_ID:
        return
    try:
        await bot.send_message(
            chat_id=config.ADMIN_CHANNEL_ID,
            text=text,
            parse_mode='HTML',
            disable_notification=True,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.warning(f"Admin notify failed: {e}")


# Conversation states
WAITING_CA = 1
WAITING_SOCIAL = 2
WAITING_PAYMENT = 3

dexscreener_api = DexscreenerAPI()
solana_client = SolanaClient()

# ── Anti-spam / Rate limit ────────────────────────────────────────────────────
# Protegge le API key (Helius) dallo spam di CA da parte di utenti malintenzionati

_USER_RATE_LIMIT   = 5          # max N preview per utente ogni finestra
_RATE_WINDOW_SEC   = 300        # finestra di 5 minuti
_GLOBAL_MAX_CONC   = 3          # max richieste API contemporanee in tutto il bot
_CACHE_TTL_SEC     = 600        # cache risultati per 10 minuti

# {user_id: [timestamp, timestamp, ...]} — sliding window per utente
_user_preview_timestamps: dict[int, list] = {}

# Cache risultati: {mint: {'ts': float, 'token_info': ..., 'rc': ..., 'bundle': ..., 'socials': ...}}
_preview_cache: dict[str, dict] = {}

# Semaforo globale — limita chiamate API contemporanee
_api_semaphore = asyncio.Semaphore(_GLOBAL_MAX_CONC)


def _check_rate_limit(user_id: int) -> tuple[bool, int]:
    """
    Sliding window rate limit per utente.
    Ritorna (allowed: bool, wait_seconds: int).
    """
    now = time.time()
    timestamps = _user_preview_timestamps.get(user_id, [])
    # Rimuovi timestamp fuori dalla finestra
    timestamps = [t for t in timestamps if now - t < _RATE_WINDOW_SEC]
    if len(timestamps) >= _USER_RATE_LIMIT:
        wait = int(_RATE_WINDOW_SEC - (now - timestamps[0])) + 1
        _user_preview_timestamps[user_id] = timestamps
        return False, wait
    timestamps.append(now)
    _user_preview_timestamps[user_id] = timestamps
    return True, 0


def _get_cached_preview(mint: str) -> dict | None:
    """Ritorna risultati cached se ancora validi."""
    entry = _preview_cache.get(mint)
    if entry and (time.time() - entry['ts']) < _CACHE_TTL_SEC:
        return entry
    return None


def _set_cached_preview(mint: str, token_info, rc, bundle, socials):
    """Salva risultati in cache."""
    # Pulizia cache se troppo grande (max 200 entry)
    if len(_preview_cache) > 200:
        oldest = sorted(_preview_cache.items(), key=lambda x: x[1]['ts'])[:50]
        for k, _ in oldest:
            del _preview_cache[k]
    _preview_cache[mint] = {
        'ts': time.time(),
        'token_info': token_info,
        'rc': rc,
        'bundle': bundle,
        'socials': socials,
    }

# ── Piani promo ──────────────────────────────────────
PLAN_STANDARD  = 'standard'   # 0.6 SOL — 1 post
PLAN_BOOST     = 'boost'      # 0.8 SOL — 1 post + pin 24h
PLAN_PREMIUM   = 'premium'    # 1.5 SOL — 1 post + pin 24h + repost ogni ora 24h
PLAN_VIP       = 'vip'        # 4.0 SOL — 1 post + pin 72h + repost ogni ora 72h

PLAN_PRICES = {
    PLAN_STANDARD: 0.6,
    PLAN_BOOST:    0.8,
    PLAN_PREMIUM:  1.5,
    PLAN_VIP:      4.0,
}
PLAN_PIN_HOURS = {
    PLAN_STANDARD: 0,
    PLAN_BOOST:    24,
    PLAN_PREMIUM:  24,
    PLAN_VIP:      72,
}
PLAN_REPOST = {
    PLAN_STANDARD: False,
    PLAN_BOOST:    False,
    PLAN_PREMIUM:  True,
    PLAN_VIP:      True,
}
REPOST_INTERVAL_SEC = 3600  # repost ogni ora
import utils.db as db
db.init_db()
db.init_used_tx_table()
SESSION_TIMEOUT_SEC = 300  # 5 minuti di inattività → sessione scaduta


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Live Trending Tokens", callback_data="trending")],
        [InlineKeyboardButton("🚀 Promote My Token", callback_data="buytrending")],
        [InlineKeyboardButton("⚠️ Risk Disclaimer", callback_data="disclaimer")],
    ])


# ─────────────────────────────────────────────
# Commands (usati sia da /comando che da callback)
# ─────────────────────────────────────────────

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start — gestisce anche deep link ?start=buytrending e ?start=ref_<slug>"""
    args = context.args

    # Deep link referral: ?start=ref_<slug>
    if args and args[0].startswith('ref_'):
        slug = args[0][4:]  # rimuove prefisso "ref_"
        ref_data = db.get_referral_link(slug)
        if ref_data:
            # Salva il referral in sessione per tracciarlo sugli eventi futuri
            context.user_data['referral_slug'] = slug
            context.user_data['referral_label'] = ref_data['label']
        # Continua con il flusso buytrending
        await buytrending_command(update, context)
        return

    # Deep link dal bottone "Buy Trending" nel canale → apre il flusso buytrending
    if args and args[0] == 'buytrending':
        await buytrending_command(update, context)
        return

    text = (
        "👋 <b>Welcome to Pumpfun Early Trending!</b>\n\n"
        "Your real-time radar for early trending tokens on <b>Pump.fun</b>.\n\n"
        "📈 <b>Trending Tokens</b> — See live tokens being tracked right now, sorted by performance\n"
        "💰 <b>Promote Token</b> — Pay SOL to feature your token in the channel (optional pin + repost)\n"
        "⚠️ <b>Disclaimer</b> — Important risk info before trading\n\n"
        "Choose an option below:"
    )
    target = update.message if update.message else update.callback_query.message
    import os as _os
    img_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'assets', 'start.png')
    try:
        with open(img_path, 'rb') as _img:
            await target.reply_photo(photo=_img, caption=text, reply_markup=_main_menu_keyboard(), parse_mode='HTML')
    except Exception:
        await target.reply_text(text, reply_markup=_main_menu_keyboard(), parse_mode='HTML')


async def trending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /trending command — mostra lista token tracked stile hourly recap"""
    from datetime import datetime
    from formatters.message_formatter import format_hourly_recap

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="trending_refresh")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")],
    ])

    # Prendi il monitor dal bot_data (iniettato in main.py)
    monitor = context.bot_data.get("monitor")

    if monitor and monitor.tracked:
        # Usa i dati già in memoria dal gain check loop — no chiamate API aggiuntive
        # last_notified_multiplier rispecchia l'ultimo gain rilevato per ogni token
        results = []
        for mint, tracked in list(monitor.tracked.items()):
            try:
                if not tracked.initial_mc or tracked.initial_mc <= 0:
                    continue
                # Stima MC attuale: initial_mc × last_notified_multiplier
                # (aggiornato ogni 60s dal gain check loop)
                approx_mc = tracked.initial_mc * max(tracked.last_notified_multiplier, 1.0)
                multiplier = max(tracked.last_notified_multiplier, 1.0)
                results.append((tracked, approx_mc, multiplier))
            except Exception:
                continue

        if results:
            import time as _t
            now = _t.time()
            results_1h  = sorted([(t,mc,m) for t,mc,m in results if t.posted_at >= now-3600],  key=lambda x: x[2], reverse=True)
            results_3h  = sorted([(t,mc,m) for t,mc,m in results if t.posted_at >= now-10800], key=lambda x: x[2], reverse=True)
            results_24h = sorted(results, key=lambda x: x[2], reverse=True)
            message = format_hourly_recap(
                results_1h=results_1h,
                results_3h=results_3h,
                results_24h=results_24h,
                top_performers=monitor._top_performers if monitor else None,
            )
        else:
            message = (
                "📊 <b>Trending Tokens</b>\n\n"
                "⏳ No data yet — check back in a moment."
            )
    else:
        message = (
            "📊 <b>Trending Tokens</b>\n\n"
            "No tokens are being tracked yet.\n"
            "New tokens are posted to the channel automatically!"
        )

    if update.message:
        await update.message.reply_text(message, reply_markup=keyboard, parse_mode='HTML')
    else:
        await update.callback_query.message.reply_text(message, reply_markup=keyboard, parse_mode='HTML')


async def buytrending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /buytrending - Promote your token (paid)"""
    # Session check
    _state = context.user_data.get('conversation_state')
    _last = context.user_data.get('last_activity', 0)
    if _state and (time.time() - _last <= SESSION_TIMEOUT_SEC):
        _labels = {
            WAITING_CA: 'waiting for Contract Address',
            WAITING_SOCIAL: 'waiting for social links',
            WAITING_PAYMENT: 'waiting for payment',
        }
        _step = _labels.get(_state, 'in progress')
        _kb = InlineKeyboardMarkup([[
            InlineKeyboardButton('Continue', callback_data='session_continue'),
            InlineKeyboardButton('Cancel & Restart', callback_data='session_cancel'),
        ]])
        _target = update.message if update.message else update.callback_query.message
        await _target.reply_text(
            '<b>Active session detected</b>\n\n'
            'You already have a promotion session open (<i>' + _step + '</i>).\n\n'
            'Continue where you left off, or cancel and start fresh?',
            parse_mode='HTML',
            reply_markup=_kb,
        )
        return

    text = (
        "🎟 <b><u>LIMITED TIME OFFER!!!</u></b> 🎟\n\n"
        "🐳 Your token is <b><u>guaranteed between 8,000 - 64,000 Views</u></b> from active traders via:\n\n"
        "🤑 (1) Telegram Early Trending Entry Signal\n"
        "👀 (2) Bluesky and Nostr Trending Entry Signal\n"
        "📈 (12) Rank Posts\n"
        "📈 (0-60) Xs Posts\n"
        "🌍 (8) International Telegram Channels:\n"
        "　　🇨🇳 Chinese  🇷🇺 Russian  🇸🇦 Arabic\n"
        "　　🇻🇳 Vietnamese  🇰🇷 Korean  🇯🇵 Japanese\n"
        "　　🇮🇩 Indonesian  🇮🇳 Indian\n\n"
        "⚡ <i>Most devs report buys within the first 5min</i>"
    )
    context.user_data['conversation_state'] = WAITING_CA
    context.user_data['last_activity'] = time.time()

    # ── Admin notification: utente ha aperto il flusso buytrending ──────────
    user = update.effective_user
    username_str = f"@{user.username}" if user.username else "<i>no username</i>"
    import datetime as _dt
    now_str = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Referral tracking
    ref_slug = context.user_data.get('referral_slug')
    ref_label = context.user_data.get('referral_label', '')
    ref_line = f"\n🔗 <b>Ref:</b> <code>{ref_slug}</code> ({ref_label})" if ref_slug else ""
    if ref_slug:
        db.log_referral_event(slug=ref_slug, user_id=user.id, event_type='open')

    await _notify_admin(
        context.bot,
        f"👀 <b>New Buytrending Session</b>\n\n"
        f"👤 {username_str} (<code>{user.id}</code>)\n"
        f"📛 Name: {user.full_name or 'N/A'}\n"
        f"🕐 {now_str}"
        f"{ref_line}"
    )

    target = update.message if update.message else update.callback_query.message
    chat_id = target.chat_id
    import os as _os
    img_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'assets', 'buytrending.png')
    try:
        with open(img_path, 'rb') as _img:
            await context.bot.send_photo(chat_id=chat_id, photo=_img, caption=text, parse_mode='HTML')
    except Exception:
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
    await context.bot.send_message(
        chat_id=chat_id,
        text="⚙️ <b>Send your solana token address to start trending</b> 🔻",
        parse_mode='HTML'
    )


async def toptrending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /toptrending — mostra Top 10 token per performance"""
    from formatters.message_formatter import format_top10_post
    import os as _os

    monitor = context.bot_data.get("monitor")
    if not monitor or not monitor.tracked:
        await update.message.reply_text("⏳ No data yet — check back in a moment.")
        return

    results = []
    for mint, tracked in list(monitor.tracked.items()):
        try:
            if not tracked.initial_mc or tracked.initial_mc <= 0:
                continue
            multiplier = max(tracked.last_notified_multiplier, 1.0)
            approx_mc = tracked.initial_mc * multiplier
            results.append((tracked, approx_mc, multiplier))
        except Exception:
            continue

    message = format_top10_post(results)
    if not message:
        await update.message.reply_text("⏳ No data yet — check back in a moment.")
        return

    img_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'assets', 'top10.png')
    sent = False
    try:
        with open(img_path, 'rb') as _img:
            await update.message.reply_photo(photo=_img, caption=message, parse_mode='HTML')
            sent = True
    except Exception:
        pass
    if not sent:
        await update.message.reply_text(message, parse_mode='HTML', disable_web_page_preview=True)


async def disclaimer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle disclaimer button"""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_menu")],
    ])

    message = (
        "⚠️ <b>Disclaimer</b>\n\n"
        "All tokens listed by this bot are <b>not financial advice</b>.\n\n"
        "Crypto assets, especially newly launched tokens on Pump.fun, carry "
        "<b>extremely high risk</b> including total loss of funds.\n\n"
        "• Do your own research (DYOR) before investing\n"
        "• Never invest more than you can afford to lose\n"
        "• Past performance does not guarantee future results\n"
        "• Promoted tokens are paid listings — always verify independently\n\n"
        "<i>By using this bot you acknowledge these risks.</i>"
    )

    if update.message:
        await update.message.reply_text(message, reply_markup=keyboard, parse_mode='HTML')
    else:
        await update.callback_query.message.reply_text(message, reply_markup=keyboard, parse_mode='HTML')


# ─────────────────────────────────────────────
# Callback handler
# ─────────────────────────────────────────────

async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle menu button callbacks"""
    query = update.callback_query
    await query.answer()

    if query.data == "trending":
        await trending_command(update, context)
    elif query.data == "buytrending":
        await buytrending_command(update, context)
    elif query.data == "disclaimer":
        await disclaimer_command(update, context)
    elif query.data == "back_to_menu":
        await start_command(update, context)
    elif query.data == "trending_refresh":
        await trending_command(update, context)
    elif query.data.startswith("promote:"):
        await promote_button_callback(update, context)
    elif query.data.startswith("plan:"):
        await promo_plan_choice(update, context)
    elif query.data.startswith("plan_confirm:"):
        await promo_plan_confirm(update, context)
    elif query.data == "session_continue":
        _hints = {
            WAITING_CA: "Send your <b>Contract Address</b> to continue:",
            WAITING_SOCIAL: "Send your <b>social links</b> to continue:",
            WAITING_PAYMENT: "Send your <b>payment transaction hash</b> to continue:",
        }
        _cur = context.user_data.get("conversation_state")
        await query.answer("Session resumed")
        await query.message.reply_text(_hints.get(_cur, "Send your next input to continue:"), parse_mode="HTML")
    elif query.data == "session_cancel":
        context.user_data["conversation_state"] = None
        context.user_data["last_activity"] = 0
        await query.answer("Session cancelled")
        await query.message.reply_text("Session cancelled. Use /buytrending to start fresh.", parse_mode="HTML")


async def promote_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle promote button from channel"""
    query = update.callback_query
    await query.answer()

    mint = query.data.split(':')[1]
    context.user_data['current_mint'] = mint
    context.user_data['user_id'] = update.effective_user.id

    await context.bot.send_message(
        chat_id=update.effective_user.id,
        text=(
            f"🚀 <b>Promote This Token?</b>\n\n"
            f"CA: <code>{mint}</code>\n\n"
            f"Cost: <b>{config.PRICE_SOL} SOL</b>\n\n"
            f"Type your token's CA to continue:"
        ),
        parse_mode='HTML'
    )
    context.user_data['conversation_state'] = WAITING_CA


# ─────────────────────────────────────────────
# FIX: Unico handler testo con switch sullo stato
# ─────────────────────────────────────────────

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Unico handler per i messaggi di testo.
    Smista in base allo stato della conversazione.
    """
    state = context.user_data.get('conversation_state')

    if not state:
        return

    # ── Session timeout: 5 minuti di inattività ──────────
    last_activity = context.user_data.get('last_activity', 0)
    if time.time() - last_activity > SESSION_TIMEOUT_SEC:
        context.user_data['conversation_state'] = None
        context.user_data['last_activity'] = 0
        await update.message.reply_text(
            "⏱ <b>Session expired</b>\n\n"
            "You were inactive for 5 minutes. Use /start to begin again.",
            parse_mode='HTML'
        )
        return

    # Aggiorna timestamp attività
    context.user_data['last_activity'] = time.time()

    if state == WAITING_CA:
        await receive_ca_input(update, context)
    elif state == WAITING_SOCIAL:
        await receive_social_links(update, context)
    elif state == WAITING_PAYMENT:
        await receive_payment_hash(update, context)


async def receive_ca_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle CA input — con rate limit, cache e semaforo globale anti-spam."""
    ca = update.message.text.strip()
    user_id = update.effective_user.id

    if len(ca) < 40 or len(ca) > 50:
        await update.message.reply_text(
            "❌ Invalid address format. Solana addresses are 43-44 characters.",
            parse_mode='HTML'
        )
        return

    # ── Rate limit per utente ──
    allowed, wait_sec = _check_rate_limit(user_id)
    if not allowed:
        await update.message.reply_text(
            f"⏱ <b>Too many requests.</b>\n\n"
            f"Please wait <b>{wait_sec}s</b> before scanning another token.",
            parse_mode='HTML'
        )
        return

    # ── Cache check — evita rifetch per stessa CA ──
    cached = _get_cached_preview(ca)
    if cached:
        logger.info(f"📦 Cache hit for {ca[:8]} (user {user_id})")
        token_info  = cached['token_info']
        rc_preview  = cached['rc']
        bundle_preview = cached['bundle']
        # Aggiorna socials freschi dalla cache
        for _k in ('twitter', 'telegram', 'website', 'discord'):
            if cached['socials'].get(_k):
                token_info[_k] = cached['socials'][_k]
    else:
        # ── Semaforo globale — max 3 richieste API contemporanee ──
        async with _api_semaphore:
            token_info = await dexscreener_api.get_token_data(ca)

            if not token_info:
                await update.message.reply_text(
                    f"❌ Token not found: <code>{ca}</code>\n"
                    f"Make sure the address is correct.",
                    parse_mode='HTML'
                )
                return

            # Fetch in parallelo: pump.fun socials + rugcheck + bundle
            pump_socials, rc_preview, bundle_preview = await asyncio.gather(
                dexscreener_api.fetch_pump_socials(ca),
                fetch_rugcheck(ca),
                fetch_bundle_check(ca),
            )

            # Merge social pump.fun → token_info
            for _k in ('twitter', 'telegram', 'website', 'discord'):
                if pump_socials.get(_k):
                    token_info[_k] = pump_socials[_k]

            # Salva in cache
            _set_cached_preview(ca, token_info, rc_preview, bundle_preview, pump_socials)

    if not token_info:
        await update.message.reply_text(
            f"❌ Token not found: <code>{ca}</code>\n"
            f"Make sure the address is correct.",
            parse_mode='HTML'
        )
        return

    context.user_data['token_ca'] = ca
    context.user_data['token_info'] = token_info

    await update.message.reply_text(
        "✅ <b>Send your Telegram/X link</b> or type <code>skip</code> 🔻",
        parse_mode='HTML'
    )

    context.user_data['conversation_state'] = WAITING_SOCIAL


async def receive_social_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle social links"""
    social_input = update.message.text.strip()

    telegram_link = None
    twitter_link = None
    website = None
    discord = None

    if social_input.lower() != 'skip':
        parts = social_input.split()
        for p in parts:
            if 't.me/' in p:
                telegram_link = p
            elif 'twitter.com' in p.lower() or 'x.com' in p.lower():
                twitter_link = p
            elif 'discord' in p.lower():
                discord = p
            elif p.startswith('http') and not any(k in p.lower() for k in ['twitter', 'x.com', 't.me', 'discord']):
                website = p

    context.user_data['telegram_link'] = telegram_link
    context.user_data['twitter_link'] = twitter_link
    context.user_data['website'] = website
    context.user_data['discord'] = discord

    token_info = context.user_data.get('token_info', {})
    token = token_info.get('baseToken', {}) if token_info else {}
    symbol = token.get('symbol', '???')
    ca = context.user_data.get('token_ca', '')
    logo_url = token_info.get('logo') if token_info else None

    # ── Messaggio 1: logo + is ready to trend ──
    ready_text = (
        f"🚀 <b>${symbol} is Ready to Trend!</b>\n\n"
        f"Your token will be seen by thousands of degen traders actively hunting the next pump.\n\n"
        f"✅ Entry Signal post\n"
        f"✅ Automatic gain alerts as it pumps\n"
        f"✅ Cross-posted on Bluesky &amp; Nostr"
    )

    logo_sent = False
    if logo_url and logo_url.startswith('http'):
        try:
            import aiohttp as _aiohttp
            async with _aiohttp.ClientSession() as _session:
                async with _session.get(logo_url, timeout=_aiohttp.ClientTimeout(total=8)) as _resp:
                    if _resp.status == 200:
                        _logo_bytes = await _resp.read()
                        await update.message.reply_photo(
                            photo=_logo_bytes,
                            caption=ready_text,
                            parse_mode='HTML'
                        )
                        logo_sent = True
        except Exception as _e:
            logger.warning(f"Ready logo failed: {_e}")

    if not logo_sent:
        await update.message.reply_text(ready_text, parse_mode='HTML')

    # ── Messaggio 2: piani + bottoni 2 per riga ──
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Standard", callback_data=f"plan:{PLAN_STANDARD}"),
            InlineKeyboardButton("📌 Boost", callback_data=f"plan:{PLAN_BOOST}"),
        ],
        [
            InlineKeyboardButton("🔥 Premium", callback_data=f"plan:{PLAN_PREMIUM}"),
            InlineKeyboardButton("👑 VIP", callback_data=f"plan:{PLAN_VIP}"),
        ],
    ])

    plan_text = (
        f"🔽 <b>Choose your Plan</b>\n\n"
        f"🟢 <b>Standard</b> — <s>1.0 SOL</s> <b>0.6 SOL</b> (-40%)\n"
        f"Trending Post + Gain Reposts\n\n"
        f"📌 <b>Boost</b> — <s>1.25 SOL</s> <b>0.8 SOL</b> (-35%)\n"
        f"Trending Post + Pinned 24h + Gain Reposts\n\n"
        f"🔥 <b>Premium</b> — <s>2.15 SOL</s> <b>1.5 SOL</b> (-30%)\n"
        f"Trending Post + Pinned 24h + Repost every hour (24 total posts) + 1 Day visibility\n\n"
        f"👑 <b>VIP</b> — <s>5.35 SOL</s> <b>4.0 SOL</b> (-25%)\n"
        f"Trending Post + Pinned 72h + Repost every hour (72 total posts) + 3 Days visibility"
    )

    await update.message.reply_text(
        plan_text,
        reply_markup=keyboard,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


async def promo_plan_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle plan selection from inline buttons"""
    query = update.callback_query
    await query.answer()

    plan = query.data.split(':')[1]

    plan_labels = {
        PLAN_STANDARD: "🟢 Standard",
        PLAN_BOOST:    "📌 Boost",
        PLAN_PREMIUM:  "🔥 Premium Boost",
        PLAN_VIP:      "👑 VIP",
    }
    plan_notes = {
        PLAN_STANDARD: "Trending Post + Xs Gains Reposts",
        PLAN_BOOST:    "Trending Post + Pinned 24h + Xs Gains Reposts",
        PLAN_PREMIUM:  "Trending Post + Pinned 24h + Reposted every hour + 1 Day visibility + Xs Gains Reposts",
        PLAN_VIP:      "Trending Post + Pinned 72h + Reposted every hour + 3 Days visibility + Xs Gains Reposts",
    }

    # ── Check if user already selected a plan ──
    existing_plan = context.user_data.get('promo_plan')
    if existing_plan and existing_plan != plan and context.user_data.get('conversation_state') == WAITING_PAYMENT:
        existing_label = plan_labels[existing_plan]
        new_label = plan_labels[plan]
        new_price = PLAN_PRICES[plan]

        switch_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, switch", callback_data=f"plan_confirm:{plan}"),
                InlineKeyboardButton("❌ Keep current", callback_data=f"plan_confirm:{existing_plan}"),
            ]
        ])
        await query.message.reply_text(
            f"⚠️ You already selected <b>{existing_label}</b>.\n\n"
            f"Switch to <b>{new_label}</b> — <b>{new_price:.1f} SOL</b>?",
            parse_mode='HTML',
            reply_markup=switch_keyboard,
        )
        return

    await _send_payment_instructions(query.message, context, plan, plan_labels, plan_notes)


async def _send_payment_instructions(message, context, plan, plan_labels, plan_notes):
    """Send payment instructions for the selected plan"""
    total = PLAN_PRICES[plan]
    context.user_data['promo_plan'] = plan

    text = (
        f"💸 <b>Almost there!</b>\n\n"
        f"<b>{plan_labels[plan]}</b> — {total} SOL\n"
        f"<i>{plan_notes[plan]}</i>\n\n"
        f"Send exactly <b>{total:.2f} SOL</b> to:\n"
        f"<code>{config.PAYMENT_WALLET}</code>\n\n"
        f"Then paste your <b>Transaction Hash</b> here."
    )

    await message.reply_text(text, parse_mode='HTML')
    context.user_data['conversation_state'] = WAITING_PAYMENT
    context.user_data['payment_requested_at'] = time.time()


async def promo_plan_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle plan switch confirmation"""
    query = update.callback_query
    await query.answer()

    plan = query.data.split(':')[1]
    plan_labels = {
        PLAN_STANDARD: "🟢 Standard",
        PLAN_BOOST:    "📌 Boost",
        PLAN_PREMIUM:  "🔥 Premium Boost",
        PLAN_VIP:      "👑 VIP",
    }
    plan_notes = {
        PLAN_STANDARD: "Trending Post + Xs Gains Reposts",
        PLAN_BOOST:    "Trending Post + Pinned 24h + Xs Gains Reposts",
        PLAN_PREMIUM:  "Trending Post + Pinned 24h + Reposted every hour + 1 Day visibility + Xs Gains Reposts",
        PLAN_VIP:      "Trending Post + Pinned 72h + Reposted every hour + 3 Days visibility + Xs Gains Reposts",
    }
    await _send_payment_instructions(query.message, context, plan, plan_labels, plan_notes)


async def receive_payment_hash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle payment verification"""
    tx_hash = update.message.text.strip()

    await update.message.reply_text("⏳ Verifying payment...")

    plan = context.user_data.get('promo_plan', PLAN_STANDARD)
    expected_amount = PLAN_PRICES.get(plan, PLAN_PRICES[PLAN_STANDARD])
    pin_requested = plan in (PLAN_BOOST, PLAN_PREMIUM, PLAN_VIP)
    repost_requested = plan in (PLAN_PREMIUM, PLAN_VIP)

    # ── Session: verifica che siamo ancora in tempo ──────
    payment_requested_at = context.user_data.get('payment_requested_at', 0)
    if time.time() - payment_requested_at > SESSION_TIMEOUT_SEC:
        context.user_data['conversation_state'] = None
        await update.message.reply_text(
            "⏱ <b>Session expired</b>\n\n"
            "Your session timed out. Use /start to begin again.",
            parse_mode='HTML'
        )
        return

    # ── Anti-replay: tx hash già usato? ──────────────────
    if db.is_tx_used(tx_hash):
        await update.message.reply_text(
            "❌ This transaction has already been used.\n"
            "Please send a new payment.",
            parse_mode='HTML'
        )
        return

    # ── Test bypass per admin ──────────────────────────────
    TEST_USERS = {'fra_cr'}
    username = (update.effective_user.username or '').lower()
    is_valid = username in TEST_USERS  # bypass pagamento per test

    if not is_valid:
        is_valid = await solana_client.verify_transaction(
            tx_signature=tx_hash,
            expected_amount=expected_amount,
            to_wallet=config.PAYMENT_WALLET
        )

    if not is_valid:
        await update.message.reply_text(
            "❌ Payment verification failed.\n"
            "Check: wallet, amount, and that the transaction is confirmed.",
            parse_mode='HTML'
        )
        return

    if username in TEST_USERS:
        logger.info(f"🧪 Test bypass for @{username}")
    else:
        db.mark_tx_used(tx_hash)

    ca = context.user_data.get('token_ca')
    token_info = context.user_data.get('token_info')

    # ── Admin notification: pagamento ricevuto e verificato ─────────────────
    user = update.effective_user
    username_str = f"@{user.username}" if user.username else "<i>no username</i>"
    token_name   = token_info.get('baseToken', {}).get('name', 'N/A') if token_info else 'N/A'
    token_symbol = token_info.get('baseToken', {}).get('symbol', 'N/A') if token_info else 'N/A'
    token_mc     = token_info.get('marketCap', 0) if token_info else 0
    plan_labels_admin = {PLAN_STANDARD: "🟢 Standard", PLAN_BOOST: "📌 Boost", PLAN_PREMIUM: "🔥 Premium Boost", PLAN_VIP: "👑 VIP 3 Days"}
    plan_label = plan_labels_admin.get(plan, plan)
    import datetime as _dt
    now_str = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Referral tracking sul pagamento
    ref_slug  = context.user_data.get('referral_slug')
    ref_label = context.user_data.get('referral_label', '')
    ref_line  = f"\n🔗 <b>Ref:</b> <code>{ref_slug}</code> ({ref_label})" if ref_slug else ""
    if ref_slug:
        db.log_referral_event(
            slug=ref_slug, user_id=user.id, event_type='payment',
            plan=plan, amount_sol=expected_amount,
            token_ca=ca or '', token_symbol=token_symbol,
            tx_hash=tx_hash
        )

    admin_caption = (
        f"💰 <b>Payment Received!</b>\n\n"
        f"👤 {username_str} (<code>{user.id}</code>)\n"
        f"📛 Name: {user.full_name or 'N/A'}\n\n"
        f"🪙 <b>Token:</b> {token_name} (<code>{token_symbol}</code>)\n"
        f"<b>CA:</b> <code>{ca}</code>\n"
        f"💰 <b>MC:</b> ${token_mc:,.0f}\n\n"
        f"📦 <b>Plan:</b> {plan_label}\n"
        f"💸 <b>Amount:</b> {expected_amount:.2f} SOL\n"
        f"🔗 <b>TX:</b> <code>{tx_hash}</code>\n"
        f"{'🧪 <b>TEST BYPASS</b>' if username in TEST_USERS else ''}"
        f"{ref_line}\n"
        f"🕐 {now_str}"
    )

    await _notify_admin(context.bot, admin_caption)

    # Paid posts passano sempre — nessun blocco per CA già postata organicamente

    # Fetch bonding curve (stessi dati dei post organici)
    dex_api = DexscreenerAPI()
    try:
        extra = await asyncio.wait_for(dex_api.get_token_extra_data(ca), timeout=20.0)
    except Exception:
        extra = {'bonding_curve_pct': None}

    # Fetch diretto pump.fun — fonte primaria per social
    _pump_s = await dex_api.fetch_pump_socials(ca)
    for _k in ('twitter', 'telegram', 'website', 'discord'):
        if _pump_s.get(_k) and not token_info.get(_k):
            token_info[_k] = _pump_s[_k]

    # ── Rugcheck + Bundle ──
    rc_promo, bundle_promo = await asyncio.gather(
        fetch_rugcheck(ca),
        fetch_bundle_check(ca),
    )

    # Merge social: priorità a quelli inseriti dall'utente, fallback da token_info (pump.fun)
    _website  = context.user_data.get('website')       or token_info.get('website')
    _twitter  = context.user_data.get('twitter_link')  or token_info.get('twitter')
    _telegram = context.user_data.get('telegram_link') or token_info.get('telegram')
    _discord  = context.user_data.get('discord')       or token_info.get('discord')

    _symbol = token_info.get('baseToken', {}).get('symbol', '???')
    _name   = token_info.get('baseToken', {}).get('name', '')

    # Stesso formatter dei post organici
    message = await format_token_message(
        mint=ca,
        name=_name,
        symbol=_symbol,
        market_cap=token_info.get('marketCap', 0),
        price_usd=token_info.get('priceUsd'),
        liquidity=token_info.get('liquidity'),
        volume1h=token_info.get('volume1h'),
        buys1h=token_info.get('buys1h'),
        sells1h=token_info.get('sells1h'),
        txns1h=token_info.get('txns1h'),
        priceChange1h=token_info.get('priceChange1h'),
        pairCreatedAt=token_info.get('pairCreatedAt'),
        holders=token_info.get('holders'),
        logo_url=token_info.get('logo'),
        website=_website,
        twitter=_twitter,
        telegram=_telegram,
        discord=_discord,
        bonding_curve_pct=extra.get('bonding_curve_pct'),
        rugcheck_score=rc_promo.get('score'),
        rugcheck_label=rc_promo.get('label'),
        rugcheck_top_holder=rc_promo.get('top_holder'),
        rugcheck_top10=rc_promo.get('top10'),
        rugcheck_whales=rc_promo.get('whales'),
        bundle=bundle_promo,
    )

    try:
        logo_url = token_info.get('logo')
        sent_msg = None

        # Stessa keyboard dei post organici
        from tasks.token_monitor import _make_token_keyboard
        monitor = context.bot_data.get('monitor')
        if not monitor or not monitor._bot_username:
            bot_info = await context.bot.get_me()
            _bot_username = bot_info.username
            if monitor:
                monitor._bot_username = _bot_username
        else:
            _bot_username = monitor._bot_username
        promo_keyboard = _make_token_keyboard(ca, _bot_username)

        # Stesso logo handling dei post organici
        if monitor:
            try:
                logo_bytes = await monitor._normalize_logo(logo_url, _symbol)
                sent_msg = await context.bot.send_photo(
                    chat_id=config.CHANNEL_ID,
                    photo=logo_bytes,
                    caption=message,
                    reply_markup=promo_keyboard,
                    parse_mode='HTML'
                )
            except Exception as photo_err:
                logger.warning(f"Promo photo failed: {photo_err}")

        if not sent_msg:
            sent_msg = await context.bot.send_message(
                chat_id=config.CHANNEL_ID,
                text=message,
                reply_markup=promo_keyboard,
                parse_mode='HTML'
            )

        # ── Tracking gains per promoted token ─────────────
        if sent_msg:
            monitor = context.bot_data.get('monitor')
            if monitor:
                from tasks.token_monitor import TrackedToken
                import time as _t
                promo_mc = token_info.get('marketCap', 0)
                promo_tracked = TrackedToken(
                    mint=ca,
                    symbol=token_info.get('baseToken', {}).get('symbol', '???'),
                    name=token_info.get('baseToken', {}).get('name', ''),
                    logo_url=token_info.get('logo') or '',
                    initial_mc=promo_mc,
                    message_id=sent_msg.message_id,
                    posted_at=_t.time(),
                    original_caption=message,
                    plan=plan,  # VIP → 72h gain tracking, altri → 24h
                )
                monitor.tracked[ca] = promo_tracked
                monitor.posted_mints.add(ca)
                import utils.db as _db
                _db.add_posted_mint(ca)
                _db.save_tracked_token(promo_tracked)
                logger.info(f"📊 Promoted token {ca} added to gain tracking")

        # ── Multi-language broadcast ───────────────────────────────────────
        if sent_msg and monitor:
            _token_kwargs = dict(
                mint=ca,
                name=_name,
                symbol=_symbol,
                market_cap=token_info.get('marketCap', 0),
                price_usd=token_info.get('priceUsd'),
                liquidity=token_info.get('liquidity'),
                volume1h=token_info.get('volume1h'),
                buys1h=token_info.get('buys1h'),
                sells1h=token_info.get('sells1h'),
                txns1h=token_info.get('txns1h'),
                priceChange1h=token_info.get('priceChange1h'),
                pairCreatedAt=token_info.get('pairCreatedAt'),
                logo_url=token_info.get('logo'),
                website=_website,
                twitter=_twitter,
                telegram=_telegram,
                discord=_discord,
                rugcheck_score=rc_promo.get('score'),
                rugcheck_top_holder=rc_promo.get('top_holder'),
                rugcheck_top10=rc_promo.get('top10'),
                rugcheck_whales=rc_promo.get('whales'),
                bundle=bundle_promo,
            )
            
            logger.info(f"🌍 Promo broadcast scheduled for {_symbol} to i18n channels")

        # ── Pin ────────────────────────────────────────────
        if (pin_requested or repost_requested) and sent_msg:
            try:
                await context.bot.pin_chat_message(
                    chat_id=config.CHANNEL_ID,
                    message_id=sent_msg.message_id,
                    disable_notification=True
                )
                logger.info(f"📌 Promo pinned: msg_id={sent_msg.message_id}")
            except Exception as pin_err:
                logger.warning(f"Could not pin promo: {pin_err}")

        # ── Repost ogni ora (Premium/VIP) con dati live ───────
        if repost_requested and sent_msg:
            import json as _json
            monitor = context.bot_data.get('monitor')
            _user_data_json  = _json.dumps({
                'website':       context.user_data.get('website'),
                'twitter_link':  context.user_data.get('twitter_link'),
                'telegram_link': context.user_data.get('telegram_link'),
                'discord':       context.user_data.get('discord'),
            })
            _token_base_json = _json.dumps(token_info.get('baseToken', {}))
            pin_hours     = PLAN_PIN_HOURS[plan]
            reposts_total = pin_hours  # 1 repost per ora

            # Salva il job su DB — sopravvive ai restart
            job_id = db.create_promo_job(
                mint=ca,
                symbol=token_info.get('baseToken', {}).get('symbol', '???'),
                logo_url=token_info.get('logo') or '',
                plan=plan,
                channel_id=str(config.CHANNEL_ID),
                msg_id=sent_msg.message_id,
                reposts_total=reposts_total,
                repost_interval=REPOST_INTERVAL_SEC,
                user_data_json=_user_data_json,
                token_base_json=_token_base_json,
            )

            # Avvia subito il task (il monitor lo riprenderà dal DB in caso di restart)
            if monitor:
                job = db.load_active_promo_jobs()
                job = next((j for j in job if j['id'] == job_id), None)
                if job:
                    asyncio.create_task(monitor._run_promo_job(job))
                    logger.info(f"🔄 Promo job {job_id} started for {ca} ({plan}, {reposts_total} reposts)")
            else:
                logger.warning(f"⚠️ Monitor not available — promo job {job_id} will resume at next bot restart")

        elif pin_requested and sent_msg:
            # Boost: solo pin, unpin dopo 24h
            pin_hours = PLAN_PIN_HOURS[plan]
            async def unpin_after_delay(bot, chat_id, msg_id, hours):
                await asyncio.sleep(hours * 3600)
                try:
                    await bot.unpin_chat_message(chat_id=chat_id, message_id=msg_id)
                    logger.info(f"📌 Promo unpinned after {hours}h: msg_id={msg_id}")
                except Exception as e:
                    logger.warning(f"Could not unpin promo: {e}")
            asyncio.create_task(unpin_after_delay(
                context.bot, config.CHANNEL_ID, sent_msg.message_id, pin_hours
            ))

        plan_boost_notes = {
            PLAN_STANDARD: "",
            PLAN_BOOST:    "\n📌 Post pinned for 24 hours!",
            PLAN_PREMIUM:  "\n🔥 Post pinned + reposted every hour for 24h with live data!",
            PLAN_VIP:      "\n👑 Post pinned + reposted every hour for 3 days with live data!",
        }
        boost_note = plan_boost_notes.get(plan, "")

        _token_name = token_info.get("baseToken", {}).get("name", "Your token") if token_info else "Your token"
        await update.message.reply_text(
            f"🎉 <b><a href='https://pump.fun/{ca}'>{_token_name}</a> is now Trending!</b>\n\nThousands of traders can see your token right now. 👀{boost_note}",
            parse_mode="HTML",
            disable_web_page_preview=True
        )

        logger.info(f"✅ Token {ca} promoted (pin={pin_requested}, repost={repost_requested})")

    except Exception as e:
        logger.error(f"Error posting: {str(e)}")
        await update.message.reply_text("❌ Error posting. Contact support.")

    # Reset stato
    context.user_data['conversation_state'] = None
    context.user_data['promo_plan'] = None
