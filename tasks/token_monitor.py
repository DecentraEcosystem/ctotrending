import asyncio
import json
import logging
import os
import time
import io
from dataclasses import dataclass, field
from typing import Dict, Optional, Set
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

import aiohttp
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

from utils.dexscreener_utils import DexscreenerAPI
from formatters.message_formatter import format_token_message, format_gain_alert, format_whale_alert, format_top10_post
import utils.db as db
import config

LOGO_SIZE = 512  # dimensione fissa per tutti i logo (512x512 HD)

logger = logging.getLogger(__name__)

# Milestone in ordine: percentuali prima, poi moltiplicatori interi infiniti
# +25%, +50%, 2x, 3x, 4x, 5x, 6x, 7x, 8x, ... (continua all'infinito)
INITIAL_MILESTONES = [1.5]  # +50% milestone, poi 2x, 3x, 4x... all'infinito
MULTIPLIER_START = 2        # parte da 2x, poi 3x, 4x... all'infinito

GMGN_REF = "i_bsVrj7Mc_c"

def _make_token_keyboard(mint: str, bot_username: str) -> InlineKeyboardMarkup:
    """Keyboard standard per tutti i post token: GMGN + Axiom | Buy Trending."""
    buy_link  = f"https://t.me/{bot_username}?start=buytrending"
    gmgn_link = f"https://t.me/GMGN_sol_bot?start={GMGN_REF}_{mint}"
    axiom_link = f"https://axiom.trade/t/{mint}/@inscribe?chain=sol"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🤖 GMGN",    url=gmgn_link),
            InlineKeyboardButton("⚡️ Axiom",   url=axiom_link),
        ],
        [InlineKeyboardButton("🚀 Buy Trending", url=buy_link)],
    ])

def _compute_alpha_score(volume1h, buys1h, sells1h, priceChange1h, mint: str = "") -> int:
    """
    Score 0-100 basato su:
    - Volume 1h:       max 40 punti ($10k = pieno)
    - Buy pressure:    max 30 punti (100% buys = pieno)
    - Price change 1h: max 30 punti (+100% = pieno)
    + jitter deterministico ±10 basato sull'hash del mint
    """
    import hashlib
    volume    = float(volume1h or 0)
    buys      = float(buys1h or 0)
    sells     = float(sells1h or 0)
    change    = float(priceChange1h or 0)

    vol_score      = min(volume / 10000 * 40, 40)
    total_txns     = buys + sells
    pressure_score = (buys / total_txns * 30) if total_txns > 0 else 15
    change_score   = min(max(change, 0) / 100 * 30, 30)

    base = vol_score + pressure_score + change_score
    # Jitter deterministico ±10 basato sull'hash del mint — ogni token ha il suo score unico
    if mint:
        jitter = int(hashlib.md5(mint.encode()).hexdigest()[:2], 16) % 21 - 10  # -10 a +10
    else:
        jitter = 0

    return max(0, min(100, round(base + jitter)))


def _next_milestone(current_multiplier: float, last_notified: float) -> Optional[float]:
    """
    Ritorna il prossimo milestone da notificare dato l'ultimo notificato.
    Sequenza: 1.5 (+50%) → 2x → 3x → 4x → 5x → 6x → ... (infinito)
    """
    all_fixed = INITIAL_MILESTONES  # [1.25, 1.50]
    # Calcola fino a quale intero dobbiamo prevedere
    for m in all_fixed:
        if last_notified < m <= current_multiplier:
            return m
    # Parte intera: prossimo intero dopo last_notified, >= MULTIPLIER_START
    next_int = max(MULTIPLIER_START, int(last_notified) + 1)
    if current_multiplier >= next_int:
        return float(next_int)
    return None


@dataclass
class TrackedToken:
    mint: str
    symbol: str
    initial_mc: float
    message_id: int
    posted_at: float
    name: str = ""                         # nome completo del token (es. "Pippkin The Horse")
    logo_url: str = ""                     # URL logo originale per i gain alert
    last_notified_multiplier: float = 1.0  # parte da 1x (nessuna notifica)
    last_volume1h: float = 0.0             # snapshot volume per buy volume alert
    volume_alert_sent: bool = False        # evita alert ripetuti
    original_caption: str = ""            # caption originale del post, per editarla con i gains
    plan: str = ""                         # piano promo ('vip', 'premium', ecc.) o '' per organici
    dex_updated: bool = False              # True se il dex profile update alert è già stato postato
    dex_boost_posted: bool = False         # True se il dex boost alert è già stato postato
    dex_ads_posted: bool = False           # True se il dex ads alert è già stato postato
    dex_cto_posted: bool = False           # True se il cto alert è già stato postato
    peak_multiplier: float = 1.0
    last_gain_alert_at: float = 0.0      # timestamp ultimo gain alert postato (anti-spam)
    pending_peak_milestone: float = 0.0  # milestone più alto raggiunto durante il cooldown



class TokenMonitor:

    def __init__(self, bot: Bot, channel_id: str):
        self.bot = bot
        self.channel_id = channel_id
        self.dexscreener_api = DexscreenerAPI()

        # Inizializza DB SQLite persistente
        db.init_db()

        # Carica dati dal DB
        self.posted_mints: Set[str] = db.load_posted_mints()
        self.tracked: Dict[str, TrackedToken] = self._load_tracked()

        # Sincronizza: ogni mint in tracked deve essere in posted_mints
        for mint in self.tracked:
            if mint not in self.posted_mints:
                self.posted_mints.add(mint)
                db.add_posted_mint(mint)

        self._last_recap_message_id: Optional[int] = None
        self._last_top10_message_id: Optional[int] = None
        self._store_message_id: Optional[int] = None
        self._bot_username: Optional[str] = None
        self._posting_lock: asyncio.Lock = asyncio.Lock()
        self._gain_check_lock: asyncio.Lock = asyncio.Lock()
        self._top_performers: list = db.load_top_performers()
        self._streak: list = db.load_streak()
        # Set in-memory dei gain già in corso di invio: (mint, milestone)
        # Previene duplicati causati da await lunghi (download logo, send_photo)
        # che cedono il controllo prima che il DB sia aggiornato
        # Lock per-mint: un asyncio.Lock per token, serializza tutti i gain
        # dello stesso token anche tra tick diversi nella stessa istanza
        self._gain_mint_locks: Dict[str, asyncio.Lock] = {}
        self._pending_gains: Dict[str, tuple] = {}  # mint → (milestone, timestamp)
        self._known_boosts: list = []                # cache ultima risposta boost API
        self._whale_seen_txs: Set[str] = set()          # dedup whale alert per signature/bucket
        logger.info(f"📂 Loaded {len(self.posted_mints)} previously posted mints from DB")

        self._i18n_channels: dict = {}  # multilanguage disabled

    def _load_tracked(self) -> Dict[str, 'TrackedToken']:
        """Carica i token trackati dal DB SQLite persistente"""
        rows = db.load_tracked_tokens()
        tracked = {}
        for d in rows:
            t = TrackedToken(
                mint=d['mint'],
                symbol=d['symbol'],
                name=d.get('name', ''),
                logo_url=d.get('logo_url', ''),
                initial_mc=d['initial_mc'],
                message_id=d['message_id'],
                posted_at=d['posted_at'],
                last_notified_multiplier=d.get('last_notified_multiplier', 1.0),
                last_volume1h=d.get('last_volume1h', 0.0),
                volume_alert_sent=d.get('volume_alert_sent', False),
                original_caption=d.get('original_caption', ''),
                plan=d.get('plan', ''),
                dex_updated=d.get('dex_updated', False),
                dex_boost_posted=d.get('dex_boost_posted', False),
                dex_ads_posted=d.get('dex_ads_posted', False),
                dex_cto_posted=d.get('dex_cto_posted', False),
                peak_multiplier=d.get('peak_multiplier', 1.0),
            )
            tracked[d['mint']] = t
        logger.info(f"📂 Loaded {len(tracked)} tracked tokens from DB")
        return tracked

    def _save_tracked(self):
        """Salva tutti i token trackati nel DB"""
        for t in self.tracked.values():
            db.save_tracked_token(t)

    def _update_top_performers(self, mint: str, symbol: str, multiplier: float, current_mc: float):
        """Aggiorna il top performer nel DB e aggiorna la cache in memoria"""
        db.upsert_top_performer(mint, symbol, multiplier, current_mc)
        for entry in self._top_performers:
            if entry['mint'] == mint:
                if multiplier > entry['multiplier']:
                    entry['multiplier'] = multiplier
                    entry['current_mc'] = current_mc
                    entry['updated_at'] = time.time()
                return
        self._top_performers.append({
            'mint': mint, 'symbol': symbol,
            'multiplier': multiplier, 'current_mc': current_mc,
            'updated_at': time.time(),
        })
        self._top_performers.sort(key=lambda x: x['multiplier'], reverse=True)
        self._top_performers = self._top_performers[:100]

    async def start_polling(self, interval: int):
        logger.info(f"✅ CTObot monitor started with {interval}s interval")
        await self._recover_posted_mints_from_channel()
        asyncio.create_task(self._gain_check_loop())
        asyncio.create_task(self._top10_loop())
        asyncio.create_task(self._boost_check_loop())
        asyncio.create_task(self._ads_check_loop())
        asyncio.create_task(self._cto_check_loop())
        asyncio.create_task(self._resume_promo_jobs())

        # ── Nessun Helius WebSocket — la discovery avviene tramite DexScreener CTO endpoint ──
        logger.info("🤝 CTObot: discovery via DexScreener community-takeovers endpoint")

        while True:
            try:
                await self.check_trending_tokens()
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"❌ CTO monitor error: {str(e)}")
                await asyncio.sleep(interval)

    async def _resume_promo_jobs(self):
        """
        Al boot, riprende tutti i job di repost attivi salvati nel DB.
        Per ogni job calcola quando dovrebbe avvenire il prossimo repost
        e aspetta esattamente quel tempo — come se non ci fosse mai stato un restart.
        """
        jobs = db.load_active_promo_jobs()
        if not jobs:
            logger.info("✅ No pending promo jobs to resume")
            return

        logger.info(f"♻️ Resuming {len(jobs)} pending promo job(s) after restart")
        for job in jobs:
            remaining = job['reposts_total'] - job['reposts_done']
            logger.info(
                f"  ↩️ {job['symbol']} | plan={job['plan']} | "
                f"done={job['reposts_done']}/{job['reposts_total']} | "
                f"next_repost_at={job['next_repost_at']:.0f}"
            )
            asyncio.create_task(self._run_promo_job(job))

    async def _run_promo_job(self, job: dict):
        """
        Esegue un job di repost (nuovo o ripreso dal DB).
        Ogni iterazione:
        1. Aspetta fino a next_repost_at (gestisce correttamente i restart)
        2. Unpinna il messaggio precedente
        3. Posta il nuovo messaggio con dati live
        4. Pinna il nuovo messaggio
        5. Aggiorna il DB con il nuovo msg_id e incrementa reposts_done
        Al termine (o se il job viene completato) marca come 'completed' e unpinna.
        """
        import json as _json
        from formatters.message_formatter import format_promo_message
        from utils.dexscreener_utils import DexscreenerAPI
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        job_id       = job['id']
        mint         = job['mint']
        symbol       = job['symbol']
        logo_url     = job['logo_url']
        channel_id   = int(job['channel_id'])
        reposts_done = job['reposts_done']
        reposts_total = job['reposts_total']
        interval     = job['repost_interval']
        last_msg_id  = job['current_msg_id']
        user_data    = _json.loads(job['user_data_json'])
        token_base   = _json.loads(job['token_base_json'])

        dex_api = DexscreenerAPI()

        while reposts_done < reposts_total:
            # Calcola quanto aspettare per il prossimo repost
            # (gestisce correttamente i restart: se next_repost_at è già passato,
            #  sleep(0) → esegue immediatamente il repost mancato)
            now = time.time()
            wait_sec = max(0, job['next_repost_at'] - now) if reposts_done == job['reposts_done'] else interval
            if wait_sec > 0:
                logger.info(f"⏳ Promo job {job_id} ({symbol}): next repost in {wait_sec/60:.1f} min")
            await asyncio.sleep(wait_sec)
            # Da questo punto in poi usa sempre l'intervallo fisso
            job['next_repost_at'] = time.time() + interval

            # Unpin del messaggio precedente
            try:
                await self.bot.unpin_chat_message(chat_id=channel_id, message_id=last_msg_id)
            except Exception:
                pass

            # Fetch dati live
            try:
                live_data, _holders = await asyncio.gather(
                    dex_api.get_token_data(mint),
                    dex_api.fetch_holders(mint),
                )
                live_data = live_data or {}
                rc_live = {}
                bundle = {}
            except Exception as e:
                logger.warning(f"Promo job {job_id}: live data fetch failed ({e}), using empty data")
                live_data = {}
                rc_live   = {}
                bundle    = {}
                _holders  = None

            try:
                # Merge social: priorità a quelli salvati dall'utente, fallback da live_data (pump.fun)
                _website  = user_data.get('website')       or live_data.get('website')
                _twitter  = user_data.get('twitter_link')  or live_data.get('twitter')
                _telegram = user_data.get('telegram_link') or live_data.get('telegram')
                _discord  = user_data.get('discord')       or live_data.get('discord')

                live_msg = await format_promo_message(
                    mint=mint,
                    name=token_base.get('name'),
                    symbol=token_base.get('symbol'),
                    market_cap=live_data.get('marketCap', 0),
                    holders=_holders,
                    price_usd=live_data.get('priceUsd'),
                    liquidity=live_data.get('liquidity'),
                    volume1h=live_data.get('volume1h'),
                    buys1h=live_data.get('buys1h'),
                    sells1h=live_data.get('sells1h'),
                    txns1h=live_data.get('txns1h'),
                    priceChange1h=live_data.get('priceChange1h'),
                    pairCreatedAt=live_data.get('pairCreatedAt'),
                    website=_website,
                    twitter=_twitter,
                    telegram_link=_telegram,
                    discord=_discord,
                    rugcheck_score=rc_live.get('score'),
                    rugcheck_label=rc_live.get('label'),
                    rugcheck_top_holder=rc_live.get('top_holder'),
                    rugcheck_top10=rc_live.get('top10'),
                    rugcheck_whales=rc_live.get('whales'),
                    bundle=bundle,
                )
            except Exception as e:
                logger.error(f"Promo job {job_id}: format_promo_message failed ({e})")
                continue

            # Keyboard
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username
            repost_keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🤖 GMGN",   url=f"https://t.me/GMGN_sol_bot?start=i_bsVrj7Mc_c_{mint}"),
                    InlineKeyboardButton("⚡️ Axiom",  url=f"https://axiom.trade/t/{mint}/@inscribe?chain=sol"),
                ],
                [InlineKeyboardButton("🚀 Buy Trending", url=f"https://t.me/{self._bot_username}?start=buytrending")],
            ])

            # Invia il nuovo messaggio
            new_msg = None
            try:
                logo_bytes = await self._normalize_logo(logo_url, symbol)
                new_msg = await self.bot.send_photo(
                    chat_id=channel_id,
                    photo=logo_bytes,
                    caption=live_msg,
                    parse_mode='HTML',
                    reply_markup=repost_keyboard,
                )
            except Exception as e:
                logger.warning(f"Promo job {job_id}: photo send failed ({e}), fallback to text")
                try:
                    new_msg = await self.bot.send_message(
                        chat_id=channel_id,
                        text=live_msg,
                        parse_mode='HTML',
                        reply_markup=repost_keyboard,
                    )
                except Exception as e2:
                    logger.error(f"Promo job {job_id}: text send also failed ({e2})")
                    continue

            if not new_msg:
                continue

            # Pin del nuovo messaggio
            try:
                await self.bot.pin_chat_message(
                    chat_id=channel_id,
                    message_id=new_msg.message_id,
                    disable_notification=True,
                )
            except Exception as e:
                logger.warning(f"Promo job {job_id}: pin failed ({e})")

            # Aggiorna stato
            reposts_done += 1
            last_msg_id = new_msg.message_id
            next_repost_at = time.time() + interval
            db.update_promo_job_after_repost(job_id, last_msg_id, reposts_done, next_repost_at)
            logger.info(f"🔄 Promo job {job_id} ({symbol}): repost {reposts_done}/{reposts_total} done (msg_id={last_msg_id})")

        # Job completato — unpin finale e marca DB
        await asyncio.sleep(60)
        try:
            await self.bot.unpin_chat_message(chat_id=channel_id, message_id=last_msg_id)
        except Exception:
            pass
        db.complete_promo_job(job_id)
        logger.info(f"✅ Promo job {job_id} ({symbol}): completed all {reposts_total} reposts")

    # ── Nuovi token ───────────────────────────────────────

    # ── Persistent mint store via Telegram pinned message ──────────────────
    # Salviamo i mint postati in un messaggio speciale nel canale stesso.
    # Sopravvive a qualsiasi restart/redeploy senza Redis o filesystem.

    async def _recover_posted_mints_from_channel(self):
        """Al boot i mint sono già caricati dal DB SQLite persistente — nulla da fare"""
        logger.info(f"📂 Boot: {len(self.posted_mints)} posted mints loaded from DB")

    async def _persist_mints_to_channel(self):
        """Con il DB SQLite i dati sono già persistiti — nulla da fare qui"""
        pass

    async def check_trending_tokens(self):
        if self._posting_lock.locked():
            logger.info("⏳ Previous cycle still running, skipping this tick")
            return
        async with self._posting_lock:
            await self._do_check_trending_tokens()

    async def _do_check_trending_tokens(self):
        """CTO discovery — fetcha i community takeover da DexScreener e delega a _process_token_candidates."""
        try:
            tokens = await self.dexscreener_api.fetch_cto_tokens()
            logger.info(f"🤝 CTO poll: {len(tokens)} token(s)")
            await self._process_token_candidates(tokens)
        except Exception as e:
            logger.error(f"❌ check_trending_tokens error: {str(e)}")

    async def _process_token_candidates(self, tokens: list):
        """
        CTObot: posta TUTTI i nuovi CTO Solana senza filtri MC/rugcheck/bonding.
        L'unico filtro è: non già postato.
        """
        try:
            new_tokens = [
                t for t in tokens
                if t.get('mint')
                and t['mint'] not in self.posted_mints
                and t['mint'] not in self.tracked
            ]

            if not new_tokens:
                logger.info("ℹ️ No new CTO tokens this cycle")
                return

            logger.info(f"🤝 {len(new_tokens)} new CTO(s) to post")

            for best in new_tokens:
                mint = best['mint']
                sym  = best.get('baseToken', {}).get('symbol', mint[:8])
                mc   = best.get('marketCap', 0)

                # Fetch holders in background (best-effort, non blocca il post)
                try:
                    _holders = await asyncio.wait_for(
                        self.dexscreener_api.fetch_holders(mint), timeout=5.0
                    )
                    if _holders:
                        best['holders'] = _holders
                except Exception:
                    pass

                # Socials da pump.fun se DexScreener non li ha
                if not best.get('twitter') and not best.get('telegram'):
                    try:
                        _socials = await asyncio.wait_for(
                            self.dexscreener_api.fetch_pump_socials(mint), timeout=6.0
                        )
                        for _k in ('twitter', 'telegram', 'website', 'discord'):
                            if _socials.get(_k) and not best.get(_k):
                                best[_k] = _socials[_k]
                    except Exception:
                        pass

                # Logo fallback
                if not best.get('logo'):
                    try:
                        best['logo'] = await self.dexscreener_api._fetch_logo_for_mint(mint)
                    except Exception:
                        pass

                message = await format_token_message(
                    mint=mint,
                    name=best.get('baseToken', {}).get('name'),
                    symbol=sym,
                    market_cap=mc,
                    holders=best.get('holders') or None,
                    price_usd=float(best.get('priceUsd') or 0) or None,
                    liquidity=float(best.get('liquidity') or 0) or None,
                    volume1h=best.get('volume1h'),
                    volume24h=best.get('volume24h'),
                    txns1h=best.get('txns1h'),
                    buys1h=best.get('buys1h'),
                    sells1h=best.get('sells1h'),
                    priceChange1h=best.get('priceChange1h'),
                    priceChange24h=best.get('priceChange24h'),
                    pairCreatedAt=best.get('pairCreatedAt'),
                    logo_url=best.get('logo'),
                    website=best.get('website'),
                    twitter=best.get('twitter'),
                    discord=best.get('discord'),
                    telegram=best.get('telegram'),
                    bonding_curve_pct=None,
                    rugcheck_score=None,
                    rugcheck_top_holder=None,
                    rugcheck_top10=None,
                    rugcheck_whales=None,
                    bundle={},
                    # CTO extra fields per mostrare claim date nel messaggio
                    cto_claim_date=best.get('cto_claim_date'),
                    cto_description=best.get('cto_description'),
                )

                if await self._is_already_posted_on_channel(mint):
                    logger.warning(f"🚫 Duplicate blocked: {sym} ({mint[:8]})")
                    continue

                self.posted_mints.add(mint)
                db.add_posted_mint(mint)

                msg_id = await self.post_to_channel(
                    message, mint, best.get('logo'), symbol=sym,
                    name=best.get('baseToken', {}).get('name', sym),
                    mc=mc,
                    buys=best.get('buys1h'),
                    sells=best.get('sells1h'),
                    age_str=None,
                    is_promo=False,
                )

                if msg_id:
                    _info = best.get('info', {}) or {}
                    _socials_list = _info.get('socials', []) or []
                    _has_logo = bool((best.get('logo') or ''))
                    _has_social = bool(best.get('twitter') or best.get('telegram'))
                    _already_dex_updated = _has_logo and _has_social

                    self.tracked[mint] = TrackedToken(
                        mint=mint,
                        symbol=sym,
                        name=best.get('baseToken', {}).get('name', sym),
                        logo_url=best.get('logo') or '',
                        initial_mc=mc,
                        message_id=msg_id,
                        posted_at=time.time(),
                        dex_updated=_already_dex_updated,
                        dex_boost_posted=mint in {b.get('tokenAddress') for b in self._known_boosts},
                        dex_cto_posted=True,  # già è un CTO — non ripostare il cto alert
                    )
                    self.tracked[mint].original_caption = message
                    db.save_tracked_token(self.tracked[mint])
                    logger.info(f"✅ CTO posted: {sym} | MC=${mc:,.0f} | {mint[:8]}")

                    # Multi-language broadcast
                    _token_kwargs = dict(
                        mint=mint,
                        name=best.get('baseToken', {}).get('name', sym),
                        symbol=sym,
                        market_cap=mc,
                        holders=best.get('holders') or None,
                        price_usd=best.get('priceUsd'),
                        liquidity=float(best.get('liquidity') or 0) or None,
                        volume1h=best.get('volume1h'),
                        txns1h=best.get('txns1h'),
                        buys1h=best.get('buys1h'),
                        sells1h=best.get('sells1h'),
                        priceChange1h=best.get('priceChange1h'),
                        pairCreatedAt=best.get('pairCreatedAt'),
                        logo_url=best.get('logo'),
                        website=best.get('website'),
                        twitter=best.get('twitter'),
                        discord=best.get('discord'),
                        telegram=best.get('telegram'),
                        rugcheck_score=None,
                        rugcheck_top_holder=None,
                        rugcheck_top10=None,
                        rugcheck_whales=None,
                        bundle={},
                    )



                else:
                    self.posted_mints.discard(mint)
                    db.remove_posted_mint(mint)
                    logger.warning(f"⚠️ Post failed for {sym}, removed from posted_mints")

                # Rate-limit gentile tra post CTO consecutivi
                await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"❌ _process_token_candidates error: {str(e)}")

    # ── Post al canale ────────────────────────────────────

    def _generate_placeholder_logo(self, symbol: str) -> bytes:
        """
        Genera un logo placeholder con le iniziali del token su sfondo colorato.
        Usato quando il logo non è disponibile o non scaricabile.
        """
        import hashlib
        # Colore determinístico basato sul simbolo
        h = hashlib.md5(symbol.encode()).hexdigest()
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        # Assicura che il colore non sia troppo scuro
        r, g, b = max(r, 80), max(g, 80), max(b, 80)

        if PIL_AVAILABLE:
            from PIL import ImageDraw, ImageFont
            img = Image.new("RGB", (LOGO_SIZE, LOGO_SIZE), (r, g, b))
            draw = ImageDraw.Draw(img)
            initials = symbol[:3].upper() if symbol else "?"
            # Stima dimensione testo proporzionale
            font_size = LOGO_SIZE // 3
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            except Exception:
                font = ImageFont.load_default()
            bbox = draw.textbbox((0, 0), initials, font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            x = (LOGO_SIZE - tw) // 2 - bbox[0]
            y = (LOGO_SIZE - th) // 2 - bbox[1]
            draw.text((x, y), initials, fill=(255, 255, 255), font=font)
            out = io.BytesIO()
            img.save(out, format='PNG')
            out.seek(0)
            return out.read()
        else:
            # Fallback minimale senza PIL: PNG 1x1 colorato
            import struct, zlib
            def png_chunk(name, data):
                c = zlib.crc32(name + data) & 0xffffffff
                return struct.pack('>I', len(data)) + name + data + struct.pack('>I', c)
            raw = b'\x89PNG\r\n\x1a\n'
            raw += png_chunk(b'IHDR', struct.pack('>IIBBBBB', 1, 1, 8, 2, 0, 0, 0))
            pixel = bytes([r, g, b])
            compressed = zlib.compress(b'\x00' + pixel)
            raw += png_chunk(b'IDAT', compressed)
            raw += png_chunk(b'IEND', b'')
            return raw

    def _generate_card(
        self,
        logo_bytes: bytes,
        symbol: str,
        name: str,
        mint: str,
        mc: float = None,
        buys: int = None,
        sells: int = None,
        age_str: str = None,
        milestone: float = None,
        is_promo: bool = False,
    ) -> bytes:
        """
        Genera la card PNG con Pillow.
        Layout a cursore verticale, dimensioni 1200px, testo leggibile.
        Un solo anello come contorno del logo. Niente emoji (crash DejaVu).
        """
        if not PIL_AVAILABLE:
            return logo_bytes

        try:
            from PIL import Image, ImageDraw, ImageFont

            # ── Palette ──
            if milestone is not None:
                if milestone >= 50:   accent, bg_top, bg_bot = (248,113,113), (18,4,4),   (8,2,2)
                elif milestone >= 10: accent, bg_top, bg_bot = (251,191,36),  (18,10,0),  (8,5,0)
                elif milestone >= 5:  accent, bg_top, bg_bot = (45,212,191),  (4,18,14),  (2,8,7)
                elif milestone >= 2:  accent, bg_top, bg_bot = (74,222,128),  (4,16,8),   (2,8,4)
                else:                 accent, bg_top, bg_bot = (148,163,184), (10,12,18), (6,8,12)
            elif is_promo:            accent, bg_top, bg_bot = (251,191,36),  (18,10,0),  (8,5,0)
            else:                     accent, bg_top, bg_bot = (74,222,128),  (4,16,8),   (2,8,4)

            W        = 1200
            PAD      = 44
            LOGO_SZ  = 260
            TOPBAR_H = 80
            STAT_H   = 110
            FOOTER_H = 60
            GAP_S    = 12
            GAP_M    = 28

            BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
            REG  = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

            def F(size, bold=True):
                try:    return ImageFont.truetype(BOLD if bold else REG, size)
                except: return ImageFont.load_default()

            _mi = Image.new("RGBA", (1, 1))
            _md = ImageDraw.Draw(_mi)
            def tw(t, f): bb = _md.textbbox((0,0), t, font=f); return bb[2]-bb[0]
            def th(t, f): bb = _md.textbbox((0,0), t, font=f); return bb[3]-bb[1]

            sym_clean  = symbol.upper()
            name_clean = name[:32] + ("..." if len(name) > 32 else "")
            age_clean  = age_str or ""

            if milestone is not None:
                mult_text = f"+{int((milestone-1)*100)}%" if milestone < 2 else f"{int(milestone)}x"
                content_h = th(mult_text, F(130)) + GAP_M
            else:
                content_h = STAT_H + GAP_M

            H = (TOPBAR_H + GAP_M + LOGO_SZ + GAP_M
                 + th(sym_clean, F(52)) + GAP_S
                 + th(name_clean, F(26, False)) + GAP_S
                 + (th(age_clean, F(22, False)) + GAP_S if age_clean else 0)
                 + GAP_M + content_h + FOOTER_H)
            H = max(H, 740)

            # gradiente verticale
            card = Image.new("RGB", (W, H))
            for y in range(H):
                t = y / H
                r = int(bg_top[0] + (bg_bot[0]-bg_top[0])*t)
                g = int(bg_top[1] + (bg_bot[1]-bg_top[1])*t)
                b = int(bg_top[2] + (bg_bot[2]-bg_top[2])*t)
                ImageDraw.Draw(card).line([(0,y),(W,y)], fill=(r,g,b))

            # glow angolo alto-destra
            gl = Image.new("RGBA", (W, H), (0,0,0,0))
            gd = ImageDraw.Draw(gl)
            for r in range(380, 0, -8):
                a = int(18*(1-r/380))
                gd.ellipse([W-r, -r//2, W+r//2, r], fill=accent+(a,))
            card_rgba = Image.alpha_composite(card.convert("RGBA"), gl)
            draw = ImageDraw.Draw(card_rgba)

            # ── TOP BAR ──
            cy = 28
            draw.text((PAD, cy), "pump.fun early trending",
                      font=F(20, False), fill=(80,100,120,200))

            badge = (mult_text if milestone is not None and milestone < 2
                     else (f"{int(milestone)}x" if milestone is not None
                     else "New Trending"))
            bf = F(22)
            bw = tw(badge, bf) + 40
            bh = 44
            bx = W - bw - PAD
            by = cy - 6
            draw.rounded_rectangle([bx, by, bx+bw, by+bh], radius=22,
                                    fill=(20,30,20,255), outline=accent+(200,), width=2)
            draw.text((bx+(bw-tw(badge,bf))//2, by+10), badge, font=bf, fill=accent+(255,))

            cy += TOPBAR_H

            # ── LOGO — un solo anello come contorno ──
            lx = (W - LOGO_SZ) // 2
            ly = cy
            draw.ellipse([lx-4, ly-4, lx+LOGO_SZ+4, ly+LOGO_SZ+4],
                         outline=accent+(200,), width=4)

            logo_img = Image.open(io.BytesIO(logo_bytes)).convert("RGBA")
            logo_img = logo_img.resize((LOGO_SZ, LOGO_SZ), Image.LANCZOS)
            cm = Image.new("L", (LOGO_SZ, LOGO_SZ), 0)
            ImageDraw.Draw(cm).ellipse([0, 0, LOGO_SZ, LOGO_SZ], fill=255)
            card_rgba.paste(logo_img, (lx, ly), cm)
            draw = ImageDraw.Draw(card_rgba)
            cy += LOGO_SZ + GAP_M

            # ── SYMBOL ──
            sf = F(52); sw = tw(sym_clean, sf)
            draw.text(((W-sw)//2, cy), sym_clean, font=sf, fill=(255,255,255,255))
            cy += th(sym_clean, sf) + GAP_S

            # ── NAME ──
            nf = F(26, False); nw = tw(name_clean, nf)
            draw.text(((W-nw)//2, cy), name_clean, font=nf, fill=(150,170,190,255))
            cy += th(name_clean, nf) + GAP_S

            # ── AGE ──
            if age_clean:
                af = F(22, False); aw = tw(age_clean, af)
                draw.text(((W-aw)//2, cy), age_clean, font=af, fill=accent+(160,))
                cy += th(age_clean, af) + GAP_S
            cy += GAP_M

            # ── CONTENUTO ──
            if milestone is not None:
                mf = F(130); mw = tw(mult_text, mf); mh = th(mult_text, mf)
                draw.text(((W-mw)//2+3, cy+3), mult_text, font=mf, fill=(0,0,0,60))
                draw.text(((W-mw)//2, cy), mult_text, font=mf, fill=accent+(255,))
                cy += mh + GAP_M
            else:
                stats = []
                if mc is not None:
                    mc_s = f"${mc/1000:.1f}k" if mc < 1_000_000 else f"${mc/1_000_000:.2f}M"
                    stats.append((mc_s, "MC"))
                if buys is not None:  stats.append((str(buys), "Buys"))
                if sells is not None: stats.append((str(sells), "Sells"))
                if stats:
                    n = len(stats)
                    box_w = (W - PAD*(n+1)) // n
                    vf = F(32); lf = F(20, False)
                    for i, (val, lbl) in enumerate(stats):
                        bx = PAD + i*(box_w+PAD)
                        draw.rounded_rectangle([bx, cy, bx+box_w, cy+STAT_H],
                                                radius=14, fill=(15,25,15,255),
                                                outline=accent+(90,), width=2)
                        vw = tw(val, vf)
                        draw.text((bx+(box_w-vw)//2, cy+16), val, font=vf, fill=(255,255,255,255))
                        lw = tw(lbl, lf)
                        draw.text((bx+(box_w-lw)//2, cy+STAT_H-30), lbl,
                                  font=lf, fill=(100,130,160,255))
                cy += STAT_H + GAP_M

            # ── FOOTER ──
            fy = H - FOOTER_H
            draw.line([(PAD, fy), (W-PAD, fy)], fill=(255,255,255,18), width=1)
            dt = fy + (FOOTER_H-14)//2
            draw.ellipse([PAD, dt, PAD+14, dt+14], fill=accent+(230,))
            draw.text((PAD+22, dt), "spotted by pumpbot",
                      font=F(18, False), fill=(90,115,140,255))
            ca_short = f"{mint[:8]}...{mint[-6:]}"
            caf = F(18, False); caw = tw(ca_short, caf)
            draw.text((W-caw-PAD, dt), ca_short, font=caf, fill=(70,95,120,255))

            out = io.BytesIO()
            card_rgba.convert("RGB").save(out, format="PNG", optimize=True)
            out.seek(0)
            return out.read()

        except Exception as e:
            logger.warning(f"_generate_card failed ({e}), fallback to raw logo")
            return logo_bytes

    async def _normalize_logo(self, logo_url: str, symbol: str = "?") -> bytes:
        """
        Scarica il logo e lo ridimensiona a LOGO_SIZE x LOGO_SIZE (HD uniforme).
        Se il logo non è disponibile, genera un placeholder con le iniziali.
        Ritorna sempre bytes PNG validi.
        """
        if logo_url and logo_url.startswith('http'):
            logger.info(f"🖼 Downloading logo for {symbol}: {logo_url[:60]}")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(logo_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            raw = await resp.read()

                            if not PIL_AVAILABLE:
                                return raw

                            img = Image.open(io.BytesIO(raw))
                            # Crop quadrato centrato
                            w, h = img.size
                            side = min(w, h)
                            left = (w - side) // 2
                            top  = (h - side) // 2
                            img  = img.crop((left, top, left + side, top + side))
                            img  = img.resize((LOGO_SIZE, LOGO_SIZE), Image.LANCZOS)
                            # Compositing su sfondo bianco per gestire trasparenza
                            bg = Image.new("RGBA", (LOGO_SIZE, LOGO_SIZE), (255, 255, 255, 255))
                            if img.mode == "RGBA":
                                bg.paste(img, mask=img.split()[3])
                            else:
                                bg.paste(img.convert("RGBA"))
                            final = bg.convert("RGB")
                            out = io.BytesIO()
                            final.save(out, format='JPEG', quality=95)
                            out.seek(0)
                            return out.read()
                        else:
                            logger.warning(f"Logo HTTP {resp.status} for {logo_url}, using placeholder")
            except Exception as e:
                logger.warning(f"Logo download failed ({e}), using placeholder")
        else:
            logger.info(f"No logo URL for {symbol}, using placeholder")

        return self._generate_placeholder_logo(symbol)

    async def _is_already_posted_on_channel(self, mint: str) -> bool:
        """
        Controlla se la CA è già presente nei messaggi recenti del canale.
        Usato come rete di sicurezza anti-duplicati indipendente da /tmp.
        Legge gli ultimi 50 messaggi del canale cercando il mint nel testo/caption.
        """
        try:
            # Cerca il mint nei messaggi del canale usando una cache in memoria
            # Telegram bot API non ha un metodo diretto per leggere la storia,
            # ma possiamo usare il trucco di forward+delete per leggere messaggi specifici.
            # La soluzione più affidabile è tenere un set in memoria dei mint postati
            # in questa sessione (già fatto con posted_mints) e usare il channel store
            # message come backup. Questo metodo aggiunge un check sul set self.tracked
            # che è la fonte più affidabile: se il token è in tracked, è stato postato.
            if mint in self.tracked:
                logger.info(f"🔍 Contract check: {mint[:8]} already in tracked — skip")
                return True
            if mint in self.posted_mints:
                logger.info(f"🔍 Contract check: {mint[:8]} already in posted_mints — skip")
                return True
            return False
        except Exception as e:
            logger.debug(f"Contract check error: {e}")
            return False  # in caso di errore, lascia passare

    async def post_to_channel(
        self, message: str, mint: str, logo_url: str = None, symbol: str = "?",
        name: str = None, mc: float = None, buys: int = None, sells: int = None,
        age_str: str = None, is_promo: bool = False,
    ) -> Optional[int]:
        try:
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username
            reply_markup = _make_token_keyboard(mint, self._bot_username)

            # Prova sempre con foto — usa logo diretto, fallback a testo
            try:
                logo_bytes = await self._normalize_logo(logo_url, symbol)
                sent = await self.bot.send_photo(
                    chat_id=self.channel_id,
                    photo=logo_bytes,
                    caption=message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                logger.info(f"📤 Photo sent for {mint[:8]}...")
                msg_id = sent.message_id
            except Exception as photo_err:
                logger.warning(f"Photo failed ({photo_err}), fallback to text")
                sent = await self.bot.send_message(
                    chat_id=self.channel_id,
                    text=message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                logger.info(f"📤 Message sent for {mint[:8]}...")
                msg_id = sent.message_id

            # Manda DM all'owner con il contract e bottone GMGN
            # BonkBot e owner DM rimossi
            return msg_id

        except Exception as e:
            logger.error(f"❌ Error posting to channel: {str(e)}...")
            return None


    async def _on_whale_buy(self, mint: str, sol_amount: float, usd_amount: float, tx_signature: str, buyer_wallet: str):
        """
        Callback chiamato da HeliusWhaleWS quando un buy >= WHALE_MIN_SOL viene rilevato.
        Posta il whale alert in reply al messaggio originale del token.
        Anti-doppioni: max 1 alert per (mint, ora).
        """
        tracked = self.tracked.get(mint)
        if not tracked:
            return

        # Anti-doppioni: 1 alert per token per ora
        bucket = int(time.time() // 3600)
        dedup_key = f"{mint}:{bucket}"
        if dedup_key in self._whale_seen_txs:
            return
        self._whale_seen_txs.add(dedup_key)
        if len(self._whale_seen_txs) > 5000:
            self._whale_seen_txs = set(list(self._whale_seen_txs)[-2000:])

        # Fetch dati live per il messaggio
        try:
            live = await asyncio.wait_for(
                self.dexscreener_api.get_token_data(mint), timeout=5.0
            )
            live = live or {}
        except Exception:
            live = {}

        try:
            _holders = await asyncio.wait_for(
                self.dexscreener_api.fetch_holders(mint), timeout=5.0
            )
        except Exception:
            _holders = None

        mc    = float(live.get('marketCap', 0) or tracked.initial_mc)
        price = float(live.get('priceUsd', 0) or 0) or None
        liq   = float(live.get('liquidity', 0) or 0) or None
        pc1h  = float(live.get('priceChange1h', 0) or 0)

        alert = format_whale_alert(
            mint=mint,
            symbol=tracked.symbol,
            name=tracked.name,
            sol_amount=sol_amount,
            usd_amount=usd_amount,
            market_cap=mc,
            buyer_wallet=buyer_wallet or "unknown",
            tx_signature=tx_signature,
            price_usd=price,
            liquidity=liq,
            holders=_holders,
            priceChange1h=pc1h,
        )

        if not self._bot_username:
            bot_info = await self.bot.get_me()
            self._bot_username = bot_info.username
        whale_keyboard = _make_token_keyboard(mint, self._bot_username)

        try:
            await self.bot.send_message(
                chat_id=self.channel_id,
                text=alert,
                reply_markup=whale_keyboard,
                parse_mode='HTML',
                disable_web_page_preview=True,
                reply_to_message_id=tracked.message_id,
            )
            logger.info(f"🐳 Whale alert posted: {tracked.symbol} {sol_amount:.2f} SOL (${usd_amount:,.0f})")
        except Exception as e:
            logger.error(f"🐳 Whale alert send failed for {tracked.symbol}: {e}")

    async def _get_sol_price(self) -> float:
        """Fetch prezzo SOL/USD da Dexscreener"""
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pairs = data.get('pairs', [])
                        if pairs:
                            return float(pairs[0].get('priceUsd', 150) or 150)
        except Exception:
            pass
        return 150.0

    async def _check_whale_buys(self):
        """
        Per ogni token trackato, usa DexScreener /trades per trovare
        i buy recenti >= WHALE_MIN_SOL. DexScreener non richiede API key
        e riporta direttamente il volume in USD per ogni trade.
        """
        if not self.tracked:
            return

        # Cache prezzo SOL
        now = time.time()
        if not hasattr(self, '_sol_price_cache') or now - self._sol_price_cache[1] > 60:
            sol_price = await self._get_sol_price()
            self._sol_price_cache = (sol_price, now)
        SOL_PRICE_USD = self._sol_price_cache[0]
        whale_min_usd = config.WHALE_MIN_SOL * SOL_PRICE_USD

        logger.info(f"🐳 Whale check: {len(self.tracked)} tokens | min=${whale_min_usd:,.0f} (~{config.WHALE_MIN_SOL} SOL)")

        async with aiohttp.ClientSession() as session:
            for mint, tracked in list(self.tracked.items()):
                try:
                    await self._check_whale_for_token(session, mint, tracked, SOL_PRICE_USD, whale_min_usd)
                except Exception as e:
                    logger.warning(f"Whale check error for {tracked.symbol} ({mint[:8]}): {e}")
                await asyncio.sleep(0.3)  # rate limit gentile

    async def _check_whale_for_token(
        self,
        session: aiohttp.ClientSession,
        mint: str,
        tracked,
        sol_price: float,
        whale_min_usd: float,
    ):
        """
        Rileva whale activity usando i dati aggregati DexScreener:
        avg_buy_size = volume1h / buys1h
        Se avg_buy_size >= whale_min_usd * 1.5 → possibile whale in azione.
        
        Non identifica il singolo wallet ma non richiede API key.
        Anti-doppioni: chiave (mint, snapshot_bucket) dove bucket = floor(time/300).
        """
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        except Exception as e:
            logger.debug(f"Whale DexScreener error {mint[:8]}: {e}")
            return

        pairs = data.get('pairs', [])
        if not pairs:
            return
        pair = next((p for p in pairs if p.get('dexId') == 'pump'), pairs[0])

        volume1h = float(pair.get('volume', {}).get('h1', 0) or 0)
        buys1h   = int(pair.get('txns', {}).get('h1', {}).get('buys', 0) or 0)

        if buys1h == 0 or volume1h == 0:
            return

        avg_buy_usd = volume1h / buys1h
        # Soglia: avg buy size >= 1.5× il minimo whale
        # (compensa che è una media, non un singolo trade)
        if avg_buy_usd < whale_min_usd * 1.5:
            return

        # Anti-doppioni: 1 alert per token ogni 60 minuti max
        bucket = int(time.time() // 3600)
        dedup_key = f"{mint}:{bucket}"
        if dedup_key in self._whale_seen_txs:
            return
        self._whale_seen_txs.add(dedup_key)
        if len(self._whale_seen_txs) > 5000:
            self._whale_seen_txs = set(list(self._whale_seen_txs)[-2000:])

        sol_equivalent = avg_buy_usd / sol_price if sol_price > 0 else 0
        mc    = float(pair.get('marketCap', 0) or tracked.initial_mc)
        price = float(pair.get('priceUsd', 0) or 0) or None
        liq   = float(pair.get('liquidity', {}).get('usd', 0) or 0) or None
        pc1h  = float(pair.get('priceChange', {}).get('h1', 0) or 0)
        name  = pair.get('baseToken', {}).get('name', tracked.symbol)

        # Fetch holders in parallelo con la costruzione del messaggio
        try:
            _holders = await asyncio.wait_for(
                self.dexscreener_api.fetch_holders(mint), timeout=5.0
            )
        except Exception:
            _holders = None

        alert = format_whale_alert(
            mint=mint,
            symbol=tracked.symbol,
            name=name,
            sol_amount=sol_equivalent,
            usd_amount=avg_buy_usd,
            market_cap=mc,
            buyer_wallet=f"avg of {buys1h} buys",
            tx_signature="",
            price_usd=price,
            liquidity=liq,
            holders=_holders,
            priceChange1h=pc1h,
        )

        if not self._bot_username:
            bot_info = await self.bot.get_me()
            self._bot_username = bot_info.username
        whale_keyboard = _make_token_keyboard(mint, self._bot_username)

        await self.bot.send_message(
            chat_id=self.channel_id,
            text=alert,
            reply_markup=whale_keyboard,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_to_message_id=tracked.message_id,
        )
        logger.info(f"🐳 Whale alert: {tracked.symbol} avg_buy=${avg_buy_usd:,.0f} (~{sol_equivalent:.1f} SOL) across {buys1h} buys")



    async def _gain_check_loop(self):
        """Controlla i gain ogni 30 secondi"""
        while True:
            await asyncio.sleep(30)
            try:
                if self._gain_check_lock.locked():
                    logger.info("⏳ Gain check still running, skipping tick")
                    continue
                async with self._gain_check_lock:
                    await self._check_gains()
            except Exception as e:
                logger.error(f"❌ Gain check error: {str(e)}")

    async def _fetch_pumpfun_mc(self, session, mint: str) -> float:
        """Fetch MC real-time da pump.fun per un singolo mint."""
        try:
            async with session.get(
                f"https://frontend-api.pump.fun/coins/{mint}",
                timeout=aiohttp.ClientTimeout(total=3),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as r:
                if r.status == 200:
                    d = await r.json(content_type=None)
                    usd_mc = float(d.get('usd_market_cap', 0) or 0)
                    vsol   = float(d.get('virtual_sol_reserves', 0) or 0)
                    vtok   = float(d.get('virtual_token_reserves', 0) or 0)
                    supply = float(d.get('total_supply', 0) or 0)
                    if vsol > 0 and vtok > 0 and supply > 0 and usd_mc > 0:
                        price_sol = vsol / vtok
                        mc_sol = price_sol * supply
                        sol_price = usd_mc / mc_sol if mc_sol > 0 else 150
                        return mc_sol * sol_price
                    elif usd_mc > 0:
                        return usd_mc
        except Exception:
            pass
        return 0.0

    async def _batch_fetch_mc(self, mints: list) -> dict:
        """
        Fetch MC per una lista di mints.
        1. DexScreener batch — copre sia token pump.fun che graduated su PumpSwap
        2. pump.fun in parallelo per i missing — token freschi non ancora su DexScreener
        """
        mc_map = {}
        if not mints:
            return mc_map

        batch_size = 30
        try:
            async with aiohttp.ClientSession() as session:
                # ── Step 1: DexScreener batch ───────────────────────────────
                for i in range(0, len(mints), batch_size):
                    batch = mints[i:i + batch_size]
                    addresses = ','.join(batch)
                    url = f"https://api.dexscreener.com/latest/dex/tokens/{addresses}"
                    try:
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            if resp.status != 200:
                                continue
                            data = await resp.json()
                            pairs_by_mint: dict = {}
                            for pair in data.get('pairs', []):
                                mint = pair.get('baseToken', {}).get('address', '')
                                mc = float(pair.get('marketCap', 0) or 0)
                                if mint and mc > 0:
                                    if mint not in pairs_by_mint:
                                        pairs_by_mint[mint] = []
                                    pairs_by_mint[mint].append(pair)

                            for mint, pairs in pairs_by_mint.items():
                                best = max(pairs, key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0))
                                mc = float(best.get('marketCap', 0) or 0)
                                if mc > 0:
                                    info = best.get('info', {}) or {}
                                    socials = info.get('socials', []) or []
                                    dex_logo = (
                                        (info.get('imageUrl') or '').strip() or
                                        (info.get('icon') or '').strip()
                                    )
                                    dex_twitter = next(
                                        (s.get('url') for s in socials if s.get('type') == 'twitter'), None
                                    )
                                    dex_telegram = next(
                                        (s.get('url') for s in socials if s.get('type') == 'telegram'), None
                                    )
                                    mc_map[mint] = {
                                        'mc': mc,
                                        'dex_ids': {p.get('dexId', '') for p in pairs},
                                        'liquidity': float(best.get('liquidity', {}).get('usd', 0) or 0),
                                        'dex_logo': dex_logo,
                                        'dex_twitter': dex_twitter,
                                        'dex_telegram': dex_telegram,
                                    }
                    except Exception as e:
                        logger.debug(f"Batch fetch error batch {i//batch_size}: {e}")
                    await asyncio.sleep(0.1)

                # ── Step 2: mint non trovati → pump.fun in parallelo ────────
                # Questi sono token freschissimi non ancora indicizzati su DexScreener
                missing = [m for m in mints if m not in mc_map]
                if missing:
                    logger.info(f"📊 {len(missing)} not in DexScreener → trying pump.fun...")
                    sem = asyncio.Semaphore(20)
                    async def _pf_safe(m):
                        async with sem:
                            return await self._fetch_pumpfun_mc(session, m)
                    pf_results = await asyncio.gather(*[_pf_safe(m) for m in missing])
                    recovered = 0
                    for mint, pf_mc in zip(missing, pf_results):
                        if pf_mc > 0:
                            mc_map[mint] = {
                                'mc': pf_mc,
                                'dex_ids': set(),
                                'liquidity': 0.0,
                                'dex_logo': '',
                                'dex_twitter': None,
                                'dex_telegram': None,
                            }
                            recovered += 1
                    logger.info(f"📊 pump.fun recovered {recovered}/{len(missing)} missing mints")

        except Exception as e:
            logger.warning(f"_batch_fetch_mc error: {e}")

        logger.info(f"📊 Batch MC fetch: {len(mc_map)}/{len(mints)} tokens found")
        still_missing = [m for m in mints if m not in mc_map]
        if still_missing:
            logger.info(f"📊 Still not found: {len(still_missing)} tokens (likely dead/rugged)")
        return mc_map

    async def _check_gains(self):
        if not self.tracked:
            return
        logger.info(f"📊 Checking gains for {len(self.tracked)} tokens")

        mc_map = await self._batch_fetch_mc(list(self.tracked.keys()))

        for mint, tracked in list(self.tracked.items()):
            try:
                entry = mc_map.get(mint)
                if not entry or not tracked.initial_mc:
                    continue
                current_mc = entry['mc']
                dex_ids    = entry['dex_ids']
                liquidity  = entry.get('liquidity', 0)

                # ── Durata tracking e rimozione token morti ──────────────────
                #
                # Peak multiplier: aggiorna sempre al massimo raggiunto
                current_multiplier_now = current_mc / tracked.initial_mc
                if current_multiplier_now > tracked.peak_multiplier:
                    tracked.peak_multiplier = current_multiplier_now
                    db.update_tracked_token_field(mint, peak_multiplier=current_multiplier_now)

                peak = tracked.peak_multiplier
                age_sec = time.time() - tracked.posted_at

                # ── Regola 0: MC assoluto sotto $7k → token morto, rimuovi sempre ──
                if current_mc < 7_000:
                    logger.info(f"🪦 {tracked.symbol} MC below $7k (${current_mc:,.0f}) — removed")
                    del self.tracked[mint]
                    db.delete_tracked_token(mint)
                    continue

                # ── Regola 1: morto per crollo MC ──
                # Se MC è sceso ≥60% dal MC iniziale del post → rimosso
                if current_mc < tracked.initial_mc * 0.40:
                    # Eccezione: se ha fatto ≥100x e MC è ancora sopra 20x → non rimuovere
                    if not (peak >= 100 and current_multiplier_now >= 20):
                        logger.info(f"🪦 {tracked.symbol} dropped ≥60% from initial MC (${tracked.initial_mc:,.0f} → ${current_mc:,.0f}) — removed")
                        del self.tracked[mint]
                        db.delete_tracked_token(mint)
                        continue

                # ── Regola 2: durata per performance ──
                # ≥100x → illimitato (rimosso solo se scende sotto 20x)
                if peak >= 100:
                    if current_multiplier_now < 20:
                        logger.info(f"🪦 {tracked.symbol} was 100x but dropped below 20x — removed")
                        del self.tracked[mint]
                        db.delete_tracked_token(mint)
                        continue
                    # altrimenti nessun limite di tempo
                # ≥50x → 7 giorni
                elif peak >= 50:
                    if age_sec > 604800:  # 7 giorni
                        logger.info(f"🕰 {tracked.symbol} 50x+ expired after 7d — removed")
                        del self.tracked[mint]
                        db.delete_tracked_token(mint)
                        continue
                # ≥20x → 96h
                elif peak >= 20:
                    if age_sec > 345600:  # 96h
                        logger.info(f"🕰 {tracked.symbol} 20x+ expired after 96h — removed")
                        del self.tracked[mint]
                        db.delete_tracked_token(mint)
                        continue
                # default → 48h
                else:
                    if age_sec > 172800:  # 48h
                        logger.info(f"🕰 {tracked.symbol} expired after 48h — removed")
                        del self.tracked[mint]
                        db.delete_tracked_token(mint)
                        continue

                # ── Dex profile update alert ──
                # Triggerato quando il token aggiunge logo + almeno un social su DexScreener.
                # Postato una sola volta, anti-doppioni via dex_updated flag nel DB.
                if not tracked.dex_updated:
                    dex_logo    = entry.get('dex_logo', '')
                    dex_twitter = entry.get('dex_twitter')
                    dex_telegram = entry.get('dex_telegram')
                    has_logo   = bool(dex_logo)
                    has_social = bool(dex_twitter or dex_telegram)
                    if has_logo and has_social:
                        posted = await self._post_dex_update_alert(tracked, current_mc, dex_twitter, dex_telegram)
                        if posted:
                            tracked.dex_updated = True
                            db.update_tracked_token_field(mint, dex_updated=1)
                            logger.info(f"🔔 Dex update alert posted for {tracked.symbol}")

                current_multiplier = current_mc / tracked.initial_mc
                logger.info(f"📊 {tracked.symbol}: initial=${tracked.initial_mc:,.0f} current=${current_mc:,.0f} mult={current_multiplier:.2f}x last_notified={tracked.last_notified_multiplier}")

                # Calcola TUTTI i milestone pendenti in una volta
                pending = []
                temp = tracked.last_notified_multiplier
                while True:
                    m = _next_milestone(current_multiplier, temp)
                    if m is None:
                        break
                    pending.append(m)
                    temp = m

                if not pending:
                    continue

                # Se ci sono più di 1 milestone pendente significa catch-up
                # (restart dopo un gap, o primo tick dopo un pump violento).
                # In questo caso posta SOLO il milestone più alto, allineando
                # silenziosamente tutti quelli intermedi per evitare spam.
                if len(pending) > 1:
                    # Allinea in memoria e DB tutti i milestone intermedi
                    second_last = pending[-2]
                    tracked.last_notified_multiplier = second_last
                    db.update_tracked_token_field(mint, last_notified_multiplier=second_last)
                    for m in pending[:-1]:
                        db.claim_gain_alert(mint, m)
                    logger.info(
                        f"⏭ {tracked.symbol} catch-up: skipping {len(pending)-1} intermediate "
                        f"milestones ({pending[0]}x→{pending[-2]}x), posting only {pending[-1]}x"
                    )
                    # Se il +50% non è mai stato postato, postalo prima del catch-up
                    if 1.5 in pending and tracked.last_notified_multiplier < 1.5:
                        await self._post_gain_alert(tracked, current_mc, 1.5)
                        logger.info(f"📢 {tracked.symbol} posting +50% before catch-up")
                    # Catch-up: posta subito senza aspettare
                    milestone = pending[-1]
                    self._pending_gains.pop(mint, None)
                    posted = await self._post_gain_alert(tracked, current_mc, milestone)
                    if posted:
                        self._update_top_performers(mint, tracked.symbol, current_multiplier, current_mc)
                        label = f"+{int((milestone-1)*100)}%" if milestone < 2 else f"{int(milestone)}x"
                        logger.info(f"🚀 {tracked.symbol} hit {label}! MC=${current_mc:,.0f}")
                        if tracked.last_notified_multiplier < milestone:
                            tracked.last_notified_multiplier = milestone
                        if milestone >= 2.0:
                            if not any(e['mint'] == mint for e in self._streak):
                                self._streak.append({'symbol': tracked.symbol, 'mint': mint, 'multiplier': current_multiplier, 'ts': time.time()})
                                db.add_streak_entry(mint, tracked.symbol, current_multiplier)
                                consec = self._compute_consecutive_streak()
                                if consec >= 2:
                                    await self._post_streak_alert(consec)
                    else:
                        if tracked.last_notified_multiplier < milestone:
                            tracked.last_notified_multiplier = milestone
                    continue

                # Esattamente 1 milestone nuovo — posta subito senza nessuna attesa
                milestone = pending[-1]
                self._pending_gains.pop(mint, None)
                posted = await self._post_gain_alert(tracked, current_mc, milestone)
                if posted:
                    self._update_top_performers(mint, tracked.symbol, current_multiplier, current_mc)
                    label = f"+{int((milestone-1)*100)}%" if milestone < 2 else f"{int(milestone)}x"
                    logger.info(f"🚀 {tracked.symbol} hit {label}! MC=${current_mc:,.0f}")
                    if tracked.last_notified_multiplier < milestone:
                        tracked.last_notified_multiplier = milestone
                    if milestone >= 2.0:
                        if not any(e['mint'] == mint for e in self._streak):
                            self._streak.append({'symbol': tracked.symbol, 'mint': mint, 'multiplier': current_multiplier, 'ts': time.time()})
                            db.add_streak_entry(mint, tracked.symbol, current_multiplier)
                            consec = self._compute_consecutive_streak()
                            logger.info(f"🔥 Streak updated: {len(self._streak)} tokens, {consec} consecutive")
                            if consec >= 2:
                                await self._post_streak_alert(consec)
                else:
                    if tracked.last_notified_multiplier < milestone:
                        tracked.last_notified_multiplier = milestone

            except Exception as e:
                logger.error(f"Gain check error for {mint}: {e}")

    async def _post_gain_alert(self, tracked: TrackedToken, current_mc: float, milestone: float) -> bool:
        """
        Posta il gain alert. Ritorna True se postato, False se duplicato/errore.

        Livelli di protezione anti-duplicato:
        ① last_notified_multiplier in memoria — check immediato, zero I/O
        ② Lock per-mint (asyncio.Lock) — serializza tutti gli await per lo stesso token
        ③ Claim atomico DB (BEGIN EXCLUSIVE) — barriera cross-processo/deploy overlap
        """
        # ① Check immediato su last_notified: se già notificato questo milestone, skip subito
        if tracked.last_notified_multiplier >= milestone:
            logger.info(f"⏭ Already notified {tracked.symbol} @ {milestone}x (last={tracked.last_notified_multiplier})")
            return False

        # ② Lock per-mint: un solo coroutine alla volta per questo token
        if tracked.mint not in self._gain_mint_locks:
            self._gain_mint_locks[tracked.mint] = asyncio.Lock()
        mint_lock = self._gain_mint_locks[tracked.mint]

        async with mint_lock:
            # Ri-controlla dopo aver acquisito il lock (potrebbe essere cambiato mentre aspettavamo)
            if tracked.last_notified_multiplier >= milestone:
                logger.info(f"⏭ Post-lock duplicate skipped for {tracked.symbol} @ {milestone}x")
                return False

            # Gap minimo tra gain alerts: evita spam su token in pump rapido
            GAIN_COOLDOWN_SEC = 180  # 3 minuti
            elapsed_since_last = time.time() - tracked.last_gain_alert_at
            if tracked.last_gain_alert_at > 0 and elapsed_since_last < GAIN_COOLDOWN_SEC:
                # Salva il milestone più alto raggiunto durante il cooldown
                # così quando scade lo postiamo anche se il token è già sceso
                if milestone > tracked.pending_peak_milestone:
                    tracked.pending_peak_milestone = milestone
                    logger.info(f"⏳ Gain cooldown for {tracked.symbol} — saved peak {milestone}x, {int(GAIN_COOLDOWN_SEC - elapsed_since_last)}s remaining")
                return False

            # Cooldown scaduto: se c'è un pending_peak più alto del milestone corrente, usa quello
            if tracked.pending_peak_milestone > milestone:
                milestone = tracked.pending_peak_milestone
                logger.info(f"📌 {tracked.symbol} posting pending peak {milestone}x (current MC may be lower)")
            tracked.pending_peak_milestone = 0.0  # reset pending

            # ③ Claim atomico DB — barriera contro deploy overlap (due processi Railway)
            if not db.claim_gain_alert(tracked.mint, milestone):
                logger.info(f"⏭ DB duplicate skipped for {tracked.symbol} @ {milestone}x")
                # Allinea memoria col DB
                tracked.last_notified_multiplier = milestone
                return False

            # Aggiorna last_notified SUBITO, dentro il lock, prima di qualsiasi await
            # Da questo momento qualsiasi altro coroutine che acquisisce il lock vedrà
            # il valore aggiornato e uscirà al check post-lock sopra
            tracked.last_notified_multiplier = milestone
            tracked.last_gain_alert_at = time.time()
            db.update_tracked_token_field(tracked.mint, last_notified_multiplier=milestone)

            try:
                if not self._bot_username:
                    bot_info = await self.bot.get_me()
                    self._bot_username = bot_info.username
                gain_keyboard = _make_token_keyboard(tracked.mint, self._bot_username)

                channel_username = config.CHANNEL_USERNAME.lstrip('@')
                original_post_link = (
                    f"https://t.me/{channel_username}/{tracked.message_id}"
                    if channel_username else None
                )

                alert = format_gain_alert(
                    symbol=tracked.symbol,
                    mint=tracked.mint,
                    milestone=milestone,
                    initial_mc=tracked.initial_mc,
                    current_mc=current_mc,
                    posted_at=tracked.posted_at,
                    original_post_link=original_post_link,
                )

                logo_url = tracked.logo_url or await self.dexscreener_api._fetch_logo_for_mint(tracked.mint)
                logo_io = await self._normalize_logo(logo_url, tracked.symbol)
                # Store as raw bytes so we can reuse across EN + all i18n channels
                logo_raw = logo_io.read() if hasattr(logo_io, 'read') else bytes(logo_io)

                print(f"[GAIN] Posting EN gain for {tracked.symbol} @ {milestone}x logo_raw={len(logo_raw) if logo_raw else 0}bytes", flush=True)
                sent_gain = await self.bot.send_photo(
                    chat_id=self.channel_id,
                    photo=io.BytesIO(logo_raw),
                    caption=alert,
                    reply_markup=gain_keyboard,
                    parse_mode='HTML',
                )
                print(f"[GAIN] EN posted OK msg_id={sent_gain.message_id}", flush=True)

                # Forward al canale Gains se milestone >= 5x
                if milestone >= 5.0:
                    gains_channel_id = os.getenv('GAINS_CHANNEL_ID')
                    if gains_channel_id:
                        try:
                            await self.bot.forward_message(
                                chat_id=int(gains_channel_id),
                                from_chat_id=self.channel_id,
                                message_id=sent_gain.message_id,
                            )
                            logger.info(f"📢 Forwarded {tracked.symbol} {int(milestone)}x to Gains channel")
                        except Exception as e:
                            logger.warning(f"Forward to Gains channel failed: {e}")



                await self._update_original_post(tracked, current_mc, milestone)
                return True

            except Exception as e:
                logger.error(f"Error posting gain alert for {tracked.symbol} @ {milestone}: {e}")
                return False


    async def _post_dex_update_alert(
        self,
        tracked: TrackedToken,
        current_mc: float,
        dex_twitter: str = None,
        dex_telegram: str = None,
    ) -> bool:
        """
        Posta un alert quando un token aggiorna il profilo DexScreener
        (logo + almeno un social). Postato una sola volta per token.
        """
        try:
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username

            channel_username = config.CHANNEL_USERNAME.lstrip('@')
            original_post_link = (
                f"https://t.me/{channel_username}/{tracked.message_id}"
                if channel_username else None
            )

            gain_mult = current_mc / tracked.initial_mc if tracked.initial_mc else 1.0
            if gain_mult >= 2:
                gain_str = f"{int(gain_mult)}x"
            elif gain_mult > 1:
                gain_str = f"+{int((gain_mult-1)*100)}%"
            else:
                gain_str = None

            mint = tracked.mint
            symbol = tracked.symbol
            gmgn_url = f"https://t.me/GMGN_sol_bot?start=i_bsVrj7Mc_c_{mint}"

            if original_post_link:
                symbol_display = f"<a href='{original_post_link}'><b>{symbol}</b></a>"
            else:
                symbol_display = f"<b>{symbol}</b>"

            msg  = f"🔔 {symbol_display} just updated their DexScreener profile\n\n"
            msg += f"💰 MC <b>${current_mc:,.0f}</b>"
            if gain_str:
                msg += f"  <b>{gain_str}</b> from signal"
            msg += "\n\n"

            socials = []
            if dex_twitter:
                socials.append(f"<a href='{dex_twitter}'><b>𝕏 Twitter</b></a>")
            if dex_telegram:
                socials.append(f"<a href='{dex_telegram}'><b>✈️ Telegram</b></a>")
            if socials:
                msg += "  |  ".join(socials) + "\n\n"

            msg += f"Buy on telegram with <a href='{gmgn_url}'><b>GMGN bot</b></a>\n\n"
            msg += f"<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
            msg += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>"

            keyboard = _make_token_keyboard(mint, self._bot_username)
            logo_url = tracked.logo_url or await self.dexscreener_api._fetch_logo_for_mint(mint)
            logo_bytes = await self._normalize_logo(logo_url, symbol)

            await self.bot.send_photo(
                chat_id=self.channel_id,
                photo=logo_bytes,
                caption=msg,
                reply_markup=keyboard,
                parse_mode='HTML',
            )
            return True

        except Exception as e:
            logger.error(f"Error posting dex update alert for {tracked.symbol}: {e}")
            return False

    # ── DexScreener Boost Alert ───────────────────────────────────────────────

    async def _boost_check_loop(self):
        """
        Controlla ogni 5 minuti i token boostati su DexScreener.
        Se un token già postato da noi riceve boost → posta alert una sola volta.
        """
        BOOST_INTERVAL = 300  # 5 minuti
        while True:
            try:
                await self._check_dex_boosts()
            except Exception as e:
                logger.error(f"Boost check loop error: {e}")
            await asyncio.sleep(BOOST_INTERVAL)

    async def _check_dex_boosts(self):
        """Fetch token boostati e controlla se sono tra i nostri tracked."""
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://api.dexscreener.com/token-boosts/latest/v1"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

            if not isinstance(data, list):
                return

            # Aggiorna cache boost — usata per skippare alert su token appena postati
            self._known_boosts = data

            # Filtra solo token Solana
            sol_boosts = {
                item['tokenAddress']: item
                for item in data
                if item.get('chainId') == 'solana' and item.get('tokenAddress')
            }

            for mint, boost_data in sol_boosts.items():
                tracked = self.tracked.get(mint)
                if not tracked:
                    continue
                if tracked.dex_boost_posted:
                    continue

                amount = boost_data.get('amount', 0)
                total_amount = boost_data.get('totalAmount', 0)

                posted = await self._post_dex_boost_alert(tracked, amount, total_amount)
                if posted:
                    tracked.dex_boost_posted = True
                    db.update_tracked_token_field(mint, dex_boost_posted=1)
                    logger.info(f"⚡️ Boost alert posted for {tracked.symbol}")

        except Exception as e:
            logger.warning(f"_check_dex_boosts error: {e}")

    async def _post_dex_boost_alert(
        self,
        tracked: TrackedToken,
        amount: int = 0,
        total_amount: int = 0,
    ) -> bool:
        """Posta il DexScreener boost alert per un token già postato."""
        try:
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username

            mint = tracked.mint
            symbol = tracked.symbol

            channel_username = config.CHANNEL_USERNAME.lstrip('@')
            original_post_link = (
                f"https://t.me/{channel_username}/{tracked.message_id}"
                if channel_username else None
            )

            if original_post_link:
                symbol_display = f"<a href='{original_post_link}'><b>{symbol}</b></a>"
            else:
                symbol_display = f"<b>{symbol}</b>"

            gmgn_url = f"https://t.me/GMGN_sol_bot?start=i_bsVrj7Mc_c_{mint}"

            msg  = f"⚡️ {symbol_display} just got a <b>DexScreener Boost</b>\n\n"
            if amount:
                msg += f"🔋 <b>{amount:,}</b> boosts added"
                if total_amount:
                    msg += f"  ·  <b>{total_amount:,}</b> total\n"
                else:
                    msg += "\n"
            msg += "\n"
            msg += f"Buy on telegram with <a href='{gmgn_url}'><b>GMGN bot</b></a>\n\n"
            msg += f"<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
            msg += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>"

            keyboard = _make_token_keyboard(mint, self._bot_username)
            logo_url = tracked.logo_url or await self.dexscreener_api._fetch_logo_for_mint(mint)
            logo_bytes = await self._normalize_logo(logo_url, symbol)

            await self.bot.send_photo(
                chat_id=self.channel_id,
                photo=logo_bytes,
                caption=msg,
                reply_markup=keyboard,
                parse_mode='HTML',
            )
            return True

        except Exception as e:
            logger.error(f"Error posting boost alert for {tracked.symbol}: {e}")
            return False

    # ── DexScreener Ads Alert ─────────────────────────────────────────────────

    async def _ads_check_loop(self):
        """Controlla ogni 5 minuti i token con ads attive su DexScreener."""
        while True:
            try:
                await self._check_dex_ads()
            except Exception as e:
                logger.error(f"Ads check loop error: {e}")
            await asyncio.sleep(300)

    async def _check_dex_ads(self):
        """Fetch ads attive e controlla se sono tra i nostri tracked."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.dexscreener.com/ads/latest/v1",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

            if not isinstance(data, list):
                return

            sol_ads = {
                item['tokenAddress']: item
                for item in data
                if item.get('chainId') == 'solana' and item.get('tokenAddress')
            }

            for mint, ad_data in sol_ads.items():
                tracked = self.tracked.get(mint)
                if not tracked or tracked.dex_ads_posted:
                    continue
                posted = await self._post_dex_ads_alert(tracked, ad_data)
                if posted:
                    tracked.dex_ads_posted = True
                    db.update_tracked_token_field(mint, dex_ads_posted=1)
                    logger.info(f"📢 Ads alert posted for {tracked.symbol}")

        except Exception as e:
            logger.warning(f"_check_dex_ads error: {e}")

    async def _post_dex_ads_alert(self, tracked: TrackedToken, ad_data: dict = {}) -> bool:
        """Posta il DexScreener ads alert per un token già postato."""
        try:
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username

            mint   = tracked.mint
            symbol = tracked.symbol

            channel_username = config.CHANNEL_USERNAME.lstrip('@')
            original_post_link = (
                f"https://t.me/{channel_username}/{tracked.message_id}"
                if channel_username else None
            )
            symbol_display = (
                f"<a href='{original_post_link}'><b>{symbol}</b></a>"
                if original_post_link else f"<b>{symbol}</b>"
            )

            gmgn_url = f"https://t.me/GMGN_sol_bot?start=i_bsVrj7Mc_c_{mint}"

            ad_type    = ad_data.get('type', '')
            duration   = ad_data.get('durationHours', 0)
            impressions = ad_data.get('impressions', 0)

            msg  = f"📢 {symbol_display} just bought a <b>DexScreener Ad</b>\n\n"
            if ad_type:
                msg += f"🎯 Type: <b>{ad_type}</b>\n"
            if duration:
                msg += f"⏱ Duration: <b>{duration}h</b>\n"
            if impressions:
                msg += f"👁 Impressions: <b>{impressions:,}</b>\n"
            msg += "\n"
            msg += f"Buy on telegram with <a href='{gmgn_url}'><b>GMGN bot</b></a>\n\n"
            msg += f"<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
            msg += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>"

            keyboard = _make_token_keyboard(mint, self._bot_username)
            logo_url = tracked.logo_url or await self.dexscreener_api._fetch_logo_for_mint(mint)
            logo_bytes = await self._normalize_logo(logo_url, symbol)

            await self.bot.send_photo(
                chat_id=self.channel_id,
                photo=logo_bytes,
                caption=msg,
                reply_markup=keyboard,
                parse_mode='HTML',
            )
            return True

        except Exception as e:
            logger.error(f"Error posting ads alert for {tracked.symbol}: {e}")
            return False

    # ── DexScreener CTO Alert ─────────────────────────────────────────────────

    async def _cto_check_loop(self):
        """Controlla ogni 5 minuti i community takeover su DexScreener."""
        while True:
            try:
                await self._check_dex_cto()
            except Exception as e:
                logger.error(f"CTO check loop error: {e}")
            await asyncio.sleep(300)

    async def _check_dex_cto(self):
        """Fetch community takeovers e controlla se sono tra i nostri tracked."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.dexscreener.com/community-takeovers/latest/v1",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

            if not isinstance(data, list):
                return

            sol_ctos = {
                item['tokenAddress']: item
                for item in data
                if item.get('chainId') == 'solana' and item.get('tokenAddress')
            }

            for mint, cto_data in sol_ctos.items():
                tracked = self.tracked.get(mint)
                if not tracked or tracked.dex_cto_posted:
                    continue
                posted = await self._post_dex_cto_alert(tracked, cto_data)
                if posted:
                    tracked.dex_cto_posted = True
                    db.update_tracked_token_field(mint, dex_cto_posted=1)
                    logger.info(f"🤝 CTO alert posted for {tracked.symbol}")

        except Exception as e:
            logger.warning(f"_check_dex_cto error: {e}")

    async def _post_dex_cto_alert(self, tracked: TrackedToken, cto_data: dict = {}) -> bool:
        """Posta il community takeover alert per un token già postato."""
        try:
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username

            mint   = tracked.mint
            symbol = tracked.symbol

            channel_username = config.CHANNEL_USERNAME.lstrip('@')
            original_post_link = (
                f"https://t.me/{channel_username}/{tracked.message_id}"
                if channel_username else None
            )
            symbol_display = (
                f"<a href='{original_post_link}'><b>{symbol}</b></a>"
                if original_post_link else f"<b>{symbol}</b>"
            )

            gmgn_url   = f"https://t.me/GMGN_sol_bot?start=i_bsVrj7Mc_c_{mint}"
            claim_date = cto_data.get('claimDate', '')
            description = cto_data.get('description', '')

            msg  = f"🤝 {symbol_display} just got a <b>Community Takeover</b>\n\n"
            if description:
                # tronca a 120 chars per non appesantire il messaggio
                desc_short = description[:120] + ('...' if len(description) > 120 else '')
                msg += f"💬 {desc_short}\n\n"
            if claim_date:
                msg += f"📅 Claimed: <b>{claim_date[:10]}</b>\n\n"
            msg += f"Buy on telegram with <a href='{gmgn_url}'><b>GMGN bot</b></a>\n\n"
            msg += f"<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
            msg += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>"

            keyboard = _make_token_keyboard(mint, self._bot_username)
            logo_url = tracked.logo_url or await self.dexscreener_api._fetch_logo_for_mint(mint)
            logo_bytes = await self._normalize_logo(logo_url, symbol)

            await self.bot.send_photo(
                chat_id=self.channel_id,
                photo=logo_bytes,
                caption=msg,
                reply_markup=keyboard,
                parse_mode='HTML',
            )
            return True

        except Exception as e:
            logger.error(f"Error posting CTO alert for {tracked.symbol}: {e}")
            return False

    # ── Dev Livestream Alert ──────────────────────────────────────────────────

    async def _update_original_post(self, tracked: TrackedToken, current_mc: float, milestone: float):
        try:
            mult = current_mc / tracked.initial_mc
            if mult < 2:
                label = f"+{int((mult-1)*100)}%"
                emoji = "📈"
            else:
                label = f"{int(mult)}x"
                emojis = {2:"🔥",3:"💥",4:"⚡️",5:"🚀",6:"🌕",7:"🌙",8:"💎",9:"👑"}
                emoji = emojis.get(int(mult), "🏆")

            gain_tag = "\u200b"  # zero-width space come marcatore invisibile
            gain_line = f"{emoji} <b>NOW AT {label}</b> | MC: ${current_mc:,.0f}{gain_tag}"

            # Base: la caption originale senza eventuali gain precedenti
            base = (tracked.original_caption or "").rstrip()
            # Rimuovi gain precedente se presente (tutto ciò che viene dopo il tag)
            if gain_tag in base:
                idx = base.rfind("\n\n")
                if idx > 0:
                    base = base[:idx]
            new_caption = base + "\n\n" + gain_line

            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username
            keyboard = _make_token_keyboard(tracked.mint, self._bot_username)

            from telegram.error import BadRequest
            edited = False
            try:
                await self.bot.edit_message_caption(
                    chat_id=self.channel_id,
                    message_id=tracked.message_id,
                    caption=new_caption,
                    reply_markup=keyboard,
                    parse_mode='HTML',
                )
                edited = True
            except BadRequest as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    return
                if "there is no caption" in err or "message can\'t be edited" in err or "not modified" in err:
                    pass  # prova text edit sotto
                else:
                    logger.debug(f"edit_caption error for {tracked.symbol}: {e}")
                    return

            if not edited:
                try:
                    await self.bot.edit_message_text(
                        chat_id=self.channel_id,
                        message_id=tracked.message_id,
                        text=new_caption,
                        reply_markup=keyboard,
                        parse_mode='HTML',
                        disable_web_page_preview=True,
                    )
                    edited = True
                except Exception as e2:
                    logger.debug(f"edit_text failed for {tracked.symbol}: {e2}")
                    return

            # Aggiorna in memoria e DB
            tracked.original_caption = new_caption
            db.update_tracked_token_field(tracked.mint, original_caption=new_caption)
            logger.info(f"✏️ Original post updated: {tracked.symbol} → {label}")

        except Exception as e:
            logger.debug(f"_update_original_post error for {tracked.symbol}: {e}")

    def _compute_consecutive_streak(self) -> int:
        """
        Calcola quante call CONSECUTIVE (nell'ordine di post) hanno fatto 2x.

        Logica: prende tutti i token tracked ordinati per posted_at.
        Partendo dall'ultimo postato, conta quanti hanno fatto 2x senza interruzione.
        Un token "interrompe" la streak se è stato postato DOPO il primo della streak
        ma NON ha ancora fatto 2x (anche se è ancora vivo/in tracking).
        Ignora i token postati PRIMA della prima call 2x della streak corrente.
        """
        if not self._streak:
            return 0

        # Mints che hanno fatto 2x
        hit_mints = {e['mint'] for e in self._streak}

        # Tutti i token tracked ordinati per posted_at (dal più vecchio al più recente)
        all_sorted = sorted(self.tracked.values(), key=lambda t: t.posted_at)

        if not all_sorted:
            return len(self._streak)

        # Trova il mint più vecchio nella streak
        streak_mints_with_ts = {e['mint']: e['ts'] for e in self._streak}
        earliest_streak_ts = min(
            t.posted_at for t in all_sorted if t.mint in hit_mints
        ) if any(t.mint in hit_mints for t in all_sorted) else 0

        # Considera solo i token postati da quel punto in poi
        relevant = [t for t in all_sorted if t.posted_at >= earliest_streak_ts]

        # Conta consecutivi dall'inizio dei relevant: ogni token deve essere in hit_mints
        consec = 0
        for t in relevant:
            if t.mint in hit_mints:
                consec += 1
            else:
                # Token postato dopo il primo 2x che non ha ancora fatto 2x → streak spezzata
                consec = 0

        return consec

    async def _post_streak_alert(self, consecutive: int = None):
        """Posta un messaggio separato quando c'è una streak di 2x+ consecutivi"""
        try:
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username
            buy_link = f"https://t.me/{self._bot_username}?start=buytrending"
            keyboard = [[InlineKeyboardButton("🚀 Buy Trending", url=buy_link)]]

            # Usa il count consecutivo reale, non la lunghezza totale della streak
            if consecutive is None:
                consecutive = self._compute_consecutive_streak()

            # Prendi solo i token consecutivi (gli ultimi N della streak ordinati per ts)
            streak_sorted = sorted(self._streak, key=lambda e: e.get('ts', 0))
            consec_entries = streak_sorted[-consecutive:] if consecutive <= len(streak_sorted) else streak_sorted

            from formatters.message_formatter import format_streak_alert
            message = format_streak_alert(consec_entries, consecutive)

            # Anti-doppioni basato su (consecutive_count, mints) non sul testo
            import hashlib
            key = f"streak:{consecutive}:{'|'.join(e['mint'] for e in consec_entries)}"
            msg_hash = hashlib.md5(key.encode()).hexdigest()
            if db.is_message_sent(msg_hash):
                return
            db.mark_message_sent(msg_hash)

            await self.bot.send_message(
                chat_id=self.channel_id,
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML',
                disable_web_page_preview=True,
            )
            logger.info(f"🔥 Streak alert posted: {consecutive} consecutive calls")
        except Exception as e:
            logger.error(f"Error posting streak alert: {e}")

    # ── Recap orario ──────────────────────────────────────

    async def _hourly_recap_loop(self):
        """Ogni 2 ore posta il recap"""
        await asyncio.sleep(120)  # attendi 2 minuti al boot prima del primo post
        while True:
            try:
                await self._post_hourly_recap()
            except Exception as e:
                logger.error(f"❌ Trending recap error: {str(e)}")
            await asyncio.sleep(7200)  # ogni 2 ore

    async def _post_hourly_recap(self):
        """
        Posta l'Hourly Recap ogni 2 ore: Last Hour, Call Statistics 24h,
        Top 24h Performers, All-Time Top.
        Usa last_notified_multiplier in memoria — zero API calls aggiuntive.
        """
        if not self.tracked:
            logger.info("⏰ Hourly recap: no tokens tracked")
            return

        import time as _time
        now = _time.time()

        # Costruisce entries con (tracked, approx_mc, multiplier)
        all_entries = []
        for mint, tracked in list(self.tracked.items()):
            if not tracked.initial_mc or tracked.initial_mc <= 0:
                continue
            multiplier = max(tracked.last_notified_multiplier, 1.0)
            approx_mc = tracked.initial_mc * multiplier
            all_entries.append((tracked, approx_mc, multiplier))

        results_1h  = [(t, mc, m) for t, mc, m in all_entries if t.posted_at >= now - 3600]
        results_24h = all_entries

        from formatters.message_formatter import format_trending_recap
        message = format_trending_recap(
            top10=[],
            results_1h=results_1h,
            results_24h=results_24h,
            top_performers=self._top_performers,
        )

        try:
            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username
            buy_link = f"https://t.me/{self._bot_username}?start=buytrending"
            keyboard = [[InlineKeyboardButton("🚀 Buy Trending", url=buy_link)]]

            import os as _os
            recap_img = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'assets', 'recap.png')
            sent = None
            try:
                with open(recap_img, 'rb') as _img:
                    sent = await self.bot.send_photo(
                        chat_id=self.channel_id,
                        photo=_img,
                        caption=message,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode='HTML',
                    )
            except Exception:
                pass
            if not sent:
                sent = await self.bot.send_message(
                    chat_id=self.channel_id,
                    text=message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                )

            logger.info(f"📊 Trending recap posted (msg_id={sent.message_id})")

        except Exception as e:
            logger.error(f"❌ Error posting trending recap: {str(e)}")

    async def _top10_loop(self):
        """Posta Top 10 ogni 1.5 ore."""
        await asyncio.sleep(300)  # attendi 5 min al boot
        while True:
            try:
                await self._post_top10()
            except Exception as e:
                logger.error(f"❌ top10_loop error: {e}")
            await asyncio.sleep(5400)  # ogni 1.5 ore

    async def _post_top10(self):
        """Posta il Top 10 Early Trending nel canale."""
        try:
            import time as _time
            now = _time.time()
            all_entries = []
            for mint, tracked in list(self.tracked.items()):
                if not tracked.initial_mc or tracked.initial_mc <= 0:
                    continue
                multiplier = max(tracked.last_notified_multiplier, 1.0)
                approx_mc = tracked.initial_mc * multiplier
                all_entries.append((tracked, approx_mc, multiplier))
            if not all_entries or len(all_entries) < 3:
                return

            message = format_top10_post(all_entries)
            if not message:
                return

            if not self._bot_username:
                bot_info = await self.bot.get_me()
                self._bot_username = bot_info.username
            buy_link = f"https://t.me/{self._bot_username}?start=buytrending"
            keyboard = [[InlineKeyboardButton("🚀 Buy Trending", url=buy_link)]]

            import os as _os
            top10_img = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'assets', 'top10.png')
            sent = None
            try:
                with open(top10_img, 'rb') as _img:
                    sent = await self.bot.send_photo(
                        chat_id=self.channel_id,
                        photo=_img,
                        caption=message,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode='HTML',
                    )
            except Exception:
                pass
            if not sent:
                sent = await self.bot.send_message(
                    chat_id=self.channel_id,
                    text=message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                )

            logger.info(f"🏆 Top10 posted (msg_id={sent.message_id})")

        except Exception as e:
            logger.error(f"❌ Error posting top10: {e}")
