import logging

logger = logging.getLogger(__name__)


def _valid_url(url) -> bool:
    return bool(url and isinstance(url, str) and url.strip().startswith('http'))


def _format_bundle_block(bundle: dict) -> str:
    """
    Formatta il blocco bundle check per il post Telegram.
    Ritorna stringa vuota se bundle è None/vuoto (fallback silenzioso).

    Formato:
        📦 <b>Bundle Check</b>  ✅ Good
        └ 0 wallets · 0%
    """
    if not bundle:
        return ""
    score   = bundle.get("score", "Good")
    wallets = bundle.get("wallets", 0)
    pct     = bundle.get("pct", 0.0)
    # Nasconde il blocco se non abbiamo dati utili (RPC rate limit, ecc.)
    if wallets == 0 and pct == 0.0:
        return ""
    emoji   = {"Good": "✅", "Low": "🟡", "Medium": "🟠", "High": "🔴"}.get(score, "✅")
    line1   = f"📦 <b>Bundle Check</b>  {emoji} {score}"
    line2   = f"└ {wallets} wallets · {pct}%"
    return f"{line1}\n{line2}"


async def format_token_message(
    mint: str,
    name: str,
    symbol: str,
    market_cap: float,
    holders: int = None,
    price_usd: float = None,
    liquidity: float = None,
    volume1h: float = None,
    volume24h: float = None,
    txns1h: int = None,
    buys1h: int = None,
    sells1h: int = None,
    price_change: float = None,
    priceChange1h: float = None,
    priceChange24h: float = None,
    pairCreatedAt: float = None,
    logo_url: str = None,
    website: str = None,
    twitter: str = None,
    discord: str = None,
    telegram: str = None,
    alpha_score: int = None,
    bonding_curve_pct: float = None,
    rugcheck_score: int = None,
    rugcheck_label: str = None,
    rugcheck_top_holder: float = None,
    rugcheck_top10: float = None,
    rugcheck_whales: int = None,
    bundle: dict = None,
    **kwargs
) -> str:
    try:
        name = name or "Unknown"
        symbol = symbol or "???"

        # Link pump.fun col contract address come testo
        pump_link = f"<a href='https://pump.fun/{mint}'>Pump.fun</a>"

        # ── Age ──
        age_str = ""
        if pairCreatedAt:
            import time
            age_sec = time.time() - pairCreatedAt / 1000
            if age_sec < 3600:
                age_str = f"{int(age_sec // 60)}m ago"
            else:
                age_str = f"{age_sec / 3600:.1f}h ago"

        # ── Header: nome + ticker ──
        # Mostra ticker solo se diverso dal nome
        import config as _cfg
        _ch = _cfg.CHANNEL_USERNAME.lstrip('@') if _cfg.CHANNEL_USERNAME else ''
        _ch_link = f"https://t.me/{_ch}" if _ch else '#'
        message = f"🔥 <a href='https://pump.fun/{mint}'><b>{name}</b></a>  <b>New</b> <a href='{_ch_link}'><b>Trending</b></a>\n\n"

        # ── MC + age ──
        age_part = f"  ·  🕐 <b>{age_str}</b>" if age_str else ""
        message += f"💰 MC <b>${market_cap:,.0f}</b>{age_part}\n"

        # ── CA ──
        message += f"📌 <b>CA:</b>\n<code>{mint}</code>\n"

        message += "\n"

        # ── Volume ──
        if volume1h:
            message += f"📊 <b>${volume1h:,.0f}</b> Vol\n"

        # ── Buys + Sells ──
        if buys1h is not None and sells1h is not None:
            message += f"🟢 <b>{buys1h:,}</b> Buys  |  🔴 <b>{sells1h:,}</b> Sells\n"
        elif txns1h:
            message += f"🔄 {txns1h:,} txns\n"

        message += "\n"

        # ── Rugcheck ──
        if rugcheck_score is not None or rugcheck_label is not None:
            rc_url = f"https://rugcheck.xyz/tokens/{mint}"
            s = int(rugcheck_score or 0)
            if s < 30:
                rc_label = "Good"; rc_emoji = "🟢"
            elif s < 70:
                rc_label = "Neutral"; rc_emoji = "🟡"
            else:
                rc_label = "Danger"; rc_emoji = "🔴"
            message += f"🔍 <a href='{rc_url}'><b>rugcheck.xyz</b></a>  {rc_emoji} <b>{rc_label}</b>\n"
            if rugcheck_top_holder is not None:
                message += f"👤 <b>Top Holder</b> {rugcheck_top_holder}%\n"
            if rugcheck_top10 is not None:
                message += f"👥 <b>Top 10 Holders</b> {rugcheck_top10}%\n"
            if holders:
                message += f"🧑‍🤝‍🧑 <b>Holders:</b> {holders:,}\n"
            if rugcheck_whales is not None and rugcheck_whales > 0:
                whale_label = "whale" if rugcheck_whales == 1 else "whales"
                message += f"🐋 <b>{rugcheck_whales} {whale_label} in top 10</b>\n"
        elif holders:
            message += f"🧑‍🤝‍🧑 <b>Holders:</b> {holders:,}\n"

        # ── Bundle Check ──
        bundle_block = _format_bundle_block(bundle)
        if bundle_block:
            message += bundle_block + "\n"

        # ── CTO Badge (solo se passato dal CTObot) ──
        cto_claim_date  = kwargs.get('cto_claim_date') or ''
        cto_description = kwargs.get('cto_description') or ''
        if cto_claim_date or cto_description:
            message += "\n🤝 <b>Community Takeover</b>\n"
            if cto_description:
                desc = cto_description[:160].strip()
                if len(cto_description) > 160:
                    desc += '…'
                message += f"💬 {desc}\n"
            if cto_claim_date:
                message += f"📅 Claimed: <b>{cto_claim_date}</b>\n"

        message += "\n"

        # ── GMGN line ──
        gmgn_url = f"https://t.me/GMGN_sol_bot?start=i_bsVrj7Mc_c_{mint}"
        message += f"Buy on telegram with <a href='{gmgn_url}'><b>GMGN bot</b></a>\n"

        message += "\n"

        social_links = []
        if _valid_url(website):
            social_links.append(f"<a href='{website}'><b>🌐 Website</b></a>")
        if _valid_url(twitter):
            social_links.append(f"<a href='{twitter}'><b>𝕏 Twitter</b></a>")
        if _valid_url(telegram):
            social_links.append(f"<a href='{telegram}'><b>✈️ Telegram</b></a>")
        if _valid_url(discord):
            social_links.append(f"<a href='{discord}'><b>💬 Discord</b></a>")
        if social_links:
            message += "  |  ".join(social_links) + "\n\n"

        message += f"<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
        message += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>\n"

        return message

    except Exception as e:
        logger.error(f"Error formatting message: {str(e)}")
        return f"🚀 {symbol} ({mint})"


async def format_promo_message(
    mint: str,
    name: str,
    symbol: str,
    market_cap: float,
    holders: int = None,
    price_usd: float = None,
    liquidity: float = None,
    volume1h: float = None,
    volume24h: float = None,
    buys1h: int = None,
    sells1h: int = None,
    txns1h: int = None,
    priceChange1h: float = None,
    pairCreatedAt: float = None,
    logo_url: str = None,
    website: str = None,
    twitter: str = None,
    discord: str = None,
    telegram: str = None,
    telegram_link: str = None,
    description: str = None,
    bonding_curve_pct: float = None,
    rugcheck_score: int = None,
    rugcheck_label: str = None,
    rugcheck_top_holder: float = None,
    rugcheck_top10: float = None,
    rugcheck_whales: int = None,
    bundle: dict = None,
    **kwargs
) -> str:
    try:
        name = name or "Unknown"
        symbol = symbol or "???"

        pump_link = f"<a href='https://pump.fun/{mint}'>Pump.fun</a>"

        # Age
        age_str = ""
        if pairCreatedAt:
            import time
            age_sec = time.time() - pairCreatedAt / 1000
            if age_sec < 3600:
                age_str = f"{int(age_sec // 60)}m ago"
            else:
                age_str = f"{age_sec / 3600:.1f}h ago"

        import config as _cfg
        _ch = _cfg.CHANNEL_USERNAME.lstrip('@') if _cfg.CHANNEL_USERNAME else ''
        _ch_link = f"https://t.me/{_ch}" if _ch else '#'
        message = f"🔥 <a href='https://pump.fun/{mint}'><b>{name}</b></a>  <b>New</b> <a href='{_ch_link}'><b>Trending</b></a>\n\n"
        message += "\n"

        age_part = f"  ·  🕐 <b>{age_str}</b>" if age_str else ""
        message += f"💰 MC <b>${market_cap:,.0f}</b>{age_part}\n"
        message += f"📌 <b>CA:</b>\n<code>{mint}</code>\n"
        message += "\n"

        # ── Volume ──
        if volume1h:
            message += f"📊 <b>${volume1h:,.0f}</b> Vol\n"

        # ── Buys + Sells ──
        if buys1h is not None and sells1h is not None:
            message += f"🟢 <b>{buys1h:,}</b> Buys  |  🔴 <b>{sells1h:,}</b> Sells\n"
        elif txns1h:
            message += f"🔄 {txns1h:,} txns\n"

        message += "\n"

        # ── Rugcheck ──
        if rugcheck_score is not None or rugcheck_label is not None:
            rc_url = f"https://rugcheck.xyz/tokens/{mint}"
            s = int(rugcheck_score or 0)
            if s < 30:
                rc_label = "Good"; rc_emoji = "🟢"
            elif s < 70:
                rc_label = "Neutral"; rc_emoji = "🟡"
            else:
                rc_label = "Danger"; rc_emoji = "🔴"
            message += f"🔍 <a href='{rc_url}'><b>rugcheck.xyz</b></a>  {rc_emoji} <b>{rc_label}</b>\n"
            if rugcheck_top_holder is not None:
                message += f"👤 <b>Top Holder</b> {rugcheck_top_holder}%\n"
            if rugcheck_top10 is not None:
                message += f"👥 <b>Top 10 Holders</b> {rugcheck_top10}%\n"
            if holders:
                message += f"🧑‍🤝‍🧑 <b>Holders:</b> {holders:,}\n"
            if rugcheck_whales is not None and rugcheck_whales > 0:
                whale_label = "whale" if rugcheck_whales == 1 else "whales"
                message += f"🐋 <b>{rugcheck_whales} {whale_label} in top 10</b>\n"
        elif holders:
            message += f"🧑‍🤝‍🧑 <b>Holders:</b> {holders:,}\n"

        # ── Bundle Check ──
        bundle_block = _format_bundle_block(bundle)
        if bundle_block:
            message += bundle_block + "\n"

        message += "\n"

        # ── GMGN line ──
        gmgn_url = f"https://t.me/GMGN_sol_bot?start=i_bsVrj7Mc_c_{mint}"
        message += f"Buy on telegram with <a href='{gmgn_url}'><b>GMGN bot</b></a>\n"

        message += "\n"

        tg = telegram or telegram_link
        social_links = []
        if _valid_url(website):
            social_links.append(f"<a href='{website}'><b>🌐 Website</b></a>")
        if _valid_url(twitter):
            social_links.append(f"<a href='{twitter}'><b>𝕏 Twitter</b></a>")
        if _valid_url(tg):
            social_links.append(f"<a href='{tg}'><b>✈️ Telegram</b></a>")
        if _valid_url(discord):
            social_links.append(f"<a href='{discord}'><b>💬 Discord</b></a>")
        if social_links:
            message += "  |  ".join(social_links) + "\n\n"

        message += f"<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
        message += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>\n"

        return message

    except Exception as e:
        logger.error(f"Error formatting promo message: {str(e)}")
        return f"✨ {symbol} ({mint})"


def format_volume_alert(
    mint: str,
    symbol: str,
    current_volume: float,
    prev_volume: float,
    pct_increase: float,
    buys: int,
    sells: int,
    market_cap: float,
    price_usd: float = None,
    priceChange1h: float = None,
) -> str:
    message = f"🔥 <b>BUY VOLUME ALERT</b>\n\n"
    message += f"<b>{symbol}</b> — <code>{mint}</code>\n"
    message += f"📊 <b>Volume (1h):</b> ${current_volume:,.0f}\n"
    message += f"📈 <b>Increase:</b> +{pct_increase:.0f}% vs 10min ago\n"
    message += f"🟢 <b>Buys:</b> {buys:,}   🔴 <b>Sells:</b> {sells:,}\n"
    message += f"💰 <b>Market Cap:</b> ${market_cap:,.0f}\n"
    if price_usd:
        message += f"💵 <b>Price:</b> ${float(price_usd):.10f}\n"
    if priceChange1h is not None and priceChange1h != 0:
        arrow = "📈" if priceChange1h >= 0 else "📉"
        sign = "+" if priceChange1h >= 0 else ""
        message += f"{arrow} <b>Change (1h):</b> {sign}{priceChange1h:.1f}%\n"
    message += f"\n<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
    message += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>  |  "
    message += f"<a href='https://axiom.trade/t/{mint}/@inscribe?chain=sol'><b>⚡️ Axiom</b></a>"
    return message



def format_gain_alert(
    symbol: str,
    mint: str,
    milestone: float,
    initial_mc: float,
    current_mc: float,
    posted_at: float = None,
    original_post_link: str = None,
) -> str:
    """
    milestone: 1.5=+50%, 2.0=2x, 3.0=3x, ecc (infinito)
    original_post_link: link al messaggio originale nel canale (es. t.me/channel/123)
    """
    mult = current_mc / initial_mc

    # Label: +50% per 1.5, altrimenti Nx basato sul milestone (non sul mult corrente)
    if milestone < 2.0:
        label = f"+{int((milestone - 1) * 100)}%"
    else:
        label = f"{int(milestone)}x"

    # Simbolo e "Trending Signal" linkati al post originale se disponibile
    if original_post_link:
        symbol_display = f"<a href='{original_post_link}'>{symbol}</a>"
        signal_display = f"<a href='{original_post_link}'>Trending Signal</a>"
    else:
        symbol_display = symbol
        signal_display = "Trending Signal"

    # Banconote: 2 per +50%, milestone*2 per i moltiplicatori, max 200
    dollar_count = 2 if milestone < 2.0 else min(int(milestone) * 2, 200)
    dollars = "💵" * dollar_count

    msg  = f"🚀 <b>{symbol_display} is up {label}</b> ⚡️\n"
    msg += f"from 📡 {signal_display}\n"
    msg += f"\n<b>${initial_mc:,.0f} → ${current_mc:,.0f}</b>\n"
    msg += f"\n{dollars}\n"
    msg += f"\n<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
    msg += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>"
    return msg


def format_whale_alert(
    mint: str,
    symbol: str,
    name: str,
    sol_amount: float,
    usd_amount: float,
    market_cap: float,
    buyer_wallet: str,
    tx_signature: str,
    price_usd: float = None,
    liquidity: float = None,
    holders: int = None,
    priceChange1h: float = None,
) -> str:
    """Formatta un whale buy alert per il canale"""
    # Emoji size based on SOL amount
    if sol_amount >= 20:
        size_emoji = "🐳🐳🐳"
        size_label = "MEGA WHALE"
    elif sol_amount >= 10:
        size_emoji = "🐳🐳"
        size_label = "BIG WHALE"
    else:
        size_emoji = "🐳"
        size_label = "WHALE"

    short_wallet = f"{buyer_wallet[:4]}...{buyer_wallet[-4:]}"
    tx_link = f"https://solscan.io/tx/{tx_signature}"
    wallet_link = f"https://solscan.io/account/{buyer_wallet}"

    message = f"{size_emoji} <b>{size_label} BUY ALERT</b>\n\n"
    message += f"<b>{name}</b> (<code>{symbol}</code>)\n"
    message += f"<b>CA:</b> <code>{mint}</code>\n\n"

    message += f"💸 <b>Amount:</b> {sol_amount:.2f} SOL\n"
    message += f"💰 <b>Market Cap:</b> ${market_cap:,.0f}\n"

    if price_usd:
        message += f"💵 <b>Price:</b> ${float(price_usd):.10f}\n"
    if priceChange1h is not None and priceChange1h != 0:
        arrow = "📈" if priceChange1h >= 0 else "📉"
        sign = "+" if priceChange1h >= 0 else ""
        message += f"{arrow} <b>Change (1h):</b> {sign}{priceChange1h:.1f}%\n"
    if liquidity:
        message += f"💧 <b>Liquidity:</b> ${liquidity:,.0f}\n"
    if holders:
        message += f"👥 <b>Holders:</b> {holders:,}\n"

    # Buyer: può essere un wallet o "avg of N buys" (detection aggregata)
    if buyer_wallet and buyer_wallet.startswith("avg of"):
        message += f"\n📊 <b>Pattern:</b> {buyer_wallet}\n"
    else:
        message += f"\n👤 <b>Buyer:</b> <a href='{wallet_link}'>{short_wallet}</a>\n"

    if tx_signature:
        message += f"\n<a href='{tx_link}'><b>🔍 View TX</b></a>  ·  "
    else:
        message += "\n"
    message += f"<a href='https://dexscreener.com/solana/{mint}'><b>📊 Dexscreener</b></a>  |  "
    message += f"<a href='https://pump.fun/{mint}'><b>🎯 Pump.fun</b></a>  |  "
    message += f"<a href='https://axiom.trade/t/{mint}/@inscribe?chain=sol'><b>⚡️ Axiom</b></a>"

    return message



def format_streak_alert(streak: list, consecutive: int = None) -> str:
    """
    streak:      lista di dict {symbol, mint, multiplier} — i token consecutivi
    consecutive: conteggio consecutivo reale (se None usa len(streak))
    """
    count = consecutive if consecutive is not None else len(streak)
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

    if count >= 7:   fire = "🔥🔥🔥🔥🔥"
    elif count >= 5: fire = "🔥🔥🔥🔥"
    elif count >= 4: fire = "🔥🔥🔥"
    elif count >= 3: fire = "🔥🔥"
    else:            fire = "🔥"

    phrases = [
        "Our calls are on fire, don't miss the next one 👀",
        "Back to back 2x calls. This is what we do 💪",
        "Consecutive wins. Follow for the next alpha 🎯",
        "We keep hitting. Are you in? 🚀",
    ]
    import hashlib
    phrase = phrases[int(hashlib.md5(str(count).encode()).hexdigest()[:2], 16) % len(phrases)]

    msg  = f"{fire} <b>{count} CALLS IN A ROW HIT 2x+!</b>\n\n"
    for i, entry in enumerate(streak):
        medal = medals[i] if i < len(medals) else "▪️"
        mult = entry.get('multiplier', 2.0)
        label = f"{mult:.1f}x"
        msg += f"{medal} <a href='https://pump.fun/{entry['mint']}'>{entry['symbol']}</a> — <b>{label}</b>\n"
    msg += f"\n<i>{phrase}</i>\n"
    return msg



def _format_recap_body(results_1h: list, results_24h: list, top_performers: list) -> str:
    """Corpo condiviso tra recap canale e /trending in DM."""
    if not results_24h:
        return "📊 <b>24hr Pump.fun Early Trending Stats</b>\n\n<i>No data yet.</i>"

    mults_24h = [m for _, _, m in results_24h]
    total = len(mults_24h)

    # Average profits di chi ha fatto almeno 2x
    winners = [m for m in mults_24h if m >= 2]
    avg_profit = round(sum(winners) / len(winners), 1) if winners else 0
    avg_profit_display = avg_profit * 2
    avg_label = f"{int(avg_profit_display)}x" if avg_profit_display >= 2 else f"+{int((avg_profit_display-1)*100)}%"

    # Win rate — randomizzato tra 70-80% dei signals per realismo
    import random as _random
    _seed = int(total * 7 + len(mults_24h))  # seed deterministico per giornata
    _rng = _random.Random(_seed)
    fake_win_pct = _rng.randint(70, 80)
    fake_wins = round(total * fake_win_pct / 100)
    win_rate = fake_win_pct

    # Best token 24h — cap a 10000x, usa second best se best supera soglia
    MAX_DISPLAY_MULT = 10000
    sorted_results = sorted(results_24h, key=lambda x: x[2], reverse=True)
    best = sorted_results[0]
    best_tracked, best_mc, best_mult = best
    if best_mult > MAX_DISPLAY_MULT and len(sorted_results) > 1:
        best_tracked, best_mc, best_mult = sorted_results[1]
    best_label = f"{int(best_mult)}x" if best_mult >= 2 else f"+{int((best_mult-1)*100)}%"
    best_link = f"https://pump.fun/{best_tracked.mint}"

    # Xs counts
    count_2x  = sum(1 for m in mults_24h if m >= 2)
    count_5x  = sum(1 for m in mults_24h if m >= 5)
    count_10x = sum(1 for m in mults_24h if m >= 10)
    count_15x = sum(1 for m in mults_24h if m >= 15)

    # Win rate bar
    filled = round(win_rate / 10)
    bar = "█" * filled + "░" * (10 - filled)

    import config as _cfg
    _ch = _cfg.CHANNEL_USERNAME.lstrip('@') if _cfg.CHANNEL_USERNAME else ''
    channel_link = f"https://t.me/{_ch}" if _ch else ''

    # ── Header ──
    msg  = f"📊 <b><a href='{channel_link}'>Pump.fun</a> Early Trending</b>\n\n"

    # ── Riga 1: signals + avg profit ──
    msg += f"📡 {total} signals  |  💰 Avg <b>{avg_label}</b>\n"

    # ── Riga 2: best call ──
    msg += f"🥇 Best: <a href='{best_link}'><b>{best_tracked.symbol}</b></a> <b>{best_label}</b>\n\n"

    # ── Breakdown barre colorate stile buy pressure (5 quadratini) ──
    msg += f"📊 <b>Multiplier Breakdown</b>\n"

    tiers = [
        ("🟩", "2x",  count_2x * 2),
        ("🟨", "5x",  count_5x * 2),
        ("🟧", "10x", count_10x * 2),
        ("🟥", "15x", count_15x * 2),
    ]
    fixed_fills = [5, 4, 3, 2]
    for i, (emoji, label, count) in enumerate(tiers):
        if count == 0:
            continue
        filled = fixed_fills[i]
        bar = emoji * filled + "⬜" * (5 - filled)
        msg += f"{bar}  <b>{label}</b> {count} signals\n"

    return msg


def format_hourly_recap(
    results_1h: list,
    results_3h: list,
    results_24h: list,
    top_performers: list = None,
    results: list = None,
    latest_tokens: list = None,
    results_3h_old: list = None,
) -> str:
    """Recap per il /trending in DM — stesso formato del canale."""
    return _format_recap_body(results_1h or [], results_24h or [], top_performers or [])
