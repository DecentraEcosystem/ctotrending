import aiohttp
import asyncio
import logging
import time
from typing import Optional, Dict, List
import config

logger = logging.getLogger(__name__)

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Account noti da escludere
KNOWN_PROGRAMS = {
    PUMP_PROGRAM_ID,
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bRS",
    "SysvarRent111111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
    "SysvarC1ock11111111111111111111111111111111",
    "So11111111111111111111111111111111111111112",
    "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5zePzkDJhWnAJ2",  # pump fee account
    "CebN5WGQ4jvEPvsVU4EoHEpgznyQOinZsGLEU8LmjPqj", # pump fee account 2
}


class DexscreenerAPI:
    """
    Cerca token pump.fun creati nell'ultima ora con MC nel range.
    Fonte primaria: Helius per le tx recenti, DexScreener per MC/prezzo.
    Logo: pump.fun API → Helius getAsset → DexScreener (cascade).
    """

    def __init__(self):
        self.helius_key = config.HELIUS_API_KEY  # usato solo per getAsset logo fallback
        self.helius_rpc = f"https://mainnet.helius-rpc.com/?api-key={self.helius_key}"
        self.dex_base = "https://api.dexscreener.com/latest/dex"
        self._socials_cache = {}  # mint -> {twitter, telegram, website} da getAsset

    # ──────────────────────────────────────────────────────
    # Lookup singolo token (flusso promozione utente)
    # ──────────────────────────────────────────────────────

    async def get_token_data(self, mint: str) -> Optional[Dict]:
        """Cerca dati token su Dexscreener per CA specifica"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.dex_base}/tokens/{mint}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('pairs'):
                            pairs = data['pairs']
                            # Preferisci il pair pump.fun, fallback al primo
                            pair = next((p for p in pairs if p.get('dexId') == 'pump'), pairs[0])
                            token = self._parse_pair(pair, mint)
                            # Se DexScreener non ha il logo, cerca su pump.fun con sessione dedicata
                            if not token.get('logo') or not str(token.get('logo', '')).startswith('http'):
                                token['logo'] = await self._fetch_logo_for_mint(mint)
                            # Sempre fetch socials da pump.fun e merge
                            pump_socials = await self._fetch_socials_from_pump(session, mint)
                            for k, v in pump_socials.items():
                                if v and not token.get(k):
                                    token[k] = v
                            return token
                    logger.warning(f"No data for {mint}: status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching token data: {str(e)}")
            return None

    # ──────────────────────────────────────────────────────
    # Monitoring principale
    # ──────────────────────────────────────────────────────

    async def search_trending_tokens(self,
                                     min_market_cap: int,
                                     max_market_cap: int,
                                     limit: int = 50) -> List[Dict]:
        """
        Discovery via Helius Enhanced API.
        Le tx Helius contengono già tokenTransfers con amount → contiamo buy/sell per mint
        direttamente dalle tx, senza bisogno di DexScreener.
        MC check: pump.fun singolo (real-time, zero ritardo indicizzazione).
        """
        try:
            now = int(time.time())
            since = now - (config.TOKEN_AGE_HOURS * 3600)

            # ── Step 1: scarica tx Helius e accumula stats per mint ──────────
            mint_stats: dict = {}   # mint → {buys, sells, volume_sol, first_seen}
            last_sig = None
            MAX_PAGES = 10

            async with aiohttp.ClientSession() as session:
                for page in range(MAX_PAGES):
                    url = (
                        f"https://api.helius.xyz/v0/addresses/{PUMP_PROGRAM_ID}/transactions"
                        f"?api-key={self.helius_key}&limit=100"
                    )
                    if last_sig:
                        url += f"&before={last_sig}"

                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status != 200:
                            logger.error(f"Helius API error: {resp.status}")
                            break
                        transactions = await resp.json()
                        if not transactions:
                            break

                        logger.info(f"Helius page {page+1}: {len(transactions)} tx")
                        oldest_ts = None

                        for tx in transactions:
                            tx_time = tx.get('timestamp', 0)
                            if oldest_ts is None or tx_time < oldest_ts:
                                oldest_ts = tx_time
                            if tx_time < since:
                                continue

                            tx_type = (tx.get('type') or '').upper()  # SWAP, TOKEN_MINT, ecc.

                            # Raccogli tutti i mint pump in questa tx
                            tx_mints = set()
                            for transfer in tx.get('tokenTransfers', []):
                                m = transfer.get('mint', '')
                                if m and m not in KNOWN_PROGRAMS and len(m) in (43, 44) and m.endswith('pump'):
                                    tx_mints.add(m)
                            for acc in tx.get('accountData', []):
                                for tb in acc.get('tokenBalanceChanges', []):
                                    m = tb.get('mint', '')
                                    if m and m not in KNOWN_PROGRAMS and len(m) in (43, 44) and m.endswith('pump'):
                                        tx_mints.add(m)

                            for mint in tx_mints:
                                if mint not in mint_stats:
                                    mint_stats[mint] = {'buys': 0, 'sells': 0, 'volume_sol': 0.0, 'first_seen': tx_time}
                                s = mint_stats[mint]
                                s['first_seen'] = min(s['first_seen'], tx_time)

                                # Classifica buy/sell dalla nativeTransfers (SOL flow)
                                sol_in = sum(
                                    t.get('amount', 0) for t in tx.get('nativeTransfers', [])
                                    if t.get('toUserAccount') in (PUMP_PROGRAM_ID, '4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5zePzkDJhWnAJ2')
                                ) / 1e9
                                sol_out = sum(
                                    t.get('amount', 0) for t in tx.get('nativeTransfers', [])
                                    if t.get('fromUserAccount') in (PUMP_PROGRAM_ID, '4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5zePzkDJhWnAJ2')
                                ) / 1e9

                                if tx_type in ('SWAP',) or sol_in > 0:
                                    if sol_in >= sol_out:
                                        s['buys'] += 1
                                        s['volume_sol'] += sol_in
                                    else:
                                        s['sells'] += 1
                                        s['volume_sol'] += sol_out
                                else:
                                    # tx non classificabile — conta come buy (creazione, ecc.)
                                    s['buys'] += 1

                        last_sig = transactions[-1].get('signature')
                        if oldest_ts and oldest_ts < since:
                            break
                        if len(transactions) < 100:
                            break
                        await asyncio.sleep(0.2)

            if not mint_stats:
                logger.info("Helius: nessun mint trovato")
                return []

            logger.info(f"Helius: {len(mint_stats)} mint con attività nell'ultima {config.TOKEN_AGE_HOURS}h")

            # ── Pre-filtro attività minima ────────────────────────────────────
            # Scarta mint con meno di 5 tx totali — token appena creati o morti
            # senza attività reale. Riduce drasticamente i fetch inutili.
            MIN_TXS = 5
            before = len(mint_stats)
            mint_stats = {
                m: s for m, s in mint_stats.items()
                if (s['buys'] + s['sells']) >= MIN_TXS
            }
            logger.info(f"Pre-filtro attività: {before} → {len(mint_stats)} mint (min {MIN_TXS} tx)")

            all_mints = list(mint_stats.keys())

            # ── Step 2: getAssetBatch Helius — 1 chiamata per tutti i mint ──
            asset_map: dict = {}  # mint → asset data
            async with aiohttp.ClientSession() as session:
                for i in range(0, len(all_mints), 100):
                    batch = all_mints[i:i+100]
                    try:
                        async with session.post(
                            self.helius_rpc,
                            json={"jsonrpc": "2.0", "id": "batch", "method": "getAssetBatch",
                                  "params": {"ids": batch}},
                            timeout=aiohttp.ClientTimeout(total=15),
                        ) as r:
                            if r.status == 200:
                                data = await r.json()
                                for asset in (data.get('result') or []):
                                    if not asset:
                                        continue
                                    m = asset.get('id', '')
                                    if m:
                                        asset_map[m] = asset
                    except Exception as e:
                        logger.debug(f"getAssetBatch error: {e}")

                # ── Step 2b: DexScreener batch per MC (fallback e arricchimento) ──
                dex_mc_map: dict = {}  # mint → {mc, volume1h, buys1h, sells1h, ...}
                for i in range(0, len(all_mints), 30):
                    batch = all_mints[i:i+30]
                    try:
                        async with session.get(
                            f"{self.dex_base}/tokens/{','.join(batch)}",
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            if r.status == 200:
                                data = await r.json()
                                for pair in (data.get('pairs') or []):
                                    m = pair.get('baseToken', {}).get('address', '')
                                    if m and m not in dex_mc_map:
                                        dex_mc_map[m] = {
                                            'mc': float(pair.get('marketCap', 0) or 0),
                                            'volume1h': float(pair.get('volume', {}).get('h1', 0) or 0),
                                            'buys1h': int(pair.get('txns', {}).get('h1', {}).get('buys', 0) or 0),
                                            'sells1h': int(pair.get('txns', {}).get('h1', {}).get('sells', 0) or 0),
                                            'priceChange1h': float(pair.get('priceChange', {}).get('h1', 0) or 0),
                                            'liquidity': float(pair.get('liquidity', {}).get('usd', 0) or 0),
                                            'priceUsd': float(pair.get('priceUsd', 0) or 0),
                                            'pairCreatedAt': pair.get('pairCreatedAt', 0),
                                            'logo': (pair.get('info', {}) or {}).get('imageUrl', '') or '',
                                        }
                    except Exception as e:
                        logger.debug(f"DexScreener batch mc error: {e}")
                    await asyncio.sleep(0.1)

            # ── Step 2c: pump.fun API fallback per MC sui mint non trovati da DexScreener ──
            # Token freschi spesso non sono ancora indicizzati da DexScreener — pump.fun
            # ha sempre il MC real-time dalla bonding curve.
            missing_mc = [m for m in all_mints if not dex_mc_map.get(m, {}).get('mc')]
            if missing_mc:
                logger.info(f"🔍 Fetching pump.fun MC for {len(missing_mc)} mint not on DexScreener")
                async with aiohttp.ClientSession() as session:
                    sem = asyncio.Semaphore(10)
                    async def _pf_mc(mint):
                        async with sem:
                            try:
                                async with session.get(
                                    f"https://frontend-api.pump.fun/coins/{mint}",
                                    timeout=aiohttp.ClientTimeout(total=4),
                                    headers={"User-Agent": "Mozilla/5.0"},
                                ) as r:
                                    if r.status == 200:
                                        d = await r.json(content_type=None)
                                        usd_mc = float(d.get('usd_market_cap', 0) or 0)
                                        if usd_mc > 0:
                                            return mint, usd_mc
                            except Exception:
                                pass
                            return mint, 0.0
                    pf_results = await asyncio.gather(*[_pf_mc(m) for m in missing_mc])
                    for _mint, _mc in pf_results:
                        if _mc > 0 and _mint not in dex_mc_map:
                            dex_mc_map[_mint] = {'mc': _mc, 'volume1h': 0, 'buys1h': 0, 'sells1h': 0,
                                                  'priceChange1h': 0, 'liquidity': 0, 'priceUsd': 0,
                                                  'pairCreatedAt': 0, 'logo': ''}
                            logger.info(f"  🟡 pump.fun MC fallback: {_mint[:8]} MC=${_mc:,.0f}")

            # ── Step 3: merge e filtra per MC ────────────────────────────────
            matching = []
            sol_px = 150.0  # fallback SOL price per stima volume

            # Counters per debug
            cnt_no_mc = 0
            cnt_out_of_range = 0

            for mint in all_mints:
                asset  = asset_map.get(mint, {})
                dex    = dex_mc_map.get(mint, {})
                stats  = mint_stats.get(mint, {})

                # MC: DexScreener prima (più affidabile se presente), poi Helius getAsset
                mc = dex.get('mc', 0.0)
                if not mc and asset:
                    token_info = asset.get('token_info', {}) or {}
                    supply     = float(token_info.get('supply', 0) or 0)
                    decimals   = int(token_info.get('decimals', 6) or 6)
                    price_usd  = float((token_info.get('price_info') or {}).get('price_per_token', 0) or 0)
                    if supply > 0 and price_usd > 0:
                        mc = (supply / (10 ** decimals)) * price_usd

                if mc <= 0:
                    cnt_no_mc += 1
                    continue
                if not (min_market_cap <= mc <= max_market_cap):
                    cnt_out_of_range += 1
                    logger.info(f"  ⏭ MC out of range: {mint[:8]} MC=${mc:,.0f} (range ${min_market_cap:,}-${max_market_cap:,})")
                    continue

                # Metadata da getAsset
                meta   = (asset.get('content', {}) or {}).get('metadata', {}) or {}
                ti     = asset.get('token_info', {}) or {}
                symbol = meta.get('symbol') or ti.get('symbol') or mint[:8]
                name   = meta.get('name') or symbol
                files  = (asset.get('content', {}) or {}).get('files', []) or []
                logo   = (
                    next((f.get('uri','') for f in files if (f.get('mime','') or '').startswith('image')), '') or
                    (asset.get('content', {}) or {}).get('links', {}).get('image', '') or
                    dex.get('logo', '')
                )
                created_ms = dex.get('pairCreatedAt') or int(asset.get('created_at', 0) or 0) * 1000
                price_usd  = dex.get('priceUsd') or float((ti.get('price_info') or {}).get('price_per_token', 0) or 0)
                links      = (asset.get('content', {}) or {}).get('links', {}) or {}

                # Volume/buys: DexScreener se disponibile, altrimenti stima da Helius tx
                buys     = dex.get('buys1h') or stats.get('buys', 0)
                sells    = dex.get('sells1h') or stats.get('sells', 0)
                sells_from_dex = bool(dex.get('sells1h'))
                vol1h    = dex.get('volume1h') or stats.get('volume_sol', 0.0) * sol_px
                pc1h     = dex.get('priceChange1h', 0)
                liq      = dex.get('liquidity', 0)

                token = {
                    'mint': mint,
                    'baseToken': {'address': mint, 'symbol': symbol, 'name': name},
                    'marketCap': mc,
                    'logo': logo,
                    'priceUsd': price_usd,
                    'liquidity': liq,
                    'volume1h': vol1h,
                    'volume24h': vol1h,
                    'txns1h': buys + sells,
                    'sells_from_dex': sells_from_dex,
                    'buys1h': buys,
                    'sells1h': sells,
                    'priceChange1h': pc1h,
                    'priceChange24h': pc1h,
                    'holders': None,
                    'pairCreatedAt': created_ms,
                    'website': links.get('external_url'),
                    'twitter': None,
                    'telegram': None,
                    'discord': None,
                    'info': {},
                }
                matching.append(token)
                logger.info(f"  ✅ MATCH: {symbol} MC=${mc:,.0f} buys={buys} sells={sells} vol=${vol1h:,.0f}")

            logger.info(f"✅ {len(matching)} tokens match MC ${min_market_cap:,}-${max_market_cap:,} | no_mc={cnt_no_mc} out_of_range={cnt_out_of_range}")
            return matching[:limit]

        except Exception as e:
            logger.error(f"Error in search_trending_tokens: {str(e)}")
            return []

    async def _enrich_with_dexscreener(self, session_hint, tokens: list) -> None:
        """
        Arricchisce i token trovati con volume/buys/sells da DexScreener in batch.
        Non blocca: se DexScreener non ha ancora il token, rimane con dati pump.fun.
        """
        if not tokens:
            return
        mints = [t['mint'] for t in tokens if t.get('mint')]
        if not mints:
            return

        mint_to_token = {t['mint']: t for t in tokens}

        try:
            async with aiohttp.ClientSession() as session:
                for i in range(0, len(mints), 30):
                    batch = mints[i:i+30]
                    url = f"{self.dex_base}/tokens/{','.join(batch)}"
                    try:
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                            if r.status != 200:
                                continue
                            data = await r.json()
                            for pair in data.get('pairs', []):
                                m = pair.get('baseToken', {}).get('address', '')
                                if m not in mint_to_token:
                                    continue
                                t = mint_to_token[m]
                                t['volume1h']     = float(pair.get('volume', {}).get('h1', 0) or 0)
                                t['volume24h']    = float(pair.get('volume', {}).get('h24', 0) or 0)
                                t['buys1h']       = int(pair.get('txns', {}).get('h1', {}).get('buys', 0) or 0)
                                t['sells1h']      = int(pair.get('txns', {}).get('h1', {}).get('sells', 0) or 0)
                                t['txns1h']       = t['buys1h'] + t['sells1h']
                                t['priceChange1h']  = float(pair.get('priceChange', {}).get('h1', 0) or 0)
                                t['priceChange24h'] = float(pair.get('priceChange', {}).get('h24', 0) or 0)
                                t['liquidity']    = float(pair.get('liquidity', {}).get('usd', 0) or 0)
                                if not t.get('logo'):
                                    info = pair.get('info', {}) or {}
                                    t['logo'] = (info.get('imageUrl') or '').strip() or None
                    except Exception as e:
                        logger.debug(f"DexScreener enrich batch {i//30}: {e}")
        except Exception as e:
            logger.debug(f"_enrich_with_dexscreener error: {e}")

    async def _get_new_pump_mints_helius(self, since_timestamp: int) -> List[str]:
        """
        Usa Helius Enhanced Transactions API per trovare le transazioni recenti
        del programma pump.fun ed estrarne i mint address.
        Pagina fino a coprire tutto il window since_timestamp.
        Ritorna i mint in ordine cronologico inverso (più recenti prima).
        """
        try:
            seen = set()
            mints = []
            last_sig = None
            MAX_PAGES = 10  # 1000 tx per ciclo

            async with aiohttp.ClientSession() as session:
                for page in range(MAX_PAGES):
                    url = (
                        f"https://api.helius.xyz/v0/addresses/{PUMP_PROGRAM_ID}/transactions"
                        f"?api-key={self.helius_key}&limit=100"
                    )
                    if last_sig:
                        url += f"&before={last_sig}"

                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            logger.error(f"Helius API error: {text[:200]}")
                            break

                        transactions = await resp.json()
                        if not transactions:
                            break

                        logger.info(f"Helius page {page+1}: {len(transactions)} tx ricevute")

                        oldest_ts = None
                        for tx in transactions:
                            tx_time = tx.get('timestamp', 0)
                            if oldest_ts is None or tx_time < oldest_ts:
                                oldest_ts = tx_time

                            if tx_time < since_timestamp:
                                continue

                            # Estrai mint da tokenTransfers — solo token pump.fun (mint finisce con 'pump')
                            for transfer in tx.get('tokenTransfers', []):
                                mint = transfer.get('mint')
                                if mint and mint not in KNOWN_PROGRAMS and len(mint) in (43, 44) and mint.endswith('pump') and mint not in seen:
                                    seen.add(mint)
                                    mints.append(mint)

                            # Estrai mint da accountData.tokenBalanceChanges
                            for acc in tx.get('accountData', []):
                                for tb in acc.get('tokenBalanceChanges', []):
                                    mint = tb.get('mint')
                                    if mint and mint not in KNOWN_PROGRAMS and len(mint) in (43, 44) and mint.endswith('pump') and mint not in seen:
                                        seen.add(mint)
                                        mints.append(mint)

                        # Aggiorna cursore per prossima pagina
                        last_sig = transactions[-1].get('signature')

                        # Stop se abbiamo coperto tutto il window
                        if oldest_ts and oldest_ts < since_timestamp:
                            logger.info(f"Helius: raggiunto since_timestamp alla pagina {page+1}")
                            break

                        # Stop se meno di 100 tx (ultima pagina)
                        if len(transactions) < 100:
                            break

                        await asyncio.sleep(0.2)  # rate limit gentile

            elapsed_min = (int(time.time()) - since_timestamp) // 60
            logger.info(f"Helius: {len(mints)} mint unici negli ultimi {elapsed_min}min")
            return mints

        except Exception as e:
            logger.error(f"Helius API error: {str(e)}")
            return []

    async def _check_dex_market_cap(
        self,
        session: aiohttp.ClientSession,
        mint: str,
        min_mc: int,
        max_mc: int
    ) -> Optional[Dict]:
        """
        Controlla MC per un mint usando Helius getAsset — già funziona su Railway.
        pump.fun è bloccato da Railway quindi non lo chiamiamo qui.
        MC viene calcolato dalla bonding curve on-chain.
        """
        try:
            async with session.post(
                self.helius_rpc,
                json={"jsonrpc": "2.0", "id": "mc", "method": "getAsset", "params": {"id": mint}},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()

            asset = data.get('result', {})
            if not asset:
                return None

            # MC da token_info se disponibile
            token_info = asset.get('token_info', {}) or {}
            supply     = float(token_info.get('supply', 0) or 0)
            decimals   = int(token_info.get('decimals', 6) or 6)
            price_info = token_info.get('price_info', {}) or {}
            price_usd  = float(price_info.get('price_per_token', 0) or 0)

            mc = 0.0
            if supply > 0 and price_usd > 0:
                mc = (supply / (10 ** decimals)) * price_usd

            if mc <= 0 or not (min_mc <= mc <= max_mc):
                return None

            meta    = asset.get('content', {}).get('metadata', {}) or {}
            symbol  = meta.get('symbol') or token_info.get('symbol') or mint[:8]
            name    = meta.get('name') or symbol
            files   = asset.get('content', {}).get('files', []) or []
            logo    = next((f.get('uri', '') for f in files if f.get('mime', '').startswith('image')), '')
            if not logo:
                logo = asset.get('content', {}).get('links', {}).get('image', '') or ''

            created_ms = int(asset.get('created_at', 0) or 0) * 1000

            # Socials da authorities/links
            links = asset.get('content', {}).get('links', {}) or {}

            logger.info(f"  ✅ {mint[:8]} {symbol} MC=${mc:,.0f}")
            return {
                'mint': mint,
                'baseToken': {'address': mint, 'symbol': symbol, 'name': name},
                'marketCap': mc,
                'logo': logo,
                'priceUsd': price_usd,
                'liquidity': 0,
                'volume1h': 0,
                'volume24h': 0,
                'txns1h': 0,
                'buys1h': 0,
                'sells1h': 0,
                'priceChange1h': 0,
                'priceChange24h': 0,
                'holders': None,
                'pairCreatedAt': created_ms,
                'website': links.get('external_url'),
                'twitter': None,
                'telegram': None,
                'discord': None,
                'info': {},
            }

        except Exception as e:
            logger.debug(f"_check_dex_market_cap error {mint[:8]}: {e}")
            return None

    async def _enrich_with_helius_batch(self, mints: list) -> None:
        """
        Fetcha nome/simbolo/logo per tutti i mint in UNA sola chiamata Helius getAssetBatch.
        Risultati salvati in self._asset_cache per uso in _check_dex_market_cap.
        Risparmia N chiamate pump.fun/logo singole.
        """
        if not mints or not self.helius_key:
            return
        if not hasattr(self, '_asset_cache'):
            self._asset_cache = {}
        try:
            async with aiohttp.ClientSession() as session:
                # getAssetBatch supporta fino a 1000 mint per chiamata
                batch_size = 1000
                for i in range(0, len(mints), batch_size):
                    batch = mints[i:i + batch_size]
                    async with session.post(
                        self.helius_rpc,
                        json={
                            "jsonrpc": "2.0", "id": "batch",
                            "method": "getAssetBatch",
                            "params": {"ids": batch}
                        },
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                        for asset in (data.get('result') or []):
                            if not asset:
                                continue
                            mint = asset.get('id', '')
                            content = asset.get('content', {})
                            meta    = content.get('metadata', {})
                            links   = content.get('links', {})
                            symbol  = meta.get('symbol', '')
                            name    = meta.get('name', '')
                            logo    = links.get('image', '') or ''
                            if logo:
                                logo = logo.replace('ipfs://', 'https://pump.mypinata.cloud/ipfs/')
                            self._asset_cache[mint] = {
                                'symbol': symbol,
                                'name': name,
                                'logo': logo if logo.startswith('http') else '',
                            }
                logger.info(f"🗂 Helius batch: {len(self._asset_cache)} assets cached")
        except Exception as e:
            logger.debug(f"Helius getAssetBatch error: {e}")

    async def _check_gecko(
        self,
        session: aiohttp.ClientSession,
        mint: str,
        min_mc: int,
        max_mc: int,
        pf_mc: float = 0,
    ) -> Optional[Dict]:
        """
        Fallback quando DexScreener non ha ancora il token (token freschi < 2 min).
        GeckoTerminal è pubblico, no API key richiesta.
        Endpoint: https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}
        """
        try:
            async with session.get(
                f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}",
                headers={"Accept": "application/json;version=20230302"},
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status != 200:
                    logger.info(f"  ⏭ GeckoTerminal miss {mint[:8]}: status {resp.status}")
                    return None
                body = await resp.json()

            attrs = body.get('data', {}).get('attributes', {})
            if not attrs:
                return None

            # MC: usa pump.fun se già disponibile, altrimenti GeckoTerminal
            gt_mc = float(attrs.get('market_cap_usd') or attrs.get('fdv_usd') or 0)
            mc = pf_mc if pf_mc > 0 else gt_mc
            if not mc or not (min_mc <= mc <= max_mc):
                return None

            # GeckoTerminal non ha buys/sells 1h diretti nel token endpoint —
            # serve il top_pool per i trade stats
            pool_address = None
            try:
                incl = body.get('included', [])
                for item in incl:
                    if item.get('type') == 'pool':
                        pool_address = item.get('attributes', {}).get('address')
                        break
                # Se non c'è included, cerco il top pool separatamente
                if not pool_address:
                    async with session.get(
                        f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/pools?page=1",
                        headers={"Accept": "application/json;version=20230302"},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as pr:
                        if pr.status == 200:
                            pdata = await pr.json()
                            pools = pdata.get('data', [])
                            if pools:
                                pool_address = pools[0].get('attributes', {}).get('address')
            except Exception:
                pass

            buys1h = 0
            sells1h = 0
            vol1h = 0.0

            if pool_address:
                try:
                    async with session.get(
                        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}",
                        headers={"Accept": "application/json;version=20230302"},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as pr:
                        if pr.status == 200:
                            pattrs = (await pr.json()).get('data', {}).get('attributes', {})
                            txns = pattrs.get('transactions', {})
                            buys1h  = int(txns.get('h1', {}).get('buys',  0) or 0)
                            sells1h = int(txns.get('h1', {}).get('sells', 0) or 0)
                            vol1h   = float(pattrs.get('volume_usd', {}).get('h1', 0) or 0)
                except Exception:
                    pass

            name   = attrs.get('name', 'Unknown')
            symbol = attrs.get('symbol', 'UNKNOWN')
            logo   = attrs.get('image_url') or ''
            price  = float(attrs.get('price_usd') or 0)
            vol24h = float(attrs.get('volume_usd') or 0)
            liq    = float(attrs.get('total_reserve_in_usd') or 0)

            logger.info(f"  ✅ GeckoTerminal MATCH: {symbol} MC=${mc:,.0f} buys={buys1h} sells={sells1h}")
            return {
                'mint': mint,
                'baseToken': {'name': name, 'symbol': symbol},
                'marketCap': mc,
                'priceUsd': price,
                'liquidity': liq,
                'volume1h': vol1h,
                'volume24h': vol24h,
                'txns1h': total_txns,
                'buys1h': buys1h,
                'sells1h': sells1h,
                'priceChange1h': 0.0,
                'priceChange24h': 0.0,
                'holders': None,
                'pairCreatedAt': None,
                'logo': logo if logo.startswith('http') else None,
                'website': None,
                'twitter': None,
                'telegram': None,
                'discord': None,
                'info': {},
            }

        except Exception as e:
            logger.debug(f"GeckoTerminal error {mint[:8]}: {e}")
            return None

    async def _fetch_logo_for_mint(self, mint: str) -> Optional[str]:
        """Wrapper pubblico: apre sessione e chiama _fetch_logo."""
        try:
            async with aiohttp.ClientSession() as session:
                return await self._fetch_logo(session, mint)
        except Exception:
            return None

    async def _fetch_logo(self, session: aiohttp.ClientSession, mint: str) -> Optional[str]:
        """
        Catena di fallback per trovare il logo del token (ordine ottimizzato per crediti):
        1. DexScreener token info (gratis)
        2. pump.fun API (gratis)
        3. Helius getAsset (solo se i precedenti falliscono — consuma crediti)
        """
        # 1. DexScreener — già disponibile, zero costo
        try:
            async with session.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for pair in data.get('pairs', []):
                        img = (
                            (pair.get('info', {}).get('imageUrl') or '').strip() or
                            (pair.get('info', {}).get('icon') or '').strip() or
                            (pair.get('info', {}).get('header') or '').strip()
                        )
                        if img and img.startswith('http'):
                            logger.info(f"🖼 Logo via DexScreener: {mint[:8]}")
                            return img
        except Exception as e:
            logger.debug(f"🖼 DexScreener error for {mint[:8]}: {e}")

        # 2. pump.fun API — gratis, nessun API key (endpoint aggiornati)
        for url in [
            f"https://api.pump.fun/coins/{mint}",
            f"https://frontend-api-v3.pump.fun/coins/{mint}",
            f"https://client-api-2-74b1891ee9f9.herokuapp.com/coins/{mint}",
            f"https://frontend-api.pump.fun/coins/{mint}",
        ]:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5), headers={"User-Agent": "Mozilla/5.0"}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        img = data.get('image_uri') or data.get('imageUri') or data.get('image') or data.get('logo') or ''
                        if img and img.startswith('http'):
                            img = img.replace('ipfs://', 'https://pump.mypinata.cloud/ipfs/')
                            logger.info(f"🖼 Logo via pump.fun ({url.split('/')[2]}): {mint[:8]}")
                            return img
                        # Prova anche dentro metadata URI
                        uri = data.get('metadata_uri') or data.get('uri') or ''
                        if uri and uri.startswith('http'):
                            try:
                                async with session.get(uri, timeout=aiohttp.ClientTimeout(total=5)) as meta_resp:
                                    if meta_resp.status == 200:
                                        meta = await meta_resp.json()
                                        img = meta.get('image') or meta.get('image_uri') or ''
                                        if img:
                                            img = img.replace('ipfs://', 'https://pump.mypinata.cloud/ipfs/')
                                            if img.startswith('http'):
                                                logger.info(f"🖼 Logo via pump.fun metadata_uri: {mint[:8]}")
                                                return img
                            except Exception:
                                pass
            except Exception as e:
                logger.debug(f"🖼 pump.fun error for {mint[:8]} url={url[:40]}: {e}")

        # 3. Helius getAsset — fallback finale, estrae logo + socials in una sola chiamata
        from config import HELIUS_API_KEY
        if HELIUS_API_KEY:
            try:
                async with session.post(
                    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}",
                    json={"jsonrpc": "2.0", "id": "logo", "method": "getAsset", "params": {"id": mint}},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result = data.get('result', {})
                        links = result.get('content', {}).get('links', {})
                        # Salva i social in cache per riuso in _fetch_socials_from_pump
                        self._socials_cache[mint] = {
                            'twitter': links.get('twitter') or links.get('twitter_url') or None,
                            'telegram': links.get('telegram') or links.get('telegram_url') or None,
                            'website': links.get('external_url') or links.get('website') or None,
                        }
                        img = links.get('image', '')
                        if img and img.startswith('http'):
                            img = img.replace('ipfs://', 'https://pump.mypinata.cloud/ipfs/')
                            logger.info(f"🖼 Logo via Helius (fallback): {mint[:8]}")
                            return img
            except Exception as e:
                logger.debug(f"🖼 Helius error for {mint[:8]}: {e}")

        logger.warning(f"🖼 No logo found for {mint[:8]}, using placeholder")
        return None

    async def get_token_extra_data(self, mint: str, session: aiohttp.ClientSession = None) -> dict:
        """
        Fetch bonding curve % da pump.fun API.
        Usa una sessione propria per evitare problemi di chiusura sessione condivisa.
        """
        try:
            async with aiohttp.ClientSession() as s:
                bonding_pct = await self._get_bonding_curve_pct(s, mint)
            return {'bonding_curve_pct': bonding_pct}
        except Exception as e:
            logger.debug(f"Extra data error for {mint[:8]}: {e}")
            return {'bonding_curve_pct': None}

    async def _get_bonding_curve_pct(self, session: aiohttp.ClientSession, mint: str) -> Optional[float]:
        """
        Chiama pump.fun API per ottenere la bonding curve progress %.
        pump.fun considera completa la curva a 800 SOL (virtual_sol_reserves target).
        Ogni URL ha il suo try/except separato cosi il fallback funziona sempre.
        """
        for url in [
            f"https://client-api-2-74b1891ee9f9.herokuapp.com/coins/{mint}",
            f"https://frontend-api.pump.fun/coins/{mint}",
        ]:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        progress = data.get('bonding_curve_progress') or data.get('bondingCurveProgress')
                        if progress is not None:
                            return round(float(progress), 1)
                        # Fallback: calcola dai reserves
                        vsr = data.get('virtual_sol_reserves')
                        if vsr:
                            TARGET_SOL = 800 * 1_000_000_000
                            pct = min(int(vsr) / TARGET_SOL * 100, 100)
                            return round(pct, 1)
            except Exception as e:
                logger.debug(f"Bonding curve error {mint[:8]} url={url[:40]}: {e}")
                continue
        return None

    async def _fetch_socials_from_pump(self, session: aiohttp.ClientSession, mint: str) -> dict:
        """Wrapper interno: usa fetch_pump_socials con la stessa sessione."""
        # Delega a fetch_pump_socials che apre la propria sessione —
        # più semplice e consistente con il flusso organico e promo
        return await self.fetch_pump_socials(mint)


    async def fetch_pump_socials(self, mint: str) -> dict:
        """
        Fetch socials via Helius getAsset (fonte primaria — API key dedicata TRADER_HELIUS_KEY).
        getAsset restituisce metadata completo Metaplex: logo + twitter + telegram + website + discord
        in una sola chiamata RPC, molto più affidabile delle API pump.fun.

        Fallback: pump.fun API diretta se Helius non ha i dati.
        """
        import os as _os
        import config as _cfg

        def _clean(val) -> str | None:
            if not val or not isinstance(val, str):
                return None
            v = val.strip()
            if v.lower() in ('', 'null', 'none', 'undefined', 'n/a'):
                return None
            return v

        # Usa config prima (già caricato), poi fallback diretto a os.getenv
        trader_key = getattr(_cfg, 'TRADER_HELIUS_KEY', None) or _os.getenv('TRADER_HELIUS_KEY', '')
        helius_key  = trader_key or getattr(_cfg, 'HELIUS_API_KEY', None) or _os.getenv('HELIUS_API_KEY', '')
        logger.info(f"📡 fetch_pump_socials {mint[:8]}: helius_key={'YES' if helius_key else 'NO'}")

        # ── 1. Helius getAsset — fonte primaria ──
        if helius_key:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"https://mainnet.helius-rpc.com/?api-key={helius_key}",
                        json={"jsonrpc": "2.0", "id": "socials", "method": "getAsset", "params": {"id": mint}},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            res = data.get('result', {})
                            content_obj = res.get('content', {})
                            links = content_obj.get('links', {})
                            ext   = content_obj.get('metadata', {}).get('extensions', {})
                            json_uri = content_obj.get('json_uri', '')
                            logger.info(f"📡 Helius content_obj keys {mint[:8]}: {list(content_obj.keys())}")
                            logger.info(f"📡 Helius json_uri {mint[:8]}: '{json_uri}'")

                            # Prova prima da links/ext (raramente popolati da pump.fun)
                            twitter  = (_clean(links.get('twitter'))  or _clean(ext.get('twitter')))
                            telegram = (_clean(links.get('telegram')) or _clean(ext.get('telegram')))
                            website  = (_clean(links.get('external_url')) or _clean(links.get('website')) or _clean(ext.get('website')))
                            discord  = (_clean(links.get('discord'))  or _clean(ext.get('discord')))

                            # pump.fun salva i social nel metadata JSON (json_uri) — fonte reale
                            if json_uri and json_uri.startswith('http') and not all([twitter, telegram, website]):
                                try:
                                    async with session.get(
                                        json_uri,
                                        timeout=aiohttp.ClientTimeout(total=5),
                                        headers={"User-Agent": "Mozilla/5.0"},
                                    ) as jresp:
                                        if jresp.status == 200:
                                            jdata = await jresp.json(content_type=None)
                                            # pump.fun mette i social dentro il campo "extensions" o root
                                            jext = jdata.get('extensions', {}) or jdata.get('properties', {})
                                            if not twitter:  twitter  = _clean(jext.get('twitter'))  or _clean(jdata.get('twitter'))
                                            if not telegram: telegram = _clean(jext.get('telegram')) or _clean(jdata.get('telegram'))
                                            if not website:  website  = (_clean(jext.get('website'))
                                                                         or _clean(jdata.get('website'))
                                                                         or _clean(jdata.get('external_url')))
                                            if not discord:  discord  = _clean(jext.get('discord'))  or _clean(jdata.get('discord'))

                                            # pump.fun a volte mette i link nel campo description come testo libero
                                            if not all([twitter, telegram, website]):
                                                import re as _re
                                                desc = jdata.get('description', '') or ''
                                                if not twitter:
                                                    _tw = _re.search(r'https?://(?:x\.com|twitter\.com)/\S+', desc)
                                                    if _tw: twitter = _tw.group(0).rstrip('.,)')
                                                if not telegram:
                                                    _tg = _re.search(r'https?://t\.me/\S+', desc)
                                                    if not _tg:
                                                        _tg2 = _re.search(r'discord\.gg/\S+', desc)
                                                        if _tg2: discord = 'https://' + _tg2.group(0).rstrip('.,)')
                                                    else:
                                                        telegram = _tg.group(0).rstrip('.,)')
                                                if not website:
                                                    _web = _re.search(r'https?://(?!t\.me|x\.com|twitter\.com|pump\.fun)\S+', desc)
                                                    if _web: website = _web.group(0).rstrip('.,)')

                                            logger.info(f"📡 Helius json_uri {mint[:8]}: twitter={twitter} tg={telegram} web={website}")
                                except Exception as je:
                                    logger.debug(f"json_uri fetch error {mint[:8]}: {je}")

                            result = {
                                'twitter':  twitter,
                                'telegram': telegram,
                                'website':  website,
                                'discord':  discord,
                            }
                            found = [k for k, v in result.items() if v]
                            logger.info(f"📡 Helius getAsset {mint[:8]}: {found if found else 'no socials'}")
                            if found:
                                return result
            except Exception as e:
                logger.debug(f"Helius getAsset error {mint[:8]}: {e}")

        # ── 2. Fallback: pump.fun API ──
        for url in [
            f"https://frontend-api.pump.fun/coins/{mint}",
            f"https://client-api-2-74b1891ee9f9.herokuapp.com/coins/{mint}",
        ]:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=7),
                        headers={"User-Agent": "Mozilla/5.0"},
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json(content_type=None)
                        # Log tutti i campi per vedere la struttura reale
                        social_keys = {k: v for k, v in data.items() if any(x in k.lower() for x in ['twitter','telegram','website','discord','social','link','url'])}
                        logger.info(f"📡 pump.fun raw social fields {mint[:8]}: {social_keys}")

                twitter  = _clean(data.get('twitter_url'))  or _clean(data.get('twitter'))
                telegram = _clean(data.get('telegram_url')) or _clean(data.get('telegram'))
                website  = _clean(data.get('website'))
                discord  = _clean(data.get('discord_url'))  or _clean(data.get('discord'))

                result = {
                    'twitter':  twitter,
                    'telegram': telegram,
                    'website':  website,
                    'discord':  discord,
                }
                found = [k for k, v in result.items() if v]
                logger.info(f"📡 pump.fun fallback {mint[:8]}: {found if found else 'no socials'}")
                if found:
                    return result

            except Exception as e:
                logger.debug(f"pump.fun fallback error {mint[:8]}: {e}")
                continue

        logger.debug(f"📡 No socials found anywhere for {mint[:8]}")
        return {}

    def _parse_pair(self, pair: dict, mint: str) -> dict:
        """Normalizza un pair Dexscreener nel formato interno"""
        return {
            'mint': mint,
            'baseToken': {
                'name': pair.get('baseToken', {}).get('name', 'Unknown'),
                'symbol': pair.get('baseToken', {}).get('symbol', 'UNKNOWN'),
            },
            'marketCap': float(pair.get('marketCap', 0) or 0),
            'priceUsd': float(pair.get('priceUsd', 0) or 0),
            'liquidity': float(pair.get('liquidity', {}).get('usd', 0) or 0),
            'volume1h': float(pair.get('volume', {}).get('h1', 0) or 0),
            'volume24h': float(pair.get('volume', {}).get('h24', 0) or 0),
            'txns1h': (pair.get('txns', {}).get('h1', {}).get('buys', 0) or 0) + (pair.get('txns', {}).get('h1', {}).get('sells', 0) or 0),
            'buys1h': int(pair.get('txns', {}).get('h1', {}).get('buys', 0) or 0),
            'sells1h': int(pair.get('txns', {}).get('h1', {}).get('sells', 0) or 0),
            'priceChange1h': float(pair.get('priceChange', {}).get('h1', 0) or 0),
            'priceChange24h': float(pair.get('priceChange', {}).get('h24', 0) or 0),
            'holders': pair.get('holders'),
            # includi timestamp creazione pair (ms) per filtrare token vecchi
            'pairCreatedAt': pair.get('pairCreatedAt'),
            # logo — prova tutti i campi possibili in ordine di priorità
            'logo': (
                (pair.get('info', {}).get('imageUrl') or '').strip() or
                (pair.get('info', {}).get('icon') or '').strip() or
                (pair.get('info', {}).get('header') or '').strip() or
                None
            ),
            'website': (pair.get('info', {}).get('websites') or [None])[0],
            'twitter': next(
                (s.get('url') for s in pair.get('info', {}).get('socials', [])
                 if s.get('type') == 'twitter'), None
            ),
            'discord': next(
                (s.get('url') for s in pair.get('info', {}).get('socials', [])
                 if s.get('type') == 'discord'), None
            ),
            'telegram': next(
                (s.get('url') for s in pair.get('info', {}).get('socials', [])
                 if s.get('type') == 'telegram'), None
            ),
        }

    async def fetch_holders(self, mint: str) -> Optional[int]:
        """
        Conta gli holder unici di un token usando Helius getTokenAccounts.
        Pagina automaticamente fino a 10k account (sufficiente per token pump.fun).
        Costo: ~1-3 crediti Helius per chiamata (dipende dal numero di holder).
        """
        if not self.helius_key:
            return None
        try:
            total = 0
            cursor = None
            async with aiohttp.ClientSession() as session:
                for _ in range(20):  # max 20 pagine = 20k account
                    params: dict = {
                        "jsonrpc": "2.0",
                        "id": "holders",
                        "method": "getTokenAccounts",
                        "params": {
                            "mint": mint,
                            "limit": 1000,
                            "options": {"showZeroBalance": False},
                        }
                    }
                    if cursor:
                        params["params"]["cursor"] = cursor

                    async with session.post(
                        self.helius_rpc,
                        json=params,
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as r:
                        if r.status != 200:
                            break
                        data = await r.json()
                        result = data.get("result", {})
                        accounts = result.get("token_accounts", [])
                        total += len(accounts)
                        cursor = result.get("cursor")
                        if not accounts or not cursor:
                            break

            logger.info(f"👥 Holders {mint[:8]}: {total}")
            return total if total > 0 else None
        except Exception as e:
            logger.debug(f"fetch_holders error {mint[:8]}: {e}")
            return None

    # ──────────────────────────────────────────────────────────────────────────
    # CTO Discovery — endpoint DexScreener community-takeovers
    # ──────────────────────────────────────────────────────────────────────────

    async def fetch_cto_tokens(self) -> List[Dict]:
        """
        Fetcha tutti i Community Takeover Solana da DexScreener e li arricchisce
        con dati di mercato (MC, price, volume, logo, socials) pronti per il posting.

        Endpoint: GET https://api.dexscreener.com/community-takeovers/latest/v1
        Ritorna lista di dict nel formato standard usato da _process_token_candidates.
        Non applica nessun filtro MC/età — li posta tutti.
        """
        CTO_URL = "https://api.dexscreener.com/community-takeovers/latest/v1"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(CTO_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        logger.warning(f"CTO endpoint status {resp.status}")
                        return []
                    raw = await resp.json()

            if not isinstance(raw, list):
                logger.warning(f"CTO endpoint unexpected type: {type(raw)}")
                return []

            # Solo Solana
            sol_ctos = [
                item for item in raw
                if item.get('chainId') == 'solana' and item.get('tokenAddress')
            ]
            logger.info(f"🤝 DexScreener CTO: {len(sol_ctos)} Solana CTO(s) found")

            if not sol_ctos:
                return []

            mints = [item['tokenAddress'] for item in sol_ctos]
            cto_meta = {item['tokenAddress']: item for item in sol_ctos}

            # Arricchisci in batch da DexScreener
            dex_map: dict = {}
            async with aiohttp.ClientSession() as session:
                for i in range(0, len(mints), 30):
                    batch = mints[i:i+30]
                    try:
                        async with session.get(
                            f"{self.dex_base}/tokens/{','.join(batch)}",
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            if r.status == 200:
                                data = await r.json()
                                for pair in (data.get('pairs') or []):
                                    m = pair.get('baseToken', {}).get('address', '')
                                    if m and m not in dex_map:
                                        info    = pair.get('info') or {}
                                        socials = info.get('socials') or []
                                        dex_map[m] = {
                                            'symbol':   pair.get('baseToken', {}).get('symbol', ''),
                                            'name':     pair.get('baseToken', {}).get('name', ''),
                                            'mc':       float(pair.get('marketCap', 0) or 0),
                                            'priceUsd': float(pair.get('priceUsd', 0) or 0),
                                            'liquidity': float((pair.get('liquidity') or {}).get('usd', 0) or 0),
                                            'volume1h': float((pair.get('volume') or {}).get('h1', 0) or 0),
                                            'volume24h': float((pair.get('volume') or {}).get('h24', 0) or 0),
                                            'buys1h':   int((pair.get('txns') or {}).get('h1', {}).get('buys', 0) or 0),
                                            'sells1h':  int((pair.get('txns') or {}).get('h1', {}).get('sells', 0) or 0),
                                            'priceChange1h': float((pair.get('priceChange') or {}).get('h1', 0) or 0),
                                            'priceChange24h': float((pair.get('priceChange') or {}).get('h24', 0) or 0),
                                            'logo':     (info.get('imageUrl') or '').strip() or (info.get('icon') or '').strip() or '',
                                            'twitter':  next((s.get('url') for s in socials if s.get('type') == 'twitter'), None),
                                            'telegram': next((s.get('url') for s in socials if s.get('type') == 'telegram'), None),
                                            'website':  (info.get('websites') or [None])[0],
                                            'pairCreatedAt': pair.get('pairCreatedAt', 0) or int(time.time() * 1000),
                                        }
                    except Exception as e:
                        logger.debug(f"CTO DexScreener batch error: {e}")
                    await asyncio.sleep(0.1)

            # Converti in formato standard token
            tokens = []
            for mint in mints:
                meta = cto_meta[mint]
                dex  = dex_map.get(mint, {})

                symbol = dex.get('symbol') or mint[:8]
                name   = dex.get('name') or symbol
                mc     = dex.get('mc', 0.0)
                logo   = dex.get('logo', '')

                # Fallback logo: pump.fun API (sincrono light, solo se logo mancante)
                if not logo:
                    try:
                        async with aiohttp.ClientSession() as _s:
                            async with _s.get(
                                f"https://frontend-api.pump.fun/coins/{mint}",
                                timeout=aiohttp.ClientTimeout(total=4),
                                headers={"User-Agent": "Mozilla/5.0"},
                            ) as r:
                                if r.status == 200:
                                    d = await r.json(content_type=None)
                                    if not symbol or symbol == mint[:8]:
                                        symbol = d.get('symbol', symbol)
                                    if not name or name == symbol:
                                        name = d.get('name', name)
                                    img = d.get('image_uri') or d.get('image') or ''
                                    if img and img.startswith('http'):
                                        logo = img.replace('ipfs://', 'https://pump.mypinata.cloud/ipfs/')
                                    if not mc:
                                        mc = float(d.get('usd_market_cap', 0) or 0)
                    except Exception:
                        pass

                token = {
                    'mint': mint,
                    'baseToken': {'address': mint, 'symbol': symbol, 'name': name},
                    'marketCap': mc,
                    'logo': logo or None,
                    'priceUsd': dex.get('priceUsd', 0),
                    'liquidity': dex.get('liquidity', 0),
                    'volume1h': dex.get('volume1h', 0),
                    'volume24h': dex.get('volume24h', 0),
                    'txns1h': dex.get('buys1h', 0) + dex.get('sells1h', 0),
                    'sells_from_dex': bool(dex),
                    'buys1h': dex.get('buys1h', 0),
                    'sells1h': dex.get('sells1h', 0),
                    'priceChange1h': dex.get('priceChange1h', 0),
                    'priceChange24h': dex.get('priceChange24h', 0),
                    'holders': None,
                    # I CTO spesso sono token esistenti — usa timestamp corrente come
                    # pairCreatedAt per non farli scartare dal filtro età (che è disabilitato)
                    'pairCreatedAt': dex.get('pairCreatedAt') or int(time.time() * 1000),
                    'website': dex.get('website'),
                    'twitter': dex.get('twitter'),
                    'telegram': dex.get('telegram'),
                    'discord': None,
                    'info': {},
                    # Metadati CTO extra (usati nel formatter per mostrare claim date + description)
                    'cto_claim_date': (meta.get('claimDate') or '')[:10],
                    'cto_description': meta.get('description') or '',
                }
                tokens.append(token)
                logger.info(f"🤝 CTO: {symbol} ({mint[:8]}) MC=${mc:,.0f}")

            logger.info(f"🤝 {len(tokens)} CTO token(s) enriched and ready")
            return tokens

        except Exception as e:
            logger.error(f"fetch_cto_tokens error: {e}")
            return []
