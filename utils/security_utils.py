import asyncio
import logging
import aiohttp
import os
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

RUGCHECK_TIMEOUT = 10
SOLANA_RPC = "https://api.mainnet-beta.solana.com"

# RPC dedicato per bundle check — usa Helius se disponibile, fallback pubblico
_helius_key = os.getenv("TRADER_HELIUS_KEY", "")
BUNDLE_RPC = (
    f"https://mainnet.helius-rpc.com/?api-key={_helius_key}"
    if _helius_key
    else SOLANA_RPC
)
WHALE_THRESHOLD_SOL = 100  # 100 SOL ≈ $8.500
LAMPORTS_PER_SOL = 1_000_000_000


async def get_sol_balance(session: aiohttp.ClientSession, wallet: str) -> float:
    """Ritorna il balance in SOL di un wallet usando il RPC pubblico Solana."""
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [wallet]
        }
        async with session.post(
            SOLANA_RPC,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=3)
        ) as resp:
            if resp.status != 200:
                return 0.0
            data = await resp.json()
            lamports = data.get('result', {}).get('value', 0) or 0
            return lamports / LAMPORTS_PER_SOL
    except Exception:
        return 0.0


async def fetch_rugcheck(mint: str) -> Dict[str, Any]:
    """
    Chiama rugcheck.xyz API — endpoint /v1/tokens/{mint}/report (full report)
    Il /summary NON include topHolders, quindi usiamo il report completo.

    Ritorna dict con:
      score        (int)    0-100
      label        (str)    "Good" | "Warning" | "Danger"
      top_holder   (float)  % del singolo holder più grosso
      top10        (float)  % cumulativa top 10 holders
      whales       (int)    numero di wallet con >100 SOL tra top 10 holder
      rugged       (bool)
    """
    url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=RUGCHECK_TIMEOUT)) as resp:
                if resp.status != 200:
                    logger.debug(f"Rugcheck {mint[:8]}: HTTP {resp.status}")
                    return {}
                data = await resp.json()

            result = {}

            # Score — clamp a 100
            score = data.get('score')
            if score is not None:
                raw = int(score)
                if raw > 100:
                    logger.info(f"Rugcheck raw score out of range: {raw} for {mint[:8]}")
                result['score'] = min(raw, 100)

            # Label derivato dallo score
            s = int(score or 0)
            if s < 30:
                label = "Good"
            elif s < 70:
                label = "Warning"
            else:
                label = "Danger"
            result['label'] = label

            # Rugged flag
            if data.get('rugged'):
                result['rugged'] = True

            # Top holders
            top_holders = data.get('topHolders', [])
            if top_holders:
                sorted_holders = sorted(top_holders, key=lambda h: float(h.get('pct', 0) or 0), reverse=True)

                # Salta il primo (LP)
                real_holders = sorted_holders[1:] if len(sorted_holders) > 1 else sorted_holders

                # Top 1
                top1_pct = float(real_holders[0].get('pct', 0) or 0) if real_holders else 0
                if top1_pct > 0:
                    if top1_pct <= 1.0:
                        top1_pct = top1_pct * 100
                    result['top_holder'] = round(top1_pct, 1)

                # Top 10 cumulativo
                top10_sum = sum(float(h.get('pct', 0) or 0) for h in real_holders[:10])
                if top10_sum > 0:
                    if top10_sum <= 1.0:
                        top10_sum = top10_sum * 100
                    result['top10'] = round(top10_sum, 1)

                # Whale check — balance SOL sui top 10 holder (escluso LP)
                # Chiamate in parallelo per velocità
                wallet_addresses = [
                    h.get('address') for h in real_holders[:10]
                    if h.get('address')
                ]
                if wallet_addresses:
                    balance_tasks = [get_sol_balance(session, w) for w in wallet_addresses]
                    balances = await asyncio.gather(*balance_tasks)
                    whale_count = sum(1 for b in balances if b >= WHALE_THRESHOLD_SOL)
                    if whale_count > 0:
                        result['whales'] = whale_count

            logger.debug(f"Rugcheck {mint[:8]}: {result}")
            return result

    except asyncio.TimeoutError:
        logger.debug(f"Rugcheck timeout for {mint[:8]}")
        return {}
    except Exception as e:
        logger.debug(f"Rugcheck error for {mint[:8]}: {e}")
        return {}


# ── Bundle Check ──────────────────────────────────────────────────────────────
BUNDLE_TIMEOUT     = 15
BUNDLE_TX_LIMIT    = 100        # prime 100 tx — copre quasi tutti i bundle early
BUNDLE_SLOT_WINDOW = 5          # bundle = più wallet nello stesso slot o entro 5 slot
PUMP_TOKEN_SUPPLY  = 1_000_000_000

# Helius enhanced API — molto più veloce e affidabile del RPC pubblico
_helius_key = os.getenv("TRADER_HELIUS_KEY", "")
HELIUS_RPC = (
    f"https://mainnet.helius-rpc.com/?api-key={_helius_key}"
    if _helius_key else SOLANA_RPC
)
# Helius enhanced transactions API (parsed, no RPC needed)
HELIUS_TX_API = (
    f"https://api.helius.xyz/v0/transactions/?api-key={_helius_key}"
    if _helius_key else None
)


async def _get_signatures_helius(mint: str, session: aiohttp.ClientSession, limit: int = 100) -> list:
    """
    Fetch le prime firme per il mint usando Helius RPC.
    Ritorna lista di {signature, slot} ordinata dalla più vecchia (le prime tx = early buyers).
    """
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getSignaturesForAddress",
        "params": [mint, {"limit": limit, "commitment": "confirmed"}]
    }
    try:
        async with session.post(HELIUS_RPC, json=payload,
                                timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            sigs = [
                {"sig": s["signature"], "slot": s.get("slot", 0)}
                for s in (data.get("result") or [])
                if not s.get("err")
            ]
            # Le firme arrivano dalla più recente → invertiamo per avere le prime
            sigs.reverse()
            return sigs
    except Exception as e:
        logger.debug(f"getSignaturesForAddress error: {e}")
        return []


async def _parse_tx_helius(signatures: list, session: aiohttp.ClientSession) -> list:
    """
    Usa Helius Enhanced Transactions API per parsare le tx in bulk.
    Molto più veloce di getTransaction singoli — ritorna buyer + token amount direttamente.
    Fallback a getTransaction RPC se HELIUS_TX_API non disponibile.
    """
    if not signatures:
        return []

    sigs_only = [s["sig"] for s in signatures]

    # ── Helius Enhanced API (batch, parsed) ──
    if HELIUS_TX_API:
        try:
            async with session.post(
                HELIUS_TX_API,
                json={"transactions": sigs_only[:100]},
                timeout=aiohttp.ClientTimeout(total=12)
            ) as resp:
                if resp.status == 200:
                    txs = await resp.json()
                    if isinstance(txs, list) and txs:
                        logger.debug(f"Helius enhanced: got {len(txs)} parsed txs")
                        return txs
                logger.debug(f"Helius enhanced API: HTTP {resp.status}, fallback to RPC")
        except Exception as e:
            logger.debug(f"Helius enhanced API error: {e}, fallback to RPC")

    # ── Fallback: getTransaction via Helius RPC (batch da 10) ──
    async def get_tx_rpc(sig: str):
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTransaction",
            "params": [sig, {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed"
            }]
        }
        try:
            async with session.post(HELIUS_RPC, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200:
                    return None
                d = await r.json()
                return d.get("result")
        except Exception:
            return None

    txs = []
    for i in range(0, len(sigs_only), 10):
        batch = sigs_only[i:i + 10]
        results = await asyncio.gather(*[get_tx_rpc(s) for s in batch])
        txs.extend([r for r in results if r])
        await asyncio.sleep(0.1)  # rate limit gentile su Helius
    return txs


def _extract_buyers_from_parsed_tx(txs: list, mint: str) -> dict:
    """
    Estrae {wallet: token_amount} dai risultati Helius Enhanced API (formato parsed).
    Helius Enhanced ritorna oggetti con 'tokenTransfers', 'feePayer', 'slot' ecc.
    """
    buyers = {}  # wallet → token_amount

    for tx in txs:
        if not isinstance(tx, dict):
            continue
        # Formato Helius Enhanced
        fee_payer = tx.get("feePayer", "")
        slot = tx.get("slot", 0)

        for transfer in tx.get("tokenTransfers", []):
            if transfer.get("mint") != mint:
                continue
            to_wallet = transfer.get("toUserAccount", "")
            amount = float(transfer.get("tokenAmount", 0) or 0)
            if to_wallet and to_wallet != mint and amount > 0:
                buyers[to_wallet] = buyers.get(to_wallet, 0) + amount

    return buyers


def _extract_buyers_from_rpc_tx(txs: list, mint: str, sig_slots: dict) -> dict:
    """
    Estrae {wallet: token_amount_acquistato} dai risultati RPC getTransaction (jsonParsed).

    USA il DELTA pre→post token balance per calcolare quanti token ha COMPRATO ogni wallet
    in quella tx — non quanto ne tiene ora (che potrebbe essere già stato venduto).
    Questo dà la % bundlata originale, non quella corrente.
    """
    buyers = {}

    for tx in txs:
        if not isinstance(tx, dict):
            continue
        try:
            meta = tx.get("meta", {})
            pre_balances  = {b.get("accountIndex"): b for b in meta.get("preTokenBalances", [])  if b.get("mint") == mint}
            post_balances = {b.get("accountIndex"): b for b in meta.get("postTokenBalances", []) if b.get("mint") == mint}

            # Delta per ogni account che appare nei post balances
            for idx, post_bal in post_balances.items():
                owner = post_bal.get("owner", "")
                if not owner or owner == mint:
                    continue

                def _parse_amt(bal_entry):
                    if not bal_entry:
                        return 0.0
                    ui_data = bal_entry.get("uiTokenAmount", {})
                    ui_amt = ui_data.get("uiAmount")
                    if ui_amt is not None:
                        return float(ui_amt)
                    raw = int(ui_data.get("amount") or 0)
                    dec = int(ui_data.get("decimals") or 6)
                    return raw / (10 ** dec)

                post_amt = _parse_amt(post_bal)
                pre_amt  = _parse_amt(pre_balances.get(idx))
                delta    = post_amt - pre_amt  # quanti token ha RICEVUTO in questa tx

                if delta > 0:
                    buyers[owner] = buyers.get(owner, 0) + delta

        except Exception:
            continue

    return buyers


def _detect_bundles(sig_slots: list, buyers: dict) -> tuple[set, float]:
    """
    Logica bundle detection migliorata:

    1. SLOT CLUSTERING: wallet che comprano nello stesso slot (o entro BUNDLE_SLOT_WINDOW slot)
       dalla prima tx. I primi 3-5 slot dopo la creazione sono il window bundle classico.

    2. SUPPLY CONCENTRATION: wallet con >5% supply individuale nei primi acquisti
       sono segnalati indipendentemente dal clustering (snipers single-wallet).

    Ritorna (bundle_wallets: set, bundled_supply_pct: float)
    """
    if not sig_slots or not buyers:
        return set(), 0.0

    # Slot della prima transazione (creazione token / primo buy)
    first_slot = sig_slots[0]["slot"] if sig_slots else 0

    # Mappa slot → wallets che hanno comprato in quel slot
    slot_to_wallets: dict[int, set] = {}
    for entry in sig_slots:
        slot = entry.get("slot", 0)
        # Considera solo i primi BUNDLE_SLOT_WINDOW slot dalla creazione
        if slot > first_slot + BUNDLE_SLOT_WINDOW:
            break
        for wallet, amt in buyers.items():
            # Associa wallet allo slot se ha comprato (approssimazione via ordine tx)
            slot_to_wallets.setdefault(slot, set())

    # Rebuilda slot → wallet guardando l'ordine delle firme
    # Ogni firma ha un slot; il buyer di quella firma va in quel slot
    wallet_slots: dict[str, int] = {}
    for entry in sig_slots:
        slot = entry.get("slot", 0)
        # Non possiamo linkare esattamente firma→wallet senza parsare la tx,
        # ma sappiamo che i wallet che hanno comprato nei primi N slot sono sospetti
        if slot <= first_slot + BUNDLE_SLOT_WINDOW:
            for wallet in buyers:
                if wallet not in wallet_slots:
                    wallet_slots[wallet] = slot

    # Bundle wallets = tutti i wallet che hanno comprato entro il window iniziale
    early_wallets = {w for w, s in wallet_slots.items() if s <= first_slot + BUNDLE_SLOT_WINDOW}

    # Aggiungi wallet con supply individuale > 3% (snipers)
    sniper_wallets = {
        w for w, amt in buyers.items()
        if (amt / PUMP_TOKEN_SUPPLY * 100) >= 3.0
    }

    bundle_wallets = early_wallets | sniper_wallets

    # Supply bundlata
    bundled_tokens = sum(buyers.get(w, 0) for w in bundle_wallets)
    pct = round((bundled_tokens / PUMP_TOKEN_SUPPLY) * 100, 1)

    return bundle_wallets, pct


async def fetch_bundle_check(mint: str) -> dict:
    """
    Bundle check via Helius (Enhanced API + RPC fallback).

    Strategia:
    1. Fetch prime 100 firme per il mint (Helius RPC, veloce)
    2. Fetch le prime 30 tx parsed (Helius Enhanced API, bulk)
    3. Estrai buyer → token_amount
    4. Detect bundle: slot clustering nei primi 5 slot + snipers >3% supply

    Ritorna dict con:
        wallets   (int)    wallet bundle/sniper rilevati
        pct       (float)  % supply bundlata (0-100)
        score     (str)    "Good" | "Low" | "Medium" | "High"
    Ritorna {} in caso di errore (fallback silenzioso nel post).
    """
    try:
        async with aiohttp.ClientSession() as session:
            # 1. Prime firme (ordinata dalla più vecchia)
            sig_slots = await _get_signatures_helius(mint, session, limit=BUNDLE_TX_LIMIT)
            if not sig_slots:
                logger.info(f"Bundle check {mint[:8]}: no signatures found")
                return {}

            logger.info(f"Bundle check {mint[:8]}: {len(sig_slots)} sigs, first_slot={sig_slots[0].get('slot', '?')}")

            # Analizza solo le prime 30 tx per velocità (bundle avviene nei primissimi blocchi)
            early_sigs = sig_slots[:30]

            # 2. Parse tx
            txs = await _parse_tx_helius(early_sigs, session)
            if not txs:
                logger.info(f"Bundle check {mint[:8]}: no tx data")
                return {}

            # 3. Estrai buyers
            # Prova formato Helius Enhanced prima, fallback RPC
            buyers = _extract_buyers_from_parsed_tx(txs, mint)
            if not buyers:
                buyers = _extract_buyers_from_rpc_tx(txs, mint, {s["sig"]: s["slot"] for s in early_sigs})

            if not buyers:
                logger.info(f"Bundle check {mint[:8]}: no buyers found in tx data")
                return {}

            logger.info(f"Bundle check {mint[:8]}: {len(buyers)} unique buyers in first {len(txs)} txs")

            # 4. Detect bundle
            bundle_wallets, pct = _detect_bundles(sig_slots, buyers)
            wallet_count = len(bundle_wallets)

            # Score
            if wallet_count == 0 or pct < 1:
                score = "Good"
            elif pct < 10:
                score = "Low"
            elif pct < 25:
                score = "Medium"
            else:
                score = "High"

            result = {"wallets": wallet_count, "pct": pct, "score": score}
            logger.info(f"Bundle check {mint[:8]}: wallets={wallet_count} pct={pct}% → {score}")
            return result

    except asyncio.TimeoutError:
        logger.debug(f"Bundle check timeout for {mint[:8]}")
        return {}
    except Exception as e:
        logger.warning(f"Bundle check error for {mint[:8]}: {e}")
        return {}
