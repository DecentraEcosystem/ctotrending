"""
Persistent SQLite database for PumpBot.
Stores: posted_mints, tracked_tokens, sent_messages, top_performers, streak.

DB_PATH env var → default /data/pumpbot.db (Railway persistent volume)
Fallback: /tmp/pumpbot.db (non-persistent, solo per test locali)
"""

import os
import sqlite3
import time
import json
import logging
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)

# Railway monta i volume su /data. Se non esiste, fallback /tmp.
_default_db = '/data/pumpbot.db' if os.path.isdir('/data') else '/tmp/pumpbot.db'
DB_PATH = os.getenv('DB_PATH', _default_db)


@contextmanager
def _conn():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL")   # write-ahead log — più performante
    con.execute("PRAGMA foreign_keys=ON")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db():
    """Crea le tabelle se non esistono. Da chiamare all'avvio."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if os.path.dirname(DB_PATH) else None
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS posted_mints (
                mint      TEXT PRIMARY KEY,
                posted_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tracked_tokens (
                mint                      TEXT PRIMARY KEY,
                symbol                    TEXT NOT NULL,
                initial_mc                REAL NOT NULL,
                message_id                INTEGER NOT NULL,
                posted_at                 REAL NOT NULL,
                last_notified_multiplier  REAL NOT NULL DEFAULT 1.0,
                last_volume1h             REAL NOT NULL DEFAULT 0.0,
                volume_alert_sent         INTEGER NOT NULL DEFAULT 0,
                original_caption          TEXT NOT NULL DEFAULT '',
                logo_url                  TEXT NOT NULL DEFAULT '',
                graduation_posted         INTEGER NOT NULL DEFAULT 1,
                dex_updated               INTEGER NOT NULL DEFAULT 1,
                dex_boost_posted          INTEGER NOT NULL DEFAULT 0,
                peak_multiplier           REAL NOT NULL DEFAULT 1.0,
                livestream_posted         INTEGER NOT NULL DEFAULT 0,
                dex_ads_posted            INTEGER NOT NULL DEFAULT 0,
                dex_cto_posted            INTEGER NOT NULL DEFAULT 0
            );
            -- Aggiungi colonna se già esistente senza (upgrade da versione precedente)
            PRAGMA foreign_keys=OFF;

            -- Anti-doppioni gain: chiave (mint, milestone) invece di hash messaggio
            CREATE TABLE IF NOT EXISTS gain_alerts_sent (
                mint      TEXT NOT NULL,
                milestone REAL NOT NULL,
                sent_at   REAL NOT NULL,
                PRIMARY KEY (mint, milestone)
            );

            -- Anti-doppioni messaggi generici (streak, etc.)
            CREATE TABLE IF NOT EXISTS sent_messages (
                msg_hash  TEXT PRIMARY KEY,
                sent_at   REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS top_performers (
                mint        TEXT PRIMARY KEY,
                symbol      TEXT NOT NULL,
                multiplier  REAL NOT NULL,
                current_mc  REAL NOT NULL,
                updated_at  REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS streak (
                mint        TEXT PRIMARY KEY,
                symbol      TEXT NOT NULL,
                multiplier  REAL NOT NULL,
                ts          REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- Repost job persistenti per piani Premium/VIP
            -- Sopravvivono ai restart del processo
            CREATE TABLE IF NOT EXISTS promo_jobs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                mint            TEXT NOT NULL,
                symbol          TEXT NOT NULL,
                logo_url        TEXT NOT NULL DEFAULT '',
                plan            TEXT NOT NULL,
                channel_id      TEXT NOT NULL,
                current_msg_id  INTEGER NOT NULL,
                reposts_done    INTEGER NOT NULL DEFAULT 0,
                reposts_total   INTEGER NOT NULL,
                repost_interval INTEGER NOT NULL DEFAULT 3600,
                user_data_json  TEXT NOT NULL DEFAULT '{}',
                token_base_json TEXT NOT NULL DEFAULT '{}',
                created_at      REAL NOT NULL,
                next_repost_at  REAL NOT NULL,
                status          TEXT NOT NULL DEFAULT 'active'
            );
        """)
    # Migrations — ALTER TABLE safe (ignorano se la colonna esiste già)
    for migration in [
        "ALTER TABLE tracked_tokens ADD COLUMN original_caption TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE tracked_tokens ADD COLUMN name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE tracked_tokens ADD COLUMN plan TEXT NOT NULL DEFAULT ''",
        # graduation_posted: DEFAULT 1 per i token già esistenti al momento del deploy
        # (evita spam su tutti i token già graduated al primo ciclo).
        # I nuovi token vengono inseriti con graduation_posted=0 esplicitamente da save_tracked_token.
        "ALTER TABLE tracked_tokens ADD COLUMN graduation_posted INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE tracked_tokens ADD COLUMN logo_url TEXT NOT NULL DEFAULT ''",
        # dex_updated: DEFAULT 1 per i token già esistenti — non vengono ricontrollati.
        # Solo i token postati dopo questo deploy (inseriti con dex_updated=0) vengono monitorati.
        "ALTER TABLE tracked_tokens ADD COLUMN dex_updated INTEGER NOT NULL DEFAULT 1",
        # dex_boost_posted: DEFAULT 1 per i token già esistenti — stessa logica.
        "ALTER TABLE tracked_tokens ADD COLUMN dex_boost_posted INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE tracked_tokens ADD COLUMN peak_multiplier REAL NOT NULL DEFAULT 1.0",
        # livestream_posted: DEFAULT 0 — solo token postati dopo questo deploy
        "ALTER TABLE tracked_tokens ADD COLUMN livestream_posted INTEGER NOT NULL DEFAULT 0",
        # dex_ads_posted: DEFAULT 1 per token esistenti — non ricontrollati, solo nuovi
        "ALTER TABLE tracked_tokens ADD COLUMN dex_ads_posted INTEGER NOT NULL DEFAULT 1",
        # dex_cto_posted: DEFAULT 1 per token esistenti — non ricontrollati, solo nuovi
        "ALTER TABLE tracked_tokens ADD COLUMN dex_cto_posted INTEGER NOT NULL DEFAULT 1",
    ]:
        try:
            with _conn() as con:
                con.execute(migration)
        except Exception:
            pass  # colonna già esistente

    fix_top_performers_symbols()
    logger.info(f"✅ DB initialized at {DB_PATH}")


# ── posted_mints ─────────────────────────────────────────────────────────────

def load_posted_mints(max_age_sec: float = 86400) -> set:
    cutoff = time.time() - max_age_sec
    with _conn() as con:
        rows = con.execute(
            "SELECT mint FROM posted_mints WHERE posted_at >= ?", (cutoff,)
        ).fetchall()
    return {r[0] for r in rows}


def add_posted_mint(mint: str):
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO posted_mints (mint, posted_at) VALUES (?, ?)",
            (mint, time.time())
        )


def remove_posted_mint(mint: str):
    with _conn() as con:
        con.execute("DELETE FROM posted_mints WHERE mint = ?", (mint,))


def prune_old_mints(max_age_sec: float = 86400):
    cutoff = time.time() - max_age_sec
    with _conn() as con:
        con.execute("DELETE FROM posted_mints WHERE posted_at < ?", (cutoff,))


# ── tracked_tokens ───────────────────────────────────────────────────────────

def load_tracked_tokens(max_age_sec: float = 172800) -> list:
    """
    Ritorna lista di dict con tutti i campi.
    La durata di tracking dipende dalla performance del token:
    - Default:  48h
    - ≥20x:     96h
    - ≥50x:     7 giorni
    - ≥100x:    illimitato (rimosso solo se scende sotto 20x — gestito in memoria)
    """
    # Cutoff DB a 7 giorni — i token 100x illimitati vengono gestiti dalla logica in _check_gains
    max_cutoff = time.time() - 604800  # 7 giorni
    # Migration: aggiungi colonne se non esistono
    for migration in [
        "ALTER TABLE tracked_tokens ADD COLUMN original_caption TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE tracked_tokens ADD COLUMN name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE tracked_tokens ADD COLUMN plan TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE tracked_tokens ADD COLUMN graduation_posted INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE tracked_tokens ADD COLUMN logo_url TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE tracked_tokens ADD COLUMN dex_updated INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE tracked_tokens ADD COLUMN dex_boost_posted INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE tracked_tokens ADD COLUMN peak_multiplier REAL NOT NULL DEFAULT 1.0",
        "ALTER TABLE tracked_tokens ADD COLUMN livestream_posted INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE tracked_tokens ADD COLUMN dex_ads_posted INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE tracked_tokens ADD COLUMN dex_cto_posted INTEGER NOT NULL DEFAULT 1",
    ]:
        try:
            with _conn() as con:
                con.execute(migration)
        except Exception:
            pass

    with _conn() as con:
        rows = con.execute("""
            SELECT mint, symbol, initial_mc, message_id, posted_at,
                   last_notified_multiplier, last_volume1h, volume_alert_sent,
                   original_caption, name, plan, logo_url, dex_updated,
                   dex_boost_posted, peak_multiplier, livestream_posted,
                   dex_ads_posted, dex_cto_posted
            FROM tracked_tokens
            WHERE posted_at >= ?
        """, (max_cutoff,)).fetchall()

        result = []
        for r in rows:
            mint = r[0]
            plan = r[10] or ''
            # La logica di durata/rimozione è ora interamente in _check_gains
            # basata su peak_multiplier e MC drop. Il DB carica tutto ciò che
            # è entro 7 giorni, poi _check_gains decide cosa rimuovere.

            last_notified_from_tracked = r[5]

            max_sent = con.execute("""
                SELECT COALESCE(MAX(milestone), 1.0)
                FROM gain_alerts_sent
                WHERE mint = ?
            """, (mint,)).fetchone()[0]

            last_notified = max(last_notified_from_tracked, max_sent)

            result.append({
                'mint': mint, 'symbol': r[1], 'initial_mc': r[2],
                'message_id': r[3], 'posted_at': r[4],
                'last_notified_multiplier': last_notified,
                'last_volume1h': r[6],
                'volume_alert_sent': bool(r[7]),
                'original_caption': r[8] or '',
                'name': r[9] or '',
                'plan': plan,
                'graduation_posted': bool(r[11]),
                'logo_url': r[12] or '',
                'dex_updated': bool(r[13]),
                'dex_boost_posted': bool(r[14]),
                'peak_multiplier': float(r[15] or 1.0),
                'livestream_posted': bool(r[16]),
                'dex_ads_posted': bool(r[17]),
                'dex_cto_posted': bool(r[18]),
            })

    return result


def save_tracked_token(t) -> None:
    """t può essere TrackedToken o dict."""
    if hasattr(t, '__dict__'):
        d = t.__dict__
    else:
        d = t
    with _conn() as con:
        con.execute("""
            INSERT OR REPLACE INTO tracked_tokens
              (mint, symbol, initial_mc, message_id, posted_at,
               last_notified_multiplier, last_volume1h, volume_alert_sent,
               original_caption, name, plan, graduation_posted, logo_url, dex_updated,
               dex_boost_posted, peak_multiplier, livestream_posted,
               dex_ads_posted, dex_cto_posted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            d['mint'], d['symbol'], d['initial_mc'], d['message_id'], d['posted_at'],
            d.get('last_notified_multiplier', 1.0),
            d.get('last_volume1h', 0.0),
            int(d.get('volume_alert_sent', False)),
            d.get('original_caption', ''),
            d.get('name', ''),
            d.get('plan', ''),
            int(d.get('graduation_posted', False)),
            d.get('logo_url', ''),
            int(d.get('dex_updated', False)),
            int(d.get('dex_boost_posted', False)),
            float(d.get('peak_multiplier', 1.0)),
            int(d.get('livestream_posted', False)),
            int(d.get('dex_ads_posted', False)),
            int(d.get('dex_cto_posted', False)),
        ))


def update_tracked_token_field(mint: str, **kwargs):
    """Aggiorna uno o più campi di un token senza riscrivere tutto."""
    if not kwargs:
        return
    sets = ', '.join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [mint]
    with _conn() as con:
        con.execute(f"UPDATE tracked_tokens SET {sets} WHERE mint = ?", values)


def delete_tracked_token(mint: str):
    with _conn() as con:
        con.execute("DELETE FROM tracked_tokens WHERE mint = ?", (mint,))


def prune_old_tracked(max_age_sec: float = 86400):
    cutoff = time.time() - max_age_sec
    with _conn() as con:
        con.execute("DELETE FROM tracked_tokens WHERE posted_at < ?", (cutoff,))


# ── gain_alerts_sent ─────────────────────────────────────────────────────────

def is_gain_alert_sent(mint: str, milestone: float) -> bool:
    """Controlla se il gain alert è già stato inviato."""
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM gain_alerts_sent WHERE mint = ? AND milestone = ?",
            (mint, milestone)
        ).fetchone()
    return row is not None


def mark_gain_alert_sent(mint: str, milestone: float):
    """Segna il gain alert come inviato."""
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO gain_alerts_sent (mint, milestone, sent_at) VALUES (?, ?, ?)",
            (mint, milestone, time.time())
        )


def get_max_milestone_sent(mint: str) -> float:
    """Ritorna il milestone più alto già inviato per questo mint. 1.0 se nessuno."""
    with _conn() as con:
        row = con.execute(
            "SELECT COALESCE(MAX(milestone), 1.0) FROM gain_alerts_sent WHERE mint = ?",
            (mint,)
        ).fetchone()
    return float(row[0]) if row else 1.0


def claim_gain_alert(mint: str, milestone: float) -> bool:
    """
    Operazione atomica cross-process: usa BEGIN EXCLUSIVE per bloccare altri processi
    (es. due istanze Railway durante il deploy overlap) mentre esegue il claim.
    
    INSERT OR IGNORE + changes() dentro EXCLUSIVE transaction:
    - Solo un processo alla volta può entrare nella transazione
    - Il secondo processo aspetta il timeout poi legge la riga già inserita
    - Ritorna True solo chi ha inserito la riga (primo arrivato)
    """
    import sqlite3 as _sqlite3
    con = _sqlite3.connect(DB_PATH, timeout=15)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("BEGIN EXCLUSIVE")  # blocca tutti gli altri processi
        row = con.execute(
            "SELECT 1 FROM gain_alerts_sent WHERE mint = ? AND milestone = ?",
            (mint, milestone)
        ).fetchone()
        if row:
            con.execute("ROLLBACK")
            return False  # già inviato da un altro processo
        con.execute(
            "INSERT INTO gain_alerts_sent (mint, milestone, sent_at) VALUES (?, ?, ?)",
            (mint, milestone, time.time())
        )
        con.execute("COMMIT")
        return True  # claim riuscito — questo processo deve postare
    except Exception as e:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        logger.warning(f"claim_gain_alert error: {e}")
        return False  # in caso di errore, non postare (meglio saltare che duplicare)
    finally:
        con.close()


# ── sent_messages (streak, ecc.) ─────────────────────────────────────────────

def is_message_sent(msg_hash: str) -> bool:
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM sent_messages WHERE msg_hash = ?", (msg_hash,)
        ).fetchone()
    return row is not None


def is_message_sent_recently(msg_hash: str, within_seconds: float = 300) -> bool:
    """Controlla se il messaggio è già stato inviato negli ultimi N secondi."""
    cutoff = time.time() - within_seconds
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM sent_messages WHERE msg_hash = ? AND sent_at >= ?",
            (msg_hash, cutoff)
        ).fetchone()
    return row is not None


def mark_message_sent(msg_hash: str):
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO sent_messages (msg_hash, sent_at) VALUES (?, ?)",
            (msg_hash, time.time())
        )


def prune_sent_messages(max_age_sec: float = 86400):
    cutoff = time.time() - max_age_sec
    with _conn() as con:
        con.execute("DELETE FROM sent_messages WHERE sent_at < ?", (cutoff,))


# ── top_performers ───────────────────────────────────────────────────────────

def fix_top_performers_symbols():
    """
    Migration: rimuove i record in top_performers dove symbol == mint
    (bug precedente in _update_top_performers che salvava mint invece di symbol).
    Chiamata una volta al boot da init_db.
    """
    with _conn() as con:
        deleted = con.execute("""
            DELETE FROM top_performers WHERE symbol = mint
        """).rowcount
    if deleted:
        logger.info(f"🔧 Removed {deleted} corrupt top_performers records (symbol=mint bug)")



def load_top_performers(limit: int = 100) -> list:
    with _conn() as con:
        rows = con.execute("""
            SELECT mint, symbol, multiplier, current_mc, updated_at
            FROM top_performers
            ORDER BY multiplier DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [
        {'mint': r[0], 'symbol': r[1], 'multiplier': r[2],
         'current_mc': r[3], 'updated_at': r[4]}
        for r in rows
    ]


def upsert_top_performer(mint: str, symbol: str, multiplier: float, current_mc: float):
    with _conn() as con:
        con.execute("""
            INSERT INTO top_performers (mint, symbol, multiplier, current_mc, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(mint) DO UPDATE SET
                multiplier = CASE WHEN excluded.multiplier > multiplier THEN excluded.multiplier ELSE multiplier END,
                current_mc = CASE WHEN excluded.multiplier > multiplier THEN excluded.current_mc ELSE current_mc END,
                updated_at = CASE WHEN excluded.multiplier > multiplier THEN excluded.updated_at ELSE updated_at END
        """, (mint, symbol, multiplier, current_mc, time.time()))


# ── streak ───────────────────────────────────────────────────────────────────

def load_streak(max_age_sec: float = 86400) -> list:
    cutoff = time.time() - max_age_sec
    with _conn() as con:
        rows = con.execute("""
            SELECT mint, symbol, multiplier, ts
            FROM streak
            WHERE ts >= ?
            ORDER BY ts ASC
        """, (cutoff,)).fetchall()
    return [{'mint': r[0], 'symbol': r[1], 'multiplier': r[2], 'ts': r[3]} for r in rows]


def add_streak_entry(mint: str, symbol: str, multiplier: float):
    with _conn() as con:
        con.execute("""
            INSERT OR IGNORE INTO streak (mint, symbol, multiplier, ts)
            VALUES (?, ?, ?, ?)
        """, (mint, symbol, multiplier, time.time()))


def clear_streak():
    with _conn() as con:
        con.execute("DELETE FROM streak")

# ── used_tx_hashes (pagamenti già usati) ─────────────────────────────────────

def init_used_tx_table():
    """Crea la tabella se non esiste — chiamata all'avvio di user_handlers."""
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS used_tx_hashes (
                tx_hash   TEXT PRIMARY KEY,
                used_at   REAL NOT NULL
            )
        """)


def is_tx_used(tx_hash: str) -> bool:
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM used_tx_hashes WHERE tx_hash = ?", (tx_hash,)
        ).fetchone()
    return row is not None


def mark_tx_used(tx_hash: str):
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO used_tx_hashes (tx_hash, used_at) VALUES (?, ?)",
            (tx_hash, time.time())
        )


# ── settings (chiave/valore generico) ────────────────────────────────────────

def get_setting(key: str) -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_setting(key: str, value: str):
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value)
        )


