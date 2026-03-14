"""
admin_handlers.py — Comandi admin per gestione referral link.

Comandi disponibili (solo per admin):
  /genref <slug> [etichetta] — Genera un nuovo referral link
  /listrefs               — Mostra tutti i referral link con statistiche
  /delref <slug>          — Elimina un referral link
"""

import logging
import config
import utils.db as db
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


def _is_admin(user_id: int) -> bool:
    """Ritorna True se l'utente è l'owner del bot."""
    return config.OWNER_TELEGRAM_ID and user_id == config.OWNER_TELEGRAM_ID


async def genref_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/genref <slug> [etichetta] — Crea un nuovo referral link."""
    user = update.effective_user
    if not _is_admin(user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return

    if not context.args:
        await update.message.reply_text(
            "ℹ️ Usage: <code>/genref my_link [Optional Label]</code>\n\n"
            "Example: <code>/genref influencer1 Big Crypto Influencer</code>",
            parse_mode='HTML'
        )
        return

    slug = context.args[0].strip().lower()
    label = " ".join(context.args[1:]) if len(context.args) > 1 else slug

    # Slug: solo lettere, numeri, underscore, trattino
    import re
    if not re.match(r'^[a-z0-9_\-]{2,32}$', slug):
        await update.message.reply_text(
            "❌ Invalid slug. Use only lowercase letters, numbers, <code>_</code> or <code>-</code> (2–32 chars).",
            parse_mode='HTML'
        )
        return

    bot_info = await context.bot.get_me()
    created = db.create_referral_link(slug=slug, label=label, created_by=user.id)

    if not created:
        await update.message.reply_text(
            f"❌ Slug <code>{slug}</code> already exists. Choose another.",
            parse_mode='HTML'
        )
        return

    ref_url = f"https://t.me/{bot_info.username}?start=ref_{slug}"

    await update.message.reply_text(
        f"✅ <b>Referral link created!</b>\n\n"
        f"🏷 Label: <b>{label}</b>\n"
        f"🔑 Slug: <code>{slug}</code>\n\n"
        f"🔗 Link:\n<code>{ref_url}</code>\n\n"
        f"Share this link — every session opened and every purchase via this link will be tracked and notified to the admin channel.",
        parse_mode='HTML'
    )


async def listrefs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/listrefs — Mostra tutti i referral link con statistiche."""
    user = update.effective_user
    if not _is_admin(user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return

    links = db.list_referral_links()
    if not links:
        await update.message.reply_text("📭 No referral links yet. Use /genref to create one.")
        return

    bot_info = await context.bot.get_me()
    lines = ["📊 <b>Referral Links</b>\n"]
    for lnk in links:
        ref_url = f"https://t.me/{bot_info.username}?start=ref_{lnk['slug']}"
        lines.append(
            f"🔑 <code>{lnk['slug']}</code> — <b>{lnk['label']}</b>\n"
            f"   👀 Opens: <b>{lnk['opens']}</b>  |  💰 Payments: <b>{lnk['payments']}</b>  |  SOL: <b>{lnk['total_sol']:.2f}</b>\n"
            f"   🔗 <code>{ref_url}</code>\n"
        )

    await update.message.reply_text("\n".join(lines), parse_mode='HTML', disable_web_page_preview=True)


async def delref_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/delref <slug> — Elimina un referral link."""
    user = update.effective_user
    if not _is_admin(user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return

    if not context.args:
        await update.message.reply_text("ℹ️ Usage: <code>/delref &lt;slug&gt;</code>", parse_mode='HTML')
        return

    slug = context.args[0].strip().lower()
    deleted = db.delete_referral_link(slug)
    if deleted:
        await update.message.reply_text(f"🗑 Referral link <code>{slug}</code> deleted.", parse_mode='HTML')
    else:
        await update.message.reply_text(f"❌ Slug <code>{slug}</code> not found.", parse_mode='HTML')
