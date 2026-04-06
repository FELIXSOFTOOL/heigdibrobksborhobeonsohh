#!/usr/bin/env python3
"""
⚡ Full Telegram Bot — Admin Panel + CryptoPay
"""

import asyncio
import logging
import sqlite3
import os
import json
from datetime import datetime, date, timezone, timedelta

MSK = timezone(timedelta(hours=3))

def now_msk() -> datetime:
    return datetime.now(MSK).replace(tzinfo=None)

def msk_str(ts: str, fmt="%H:%M:%S") -> str:
    """Форматирует ISO строку в МСК время."""
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            # Предполагаем что хранится UTC, переводим в МСК
            dt = dt.replace(tzinfo=timezone.utc).astimezone(MSK)
        return dt.strftime(fmt)
    except:
        return "—"
from typing import Optional

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler,
    PicklePersistence,
)

# ═══════════════════════════════════════════════════════════
#  ⚙️  КОНФИГ — замени значения на свои
# ═══════════════════════════════════════════════════════════
BOT_TOKEN         = "8612413895:AAG64e7TdnTNDTLT7r0Ch_efzqilzZkOw0Q"
ADMIN_IDS         = [914120031, 8141589939]
CRYPTO_PAY_TOKEN  = "562727:AA3wWvdXo6ot5Ah5LeDa6MS0psisIr0XPge"
CRYPTO_PAY_API    = "https://pay.crypt.bot/api"   # Production
# CRYPTO_PAY_API  = "https://testnet-pay.crypt.bot/api"  # Testnet

DB_PATH           = "bot.db"
REPORTS_DIR       = "reports"
MAIN_MENU_PHOTO   = "main_menu.jpg"   # Фото для главного меню

# ═══════════════════════════════════════════════════════════
#  ConversationHandler states
# ═══════════════════════════════════════════════════════════
(
    WAIT_MSG_TARGET, WAIT_MSG_TEXT,
    WAIT_BROADCAST,
    WAIT_BAN_TARGET, WAIT_UNBAN_TARGET,
    WAIT_SETTING_VALUE,
    WAIT_PHONE,
    WAIT_PHONE_TYPE,
    WAIT_QR_PHOTO,
) = range(9)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  🗄️  DATABASE
# ═══════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        full_name   TEXT,
        balance     REAL    DEFAULT 0,
        registered  TEXT,
        is_banned   INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS queue (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        number      TEXT    NOT NULL,
        status      TEXT    DEFAULT 'new',
        operator    TEXT,
        user_id     INTEGER,
        loaded_at   TEXT,
        stood_at    TEXT,
        deleted     INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS bans (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     INTEGER,
        username    TEXT,
        banned_at   TEXT,
        banned_by   INTEGER
    );

    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT
    );

    CREATE TABLE IF NOT EXISTS payments (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     INTEGER,
        number      TEXT,
        amount      REAL,
        currency    TEXT    DEFAULT 'USDT',
        invoice_id  TEXT,
        status      TEXT    DEFAULT 'pending',
        created_at  TEXT,
        paid_at     TEXT
    );

    CREATE TABLE IF NOT EXISTS daily_report (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        number              TEXT,
        user_id             INTEGER,
        username            TEXT,
        operator            TEXT,
        stood_at            TEXT,
        paid                REAL    DEFAULT 0,
        invoice_id          TEXT,
        payment_status      TEXT    DEFAULT 'unpaid',
        report_date         TEXT,
        deleted_from_report INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS work_chats (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id     INTEGER NOT NULL,
        topic_id    INTEGER DEFAULT 0,
        added_by    INTEGER,
        added_at    TEXT,
        UNIQUE(chat_id, topic_id)
    );

    CREATE TABLE IF NOT EXISTS active_numbers (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        queue_id        INTEGER,
        number          TEXT,
        user_id         INTEGER,
        username        TEXT,
        op_username     TEXT,
        op_id           INTEGER,
        phone_type      TEXT    DEFAULT 'sms',
        chat_id         INTEGER,
        topic_id        INTEGER,
        group_msg_id    INTEGER,
        stood_msg_id    INTEGER,
        sms_code        TEXT,
        stood_at        TEXT,
        status          TEXT    DEFAULT 'waiting',
        created_at      TEXT
    );
    """)

    defaults = {
        "bot_status":       "off",
        "notifications":    "off",
        "channel_url":      "https://t.me/yourchannel",
        "support_id":       "-1001234567890",
        "sub_url":          "https://t.me/yourchannel",
        "sub_id":           "-1001234567890",
        "tariff":           "4.0",
        "tariff_skip":      "3.5",
        "hold":             "5",
        "moment_payment":   "off",
        "accept_sms":       "on",
        "accept_qr":        "on",
        "pay_log_id":       "",
    }
    for k, v in defaults.items():
        c.execute("INSERT OR IGNORE INTO settings (key,value) VALUES (?,?)", (k, v))

    conn.commit()

    # ── Миграция: добавляем новые колонки если их нет ──────
    migrations = [
        ("daily_report",   "user_id",       "ALTER TABLE daily_report ADD COLUMN user_id INTEGER"),
        ("daily_report",   "username",       "ALTER TABLE daily_report ADD COLUMN username TEXT"),
        ("daily_report",   "invoice_id",     "ALTER TABLE daily_report ADD COLUMN invoice_id TEXT"),
        ("daily_report",   "payment_status", "ALTER TABLE daily_report ADD COLUMN payment_status TEXT DEFAULT 'unpaid'"),
        ("queue",          "user_id",        "ALTER TABLE queue ADD COLUMN user_id INTEGER"),
        ("payments",       "number",         "ALTER TABLE payments ADD COLUMN number TEXT"),
        ("active_numbers", "sms_code",       "ALTER TABLE active_numbers ADD COLUMN sms_code TEXT"),
        ("active_numbers", "op_username",    "ALTER TABLE active_numbers ADD COLUMN op_username TEXT"),
        ("active_numbers", "op_id",          "ALTER TABLE active_numbers ADD COLUMN op_id INTEGER"),
        ("active_numbers", "stood_at",       "ALTER TABLE active_numbers ADD COLUMN stood_at TEXT"),
        ("active_numbers", "stood_msg_id",   "ALTER TABLE active_numbers ADD COLUMN stood_msg_id INTEGER"),
        ("active_numbers", "fell_at",        "ALTER TABLE active_numbers ADD COLUMN fell_at TEXT"),
        ("daily_report",   "fell_at",        "ALTER TABLE daily_report ADD COLUMN fell_at TEXT"),
        ("queue",          "skip_queue",     "ALTER TABLE queue ADD COLUMN skip_queue INTEGER DEFAULT 0"),
        ("active_numbers", "skip_queue",     "ALTER TABLE active_numbers ADD COLUMN skip_queue INTEGER DEFAULT 0"),
        ("work_chats",     "name",           "ALTER TABLE work_chats ADD COLUMN name TEXT DEFAULT ''"),
    ]
    for table, col, sql in migrations:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in cols:
            try:
                conn.execute(sql)
                log.info(f"Migration: added {table}.{col}")
            except Exception as e:
                log.warning(f"Migration skip {table}.{col}: {e}")

    conn.commit()
    conn.close()

def db():
    return sqlite3.connect(DB_PATH)

def get_setting(key: str) -> str:
    with db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else ""

def set_setting(key: str, value: str):
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))

def queue_count() -> int:
    with db() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM queue WHERE deleted=0 AND status IN ('wait_sms','wait_qr')"
        ).fetchone()[0]

# ═══════════════════════════════════════════════════════════
#  🔐  HELPERS
# ═══════════════════════════════════════════════════════════
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def is_banned(uid: int) -> bool:
    with db() as conn:
        row = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(row and row[0])

STATUS_EMOJI = {"new": "🆕", "wait_sms": "📩", "wait_qr": "📷", "stood": "✅"}
STATUS_NAME  = {"new": "Новый", "wait_sms": "Ждём СМС", "wait_qr": "Ждём QR", "stood": "Встал"}

def s_emoji(s): return STATUS_EMOJI.get(s, "❓")
def s_name(s):  return STATUS_NAME.get(s, s)

def esc_md(s: str) -> str:
    """Экранирует _ в строке для Markdown v1, чтобы не ломало форматирование."""
    return str(s).replace("_", "\\_")

async def check_sub(uid: int, context) -> bool:
    sub_id = get_setting("sub_id")
    if not sub_id:
        return True
    try:
        m = await context.bot.get_chat_member(sub_id, uid)
        return m.status not in ("left", "kicked")
    except:
        return True

def safe_url(url: str, fallback: str = "https://t.me") -> str:
    """Возвращает URL только если он валидный, иначе fallback."""
    if not url:
        return fallback
    url = url.strip()
    if url.startswith("https://") or url.startswith("http://") or url.startswith("tg://"):
        return url
    if url.startswith("@"):
        return f"https://t.me/{url.lstrip('@')}"
    return fallback

def register_user(user):
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id,username,full_name,registered) VALUES (?,?,?,?)",
            (user.id, user.username, user.full_name, now_msk().isoformat())
        )

# ═══════════════════════════════════════════════════════════
#  💳  CRYPTOPAY
# ═══════════════════════════════════════════════════════════
async def crypto_create_invoice(amount: float, currency: str = "USDT", desc: str = "Оплата") -> dict:
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    payload  = {"asset": currency, "amount": str(amount), "description": desc, "expires_in": 3600}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CRYPTO_PAY_API}/createInvoice", json=payload, headers=headers) as r:
                data = await r.json()
                return data.get("result", {}) if data.get("ok") else {}
    except Exception as e:
        log.error(f"CryptoPay create error: {e}")
        return {}

async def crypto_check_invoice(invoice_id: str) -> dict:
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{CRYPTO_PAY_API}/getInvoices",
                             params={"invoice_ids": invoice_id}, headers=headers) as r:
                data = await r.json()
                items = data.get("result", {}).get("items", [])
                return items[0] if items else {}
    except Exception as e:
        log.error(f"CryptoPay check error: {e}")
        return {}

def get_work_chat() -> tuple:
    """Возвращает (chat_id, topic_id) первого активного ворк-чата или (None, None)."""
    with db() as conn:
        row = conn.execute("SELECT chat_id, topic_id FROM work_chats LIMIT 1").fetchone()
    return (row[0], row[1]) if row else (None, None)

def get_all_work_chats() -> list:
    with db() as conn:
        return conn.execute("SELECT chat_id, topic_id FROM work_chats").fetchall()

async def send_to_work_chat(context, text: str, reply_markup=None) -> tuple:
    """Шлёт сообщение в ворк-чат. Возвращает (chat_id, topic_id, msg_id) или None."""
    chats = get_all_work_chats()
    if not chats:
        return None
    chat_id, topic_id = chats[0]
    try:
        kwargs = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if topic_id:
            kwargs["message_thread_id"] = topic_id
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        msg = await context.bot.send_message(**kwargs)
        return chat_id, topic_id, msg.message_id
    except Exception as e:
        log.error(f"send_to_work_chat error: {e}")
        return None

# ═══════════════════════════════════════════════════════════
#  📋  /setmax  /unsetmax
# ═══════════════════════════════════════════════════════════
async def cmd_setmax(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    chat_id  = update.effective_chat.id
    topic_id = update.message.message_thread_id or 0
    name     = " ".join(context.args) if context.args else ""
    now      = now_msk().isoformat()
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO work_chats (chat_id, topic_id, added_by, added_at, name) VALUES (?,?,?,?,?)",
            (chat_id, topic_id, update.effective_user.id, now, name)
        )
        if name:
            conn.execute(
                "UPDATE work_chats SET name=? WHERE chat_id=? AND topic_id=?",
                (name, chat_id, topic_id)
            )
    name_str = f" «{name}»" if name else ""
    await update.message.reply_text(
        f"✅ Ворк-чат{name_str} добавлен.\n`chat_id={chat_id}`, `topic_id={topic_id}`",
        parse_mode="Markdown"
    )
    log.info(f"/setmax: chat_id={chat_id} topic_id={topic_id} name={name!r}")

async def cmd_settopic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Задать название для текущего топика. Использование: /settopic <название>"""
    if not is_admin(update.effective_user.id):
        return
    chat_id  = update.effective_chat.id
    topic_id = update.message.message_thread_id or 0
    name     = " ".join(context.args) if context.args else ""
    if not name:
        await update.message.reply_text("Использование: `/settopic Название топика`", parse_mode="Markdown")
        return
    with db() as conn:
        wc = conn.execute(
            "SELECT id FROM work_chats WHERE chat_id=? AND topic_id=?", (chat_id, topic_id)
        ).fetchone()
        if not wc:
            await update.message.reply_text("❌ Этот чат/топик не добавлен как ворк-чат. Сначала `/setmax`.", parse_mode="Markdown")
            return
        conn.execute(
            "UPDATE work_chats SET name=? WHERE chat_id=? AND topic_id=?",
            (name, chat_id, topic_id)
        )
    await update.message.reply_text(f"✅ Название топика: «{name}»", parse_mode="Markdown")

async def cmd_unsetmax(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    chat_id  = update.effective_chat.id
    topic_id = update.message.message_thread_id or 0
    with db() as conn:
        conn.execute(
            "DELETE FROM work_chats WHERE chat_id=? AND topic_id=?",
            (chat_id, topic_id)
        )
    await update.message.reply_text(
        f"⛔ Ворк выключен.\n`chat_id={chat_id}`, `topic={topic_id}`",
        parse_mode="Markdown"
    )

# ═══════════════════════════════════════════════════════════
#  📲  ФОРВАРД НОМЕРА В ГРУППУ
# ═══════════════════════════════════════════════════════════
async def forward_number_to_group(context, queue_id: int, number: str,
                                   user_id: int, username: str, phone_type: str):
    """Отправляет карточку номера в ворк-чат."""
    type_label = "💬 СМС" if phone_type == "sms" else "📷 QR"
    num_display = number if number else "(нет номера)"
    uname_safe = esc_md(username) if username else str(user_id)
    text = (
        f"⚡ *Номер сдан* ⚡\n\n"
        f"Метод: {type_label}\n"
        f"Номер: `{num_display}`\n"
        f"Дроп: @{uname_safe} (ID: `{user_id}`)\n"
        f"Оператор: —"
    )

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Отменить номер", callback_data=f"op_cancel_{queue_id}"),
    ]])

    result = await send_to_work_chat(context, text, reply_markup=kb)
    if not result:
        log.error("Нет ворк-чата! Используй /setmax в нужной теме группы.")
        return

    chat_id, topic_id, msg_id = result

    # Сохраняем в active_numbers
    with db() as conn:
        conn.execute(
            """INSERT INTO active_numbers
               (queue_id, number, user_id, username, phone_type,
                chat_id, topic_id, group_msg_id, status, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (queue_id, number, user_id, username, phone_type,
             chat_id, topic_id, msg_id, "waiting", now_msk().isoformat())
        )

# ═══════════════════════════════════════════════════════════
#  ⏱️  ТАЙМАУТЫ (job_queue)
# ═══════════════════════════════════════════════════════════
SMS_CODE_TIMEOUT = 40   # секунд на ввод СМС кода
QR_TIMEOUT       = 60   # секунд на сканирование QR

async def _timeout_sms(context):
    """Срабатывает если дроп не подтвердил или не ввёл СМС код."""
    queue_id = context.job.data
    with db() as conn:
        row = conn.execute(
            """SELECT number, user_id, username, chat_id, topic_id, status
               FROM active_numbers
               WHERE queue_id=? AND status IN ('wait_sms_code','wait_sms_confirm')""",
            (queue_id,)
        ).fetchone()
        if not row:
            return
        conn.execute("UPDATE active_numbers SET status='timeout' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))

    number, uid, uname, chat_id, topic_id, an_status = row
    context.bot_data.pop(f"wait_sms_{uid}", None)
    context.bot_data.pop(f"wait_sms_msg_{uid}", None)

    # Удаляем сообщение подтверждения если было
    confirm_data = context.bot_data.pop(f"sms_confirm_msg_{uid}", None)
    if confirm_data:
        msg_id = confirm_data[0] if isinstance(confirm_data, tuple) else confirm_data
        try:
            await context.bot.delete_message(chat_id=uid, message_id=msg_id)
        except: pass

    reason = "не подтвердил готовность" if an_status == "wait_sms_confirm" else "не ввёл код вовремя"

    try:
        await context.bot.send_message(
            uid,
            f"❌ *Номер `{number}` отменён* — {reason}.\n\n"
            f"Сдайте новый номер через кнопку *📲 Сдать номер*.",
            parse_mode="Markdown"
        )
    except: pass

    try:
        send_kw = {"chat_id": chat_id, "text": f"❌ *Номер `{number}` отменён* — дроп {reason}.", "parse_mode": "Markdown"}
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        await context.bot.send_message(**send_kw)
    except: pass


async def _timeout_qr(context):
    """Срабатывает если QR не подтверждён или не отсканирован вовремя."""
    queue_id = context.job.data
    with db() as conn:
        row = conn.execute(
            """SELECT number, user_id, username, chat_id, topic_id, status
               FROM active_numbers
               WHERE queue_id=? AND status IN ('wait_qr_confirm','wait_qr_photo','qr_sent')""",
            (queue_id,)
        ).fetchone()
        if not row:
            return
        conn.execute("UPDATE active_numbers SET status='timeout' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))

    number, uid, uname, chat_id, topic_id, an_status = row
    context.bot_data.pop(f"wait_qr_{uid}", None)

    # Удаляем сообщение подтверждения если было
    confirm_data = context.bot_data.pop(f"qr_confirm_msg_{uid}", None)
    if confirm_data:
        msg_id = confirm_data[0] if isinstance(confirm_data, tuple) else confirm_data
        try:
            await context.bot.delete_message(chat_id=uid, message_id=msg_id)
        except: pass

    reason = "не подтвердил готовность" if an_status == "wait_qr_confirm" else "QR не отсканирован вовремя"

    try:
        await context.bot.send_message(
            uid,
            f"❌ *Номер `{number}` отменён* — {reason}.\n\n"
            f"Сдайте новый номер через кнопку *📲 Сдать номер*.",
            parse_mode="Markdown"
        )
    except: pass
    try:
        send_kw = {
            "chat_id": chat_id,
            "text": f"❌ *Номер `{number}` отменён* — дроп {reason}.",
            "parse_mode": "Markdown"
        }
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        await context.bot.send_message(**send_kw)
    except: pass


def _cancel_job(context, name: str):
    jobs = context.job_queue.get_jobs_by_name(name)
    for j in jobs:
        j.schedule_removal()


async def _notify_pos3_if_needed(context, skip_queue_id: int = None):
    """Уведомляет кто сейчас 3-й в очереди."""
    try:
        with db() as conn:
            rows = conn.execute(
                "SELECT id, user_id, number FROM queue WHERE status IN ('wait_sms','wait_qr') AND deleted=0 ORDER BY id ASC"
            ).fetchall()
        if len(rows) < 3:
            return
        q3_id, q3_uid, q3_num = rows[2]
        if skip_queue_id and q3_id == skip_queue_id:
            return
        # Проверяем не отправляли ли уже (таймер уже бежит?)
        if context.job_queue.get_jobs_by_name(f"confirm_timeout_{q3_id}"):
            return
        # Проверяем через bot_data
        sent_key = f"pos3_sent_{q3_id}"
        if context.bot_data.get(sent_key):
            return
        context.bot_data[sent_key] = True

        kb3 = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Я здесь", callback_data=f"confirm_active_{q3_id}"),
        ]])
        sent_c = await context.bot.send_message(
            q3_uid,
            f"⚡ *Твой номер 3-й в очереди*\n\nНомер: `{q3_num}`\n\n"
            f"Подтверди присутствие за *15 секунд*, иначе номер уйдёт в конец.",
            parse_mode="Markdown",
            reply_markup=kb3
        )
        context.bot_data[f"confirm_msg_{q3_uid}"] = sent_c.message_id
        _cancel_job(context, f"confirm_timeout_{q3_id}")
        context.job_queue.run_once(
            _timeout_confirm, 15,
            data={"queue_id": q3_id, "uid": q3_uid, "number": q3_num},
            name=f"confirm_timeout_{q3_id}"
        )
        log.info(f"✅ pos3 уведомление → uid={q3_uid} qid={q3_id}")
    except Exception as e:
        log.error(f"_notify_pos3_if_needed error: {e}")


# ═══════════════════════════════════════════════════════════
#  🎛️  ОПЕРАТОРСКИЕ КНОПКИ — СМС
# ═══════════════════════════════════════════════════════════
async def _check_op_access(q, queue_id: int) -> bool:
    uid = q.from_user.id
    if is_admin(uid):
        return True
    with db() as conn:
        row = conn.execute("SELECT op_id FROM active_numbers WHERE queue_id=?", (queue_id,)).fetchone()
    if not row or not row[0]:
        return True
    if int(row[0]) == uid:
        return True
    await q.answer("⛔ Это не твой номер.", show_alert=True)
    return False


async def cb_op_req_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    if not await _check_op_access(q, queue_id): return

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
    if not row:
        await q.answer("❌ Не найдено", show_alert=True); return

    number, uid, uname = row
    uname_safe = esc_md(uname) if uname else str(uid)

    # Сначала пробуем отправить дропу — если не вышло, оператору не показываем "взят"
    try:
        sent = await context.bot.send_message(
            uid,
            f"⚡️ *Ввод кода* ⚡️\n\nНомер: `{number}`\n\nОператор запросил СМС.\n"
            f"*ПРИШЛИТЕ КОД ОТВЕТОМ НА СООБЩЕНИЕ* 👇",
            parse_mode="Markdown"
        )
        with db() as conn:
            conn.execute("UPDATE active_numbers SET status='wait_sms_code' WHERE queue_id=?", (queue_id,))
        pending = context.bot_data.setdefault(f"sms_pending_{uid}", {})
        pending[sent.message_id] = queue_id
        context.bot_data[f"wait_sms_{uid}"]     = queue_id
        context.bot_data[f"wait_sms_msg_{uid}"] = sent.message_id
        _cancel_job(context, f"sms_timeout_{queue_id}")
        context.job_queue.run_once(
            _timeout_sms, SMS_CODE_TIMEOUT, data=queue_id, name=f"sms_timeout_{queue_id}"
        )
    except Exception as e:
        log.error(f"SMS request error: {e}")
        # Отправляем оператору отдельное сообщение, т.к. q.answer уже использован
        try:
            await context.bot.send_message(
                q.from_user.id,
                f"❌ Не удалось отправить запрос дропу @{uname_safe} (uid={uid}).\nВозможно, дроп заблокировал бота.",
                parse_mode="Markdown"
            )
        except: pass
        return

    await safe_edit(
        q.edit_message_text,
        f"⚡ *Номер взят* ⚡\n\nМетод: 💬 СМС\nНомер: `{number}`\n"
        f"Дроп: @{uname_safe}\n\n📩 *СМС запрошено...*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Отменить номер", callback_data=f"op_cancel_{queue_id}"),
        ]])
    )


async def cb_drop_sms_ready(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дроп нажал Готов принять код — превращаем сообщение в запрос кода."""
    q = update.callback_query
    await q.answer()
    uid      = q.from_user.id
    queue_id = int(q.data.split("_")[-1])

    with db() as conn:
        row = conn.execute(
            "SELECT number FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='wait_sms_code' WHERE queue_id=?", (queue_id,))

    if not row:
        return
    number = row[0]

    # Редактируем ТО ЖЕ сообщение — превращаем в запрос кода
    await safe_edit(
        q.edit_message_text,
        f"⚡️ *Ввод кода* ⚡️\n\nНомер: `{number}`\n\nОператор запросил СМС.\n"
        f"*ПРИШЛИТЕ КОД ОТВЕТОМ НА СООБЩЕНИЕ* 👇",
        parse_mode="Markdown"
    )

    # Сохраняем message_id для проверки reply
    pending = context.bot_data.setdefault(f"sms_pending_{uid}", {})
    pending[q.message.message_id] = queue_id
    context.bot_data[f"wait_sms_{uid}"]     = queue_id
    context.bot_data[f"wait_sms_msg_{uid}"] = q.message.message_id
    context.bot_data.pop(f"sms_confirm_msg_{uid}", None)


async def cb_op_repeat_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    if not await _check_op_access(q, queue_id): return

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username, op_username, phone_type FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
    if not row:
        await q.answer("❌ Не найдено", show_alert=True); return

    number, uid, uname, op_uname, ptype = row
    uname_safe = esc_md(uname) if uname else str(uid)
    op_safe    = esc_md(op_uname) if op_uname else "—"
    type_label = "💬 СМС" if ptype == "sms" else "📷 QR"

    with db() as conn:
        conn.execute("UPDATE active_numbers SET status='wait_sms_code' WHERE queue_id=?", (queue_id,))
    _cancel_job(context, f"sms_timeout_{queue_id}")
    context.job_queue.run_once(_timeout_sms, SMS_CODE_TIMEOUT, data=queue_id, name=f"sms_timeout_{queue_id}")

    # Редактируем карточку в группе на "Повтор Кода"
    await safe_edit(
        q.edit_message_text,
        f"⚡ *Повтор Кода* ⚡\n\n"
        f"Метод: {type_label}\n"
        f"Номер: `{number}`\n"
        f"Оператор: @{op_safe}\n"
        f"Дроп: @{uname_safe}\n\n"
        f"📩 *Ожидание повтор кода*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔴 Отменить номер", callback_data=f"op_cancel_{queue_id}"),
        ]])
    )
    context.bot_data[f"repeat_code_msg_{queue_id}"] = q.message.message_id

    try:
        sent = await context.bot.send_message(
            uid,
            f"⚡️ *Ввод кода* ⚡️\n\nНомер: `{number}`\n\nОператор запросил СМС повторно.\n"
            f"*ПРИШЛИТЕ КОД ОТВЕТОМ НА СООБЩЕНИЕ* 👇",
            parse_mode="Markdown"
        )
        pending = context.bot_data.setdefault(f"sms_pending_{uid}", {})
        pending[sent.message_id] = queue_id
        context.bot_data[f"wait_sms_{uid}"]     = queue_id
        context.bot_data[f"wait_sms_msg_{uid}"] = sent.message_id
    except Exception as e:
        try:
            await context.bot.send_message(q.from_user.id, f"❌ Не смог отправить дропу: {e}")
        except: pass


# ═══════════════════════════════════════════════════════════
#  📷  ОПЕРАТОРСКИЕ КНОПКИ — QR (оператор шлёт фото В ГРУППЕ)
# ═══════════════════════════════════════════════════════════
async def cb_op_req_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оператор нажал QR — сначала подтверждение от дропа, потом фото в группу."""
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
    if not row:
        await q.answer("❌ Не найдено", show_alert=True); return

    number, uid, uname = row

    uname_safe_qr = esc_md(uname) if uname else str(uid)
    await safe_edit(
        q.edit_message_text,
        f"⚡ *Номер взят* ⚡\n\nМетод: 📷 QR\nНомер: `{number}`\n"
        f"Дроп: @{uname_safe_qr}\n\n📷 *Отправьте фото QR-кода в этот чат*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Отменить номер", callback_data=f"op_cancel_{queue_id}"),
        ]])
    )

    try:
        chat_id  = q.message.chat_id
        topic_id = q.message.message_thread_id or 0
        with db() as conn:
            conn.execute("UPDATE active_numbers SET status='wait_qr_photo' WHERE queue_id=?", (queue_id,))
        context.bot_data[f"wait_qr_op_{chat_id}_{topic_id}"] = queue_id
        _cancel_job(context, f"qr_timeout_{queue_id}")
        context.job_queue.run_once(_timeout_qr, QR_TIMEOUT, data=queue_id, name=f"qr_timeout_{queue_id}")
    except Exception as e:
        log.error(f"QR request error: {e}")
        await q.answer("❌ Не смог отправить сообщение дропу", show_alert=True)


async def cb_drop_qr_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дроп подтвердил готовность сканировать QR."""
    q = update.callback_query
    await q.answer("✅")
    uid      = q.from_user.id
    queue_id = int(q.data.split("_")[-1])

    with db() as conn:
        row = conn.execute(
            "SELECT number, chat_id, topic_id FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='wait_qr_photo' WHERE queue_id=?", (queue_id,))

    context.bot_data.pop(f"qr_confirm_msg_{uid}", None)

    if not row:
        return
    number, chat_id, topic_id = row

    # Ставим флаг: ждём фото от оператора в группе
    context.bot_data[f"wait_qr_op_{chat_id}_{topic_id}"] = queue_id

    # Говорим дропу ждать
    await safe_edit(
        q.edit_message_text,
        f"✅ *Подтверждено!*\n\nНомер: `{number}`\n\nОжидайте QR-код от оператора...",
        parse_mode="Markdown"
    )

    # Уведомляем группу что можно слать фото
    try:
        send_kw = {
            "chat_id": chat_id,
            "text": f"📷 *Дроп готов!*\n\nНомер: `{number}`\nОтправьте фото QR-кода в этот чат.",
            "parse_mode": "Markdown"
        }
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        await context.bot.send_message(**send_kw)
    except Exception as e:
        log.error(f"Не смог уведомить группу: {e}")


async def handle_qr_photo_from_op(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Фото QR от оператора в группе — только если это reply на сообщение бота."""
    if not update.message or not update.message.photo:
        return False
    chat_id  = update.effective_chat.id
    topic_id = update.message.message_thread_id or 0
    queue_id = context.bot_data.get(f"wait_qr_op_{chat_id}_{topic_id}")
    if not queue_id:
        return False

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username, group_msg_id FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='qr_sent' WHERE queue_id=?", (queue_id,))

    context.bot_data.pop(f"wait_qr_op_{chat_id}_{topic_id}", None)
    _cancel_job(context, f"qr_timeout_{queue_id}")

    if not row:
        return True

    number, uid, uname, group_msg_id = row
    context.bot_data[f"qr_group_msg_{queue_id}"] = (chat_id, topic_id, group_msg_id)

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Отсканировал", callback_data=f"drop_qr_scanned_{queue_id}"),
        InlineKeyboardButton("🔄 Повтор",       callback_data=f"drop_qr_repeat_{queue_id}"),
    ], [
        InlineKeyboardButton("❌ Отменить",     callback_data=f"drop_qr_cancel_{queue_id}"),
    ]])
    try:
        photo = update.message.photo[-1].file_id
        num_safe = esc_md(number)
        await context.bot.send_photo(
            chat_id=uid, photo=photo,
            caption=f"⚡️ *QR-код* ⚡️\n\nНомер: `{num_safe}`\n\nПросканируйте QR и нажмите кнопку ниже 👇",
            parse_mode="Markdown",
            reply_markup=kb
        )
    except Exception as e:
        log.error(f"Не смог переслать QR дропу: {e}")
        return True

    uname_safe_qrsent = esc_md(uname) if uname else str(uid)
    try:
        send_kw = {
            "chat_id": chat_id,
            "text": f"✅ *QR отправлен дропу* @{uname_safe_qrsent}",
            "parse_mode": "Markdown",
            "reply_markup": InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Отменить", callback_data=f"op_cancel_{queue_id}"),
            ]])
        }
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        await context.bot.send_message(**send_kw)
    except: pass

    context.job_queue.run_once(_timeout_qr, QR_TIMEOUT, data=queue_id, name=f"qr_timeout_{queue_id}")
    return True


async def cb_drop_qr_scanned(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дроп нажал Отсканировал — уведомляем группу."""
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    _cancel_job(context, f"qr_timeout_{queue_id}")

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username, chat_id, topic_id FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='qr_scanned_wait' WHERE queue_id=?", (queue_id,))

    if not row:
        await q.answer("❌ Не найдено", show_alert=True); return

    number, uid, uname, chat_id, topic_id = row

    await safe_edit(
        q.edit_message_caption,
        f"⚡️ *QR-код* ⚡️\n\nНомер: `{number}`\n\n✅ *Принято. Жди подтверждения.*",
        parse_mode="Markdown"
    )

    group_data = context.bot_data.pop(f"qr_group_msg_{queue_id}", None)
    gm_id = group_data[2] if group_data else None

    try:
        send_kw = {
            "chat_id": chat_id,
            "text": (
                f"⚡ *QR отсканирован* ⚡\n\nНомер: `{number}`\n"
                f"Дроп: @{uname or uid}\n\nПодтверди результат:"
            ),
            "parse_mode": "Markdown",
            "reply_markup": InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Встал",    callback_data=f"op_stood_{queue_id}"),
                InlineKeyboardButton("🔄 Повтор",  callback_data=f"op_qr_repeat_{queue_id}"),
            ], [
                InlineKeyboardButton("❌ Не встал", callback_data=f"op_not_stood_{queue_id}"),
            ]])
        }
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        if gm_id:
            send_kw["reply_to_message_id"] = gm_id
        await context.bot.send_message(**send_kw)
    except Exception as e:
        log.error(f"Не смог отправить QR подтверждение в группу: {e}")


async def cb_op_qr_repeat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оператор нажал Повтор QR — просим оператора прислать новое фото в группу."""
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    if not await _check_op_access(q, queue_id): return

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='wait_qr_photo' WHERE queue_id=?", (queue_id,))

    if not row:
        await q.answer("❌ Не найдено", show_alert=True); return

    number, uid, uname = row
    chat_id  = q.message.chat_id
    topic_id = q.message.message_thread_id or 0
    context.bot_data[f"wait_qr_op_{chat_id}_{topic_id}"] = queue_id

    _cancel_job(context, f"qr_timeout_{queue_id}")
    context.job_queue.run_once(_timeout_qr, QR_TIMEOUT, data=queue_id, name=f"qr_timeout_{queue_id}")

    await safe_edit(
        q.edit_message_text,
        f"⚡ *QR повтор* ⚡\n\nНомер: `{number}`\nДроп: @{uname or uid}\n\n"
        f"📷 *Отправьте новое фото QR-кода в этот чат*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Отменить номер", callback_data=f"op_cancel_{queue_id}"),
        ]])
    )


async def cb_drop_qr_repeat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дроп нажал Повтор — сообщаем в группу."""
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username, chat_id, topic_id FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()

    if not row:
        await q.answer("❌ Не найдено", show_alert=True); return

    number, uid, uname, chat_id, topic_id = row
    context.bot_data[f"wait_qr_op_{chat_id}_{topic_id}"] = queue_id
    with db() as conn:
        conn.execute("UPDATE active_numbers SET status='wait_qr_photo' WHERE queue_id=?", (queue_id,))

    # Уведомляем группу
    try:
        send_kw = {
            "chat_id": chat_id,
            "text": f"⚡️ *QR повтор* ⚡️\n\nНомер: `{number}`\n\n📷 Отправьте фото QR следующим сообщением.",
            "parse_mode": "Markdown"
        }
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        await context.bot.send_message(**send_kw)
    except: pass

    await safe_edit(
        q.edit_message_caption,
        f"⚡️ *QR-код* ⚡️\n\nНомер: `{number}`\n\n⏳ Ожидай новый QR от оператора...",
        parse_mode="Markdown"
    )


async def cb_drop_qr_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дроп нажал Отменить в QR."""
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username, chat_id, topic_id FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='cancelled' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))

    if row:
        number, uid, uname, chat_id, topic_id = row
        _cancel_job(context, f"qr_timeout_{queue_id}")
        await safe_edit(
            q.edit_message_caption,
            f"❌ *Номер отменён дропом*\n\nНомер: `{number}`",
            parse_mode="Markdown"
        )
        try:
            _uname_safe_cancel = esc_md(uname) if uname else str(uid)
            send_kw = {"chat_id": chat_id, "text": f"❌ *Дроп @{_uname_safe_cancel} отменил номер* `{number}`", "parse_mode": "Markdown"}
            if topic_id:
                send_kw["message_thread_id"] = topic_id
            await context.bot.send_message(**send_kw)
        except: pass


async def cb_drop_qr_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дроп нажал Назад — просто убираем кнопку."""
    q = update.callback_query
    await q.answer()
    await safe_edit(q.edit_message_caption, q.message.caption or "⏳ Ожидай решения оператора...", parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════
#  🎯  ОПЕРАТОР БЕРЁТ НОМЕР — только по команде
# ═══════════════════════════════════════════════════════════
async def cmd_get_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ключевые слова: 'Номер смс' / 'Номер куар' — берёт номер из очереди."""
    if not update.message or not update.message.text:
        return

    raw = update.message.text.strip().lower()
    if "номер смс" in raw or "номер sms" in raw:
        ptype_filter = "sms"
    elif "номер куар" in raw or "номер qr" in raw or "номер кьюар" in raw:
        ptype_filter = "qr"
    else:
        return  # Не ключевое слово — игнорируем

    chat_id  = update.effective_chat.id
    topic_id = update.message.message_thread_id or 0

    with db() as conn:
        # Проверяем точное совпадение chat_id + topic_id
        wc = conn.execute(
            "SELECT id FROM work_chats WHERE chat_id=? AND topic_id=?", (chat_id, topic_id)
        ).fetchone()
        if not wc:
            # Фоллбэк: чат зарегистрирован без топика (topic_id=0)
            wc = conn.execute(
                "SELECT id FROM work_chats WHERE chat_id=? AND topic_id=0", (chat_id,)
            ).fetchone()
    if not wc:
        return

    # Проверяем разрешён ли тип
    if ptype_filter == "sms" and get_setting("accept_sms") != "on":
        await update.message.reply_text("⛔ Приём СМС номеров отключён.")
        return
    if ptype_filter == "qr" and get_setting("accept_qr") != "on":
        await update.message.reply_text("⛔ Приём QR номеров отключён.")
        return

    if get_setting("bot_status") != "on":
        await update.message.reply_text("⛔ Бот выключен.")
        return

    op = update.effective_user
    status_filter = "wait_sms" if ptype_filter == "sms" else "wait_qr"

    with db() as conn:
        row = conn.execute(
            """SELECT q.id, q.number, q.user_id, u.username, q.status
               FROM queue q
               LEFT JOIN users u ON u.user_id = q.user_id
               WHERE q.status=? AND q.deleted=0
               ORDER BY q.loaded_at ASC LIMIT 1""",
            (status_filter,)
        ).fetchone()

        if not row:
            await update.message.reply_text(
                f"⏳ Нет {'СМС' if ptype_filter=='sms' else 'QR'} номеров."
            )
            return

        queue_id, number, user_id, uname, _ = row
        conn.execute("UPDATE queue SET status='taken' WHERE id=?", (queue_id,))

    await _send_number_card_to_group(
        context, queue_id, number, user_id,
        uname or str(user_id), ptype_filter,
        chat_id, topic_id,  # используем topic_id из текущего сообщения
        op_username=op.username or str(op.id),
        op_id=op.id
    )
    # После взятия номера — уведомить нового 3-го
    await _notify_pos3_if_needed(context, skip_queue_id=queue_id)
    # Уведомляем дропа что номер взяли
    try:
        await context.bot.send_message(
            user_id,
            f"🟩 Ваш номер `{number}` взят.\n\nКод придёт в течение 1 минуты.",
            parse_mode="Markdown"
        )
    except: pass


async def _send_number_card_to_group(context, queue_id, number, user_id, uname, ptype,
                                      chat_id, topic_id, op_username=None, op_id=None):
    """Отправляет карточку номера в группу и сохраняет active_numbers."""
    type_label = "💬 СМС" if ptype == "sms" else "📷 QR"
    op_str = f"@{esc_md(op_username)}" if op_username else "—"
    op_id_str = str(op_id) if op_id else "—"

    uname_safe_card = esc_md(uname) if uname else str(user_id)
    text = (
        f"⚡ *Номер взят* ⚡\n\n"
        f"Метод: {type_label}\n"
        f"Номер: `{number}`\n"
        f"Дроп: @{uname_safe_card}\n"
        f"Оператор: {op_str} | ID: `{op_id_str}`"
    )

    if ptype == "sms":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("💬 Запросить СМС",  callback_data=f"op_req_sms_{queue_id}"),
            InlineKeyboardButton("❌ Отменить номер", callback_data=f"op_cancel_{queue_id}"),
        ]])
    else:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📷 Запросить QR",   callback_data=f"op_req_qr_{queue_id}"),
            InlineKeyboardButton("❌ Отменить номер", callback_data=f"op_cancel_{queue_id}"),
        ]])

    try:
        send_kw = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": kb}
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        msg = await context.bot.send_message(**send_kw)
        msg_id = msg.message_id
    except Exception as e:
        log.error(f"Не смог отправить карточку в группу: {e}")
        return

    with db() as conn:
        existing = conn.execute("SELECT id FROM active_numbers WHERE queue_id=?", (queue_id,)).fetchone()
        sq_flag = conn.execute("SELECT skip_queue FROM queue WHERE id=?", (queue_id,)).fetchone()
        skip_q_val = sq_flag[0] if sq_flag else 0
        if existing:
            conn.execute(
                "UPDATE active_numbers SET chat_id=?, topic_id=?, group_msg_id=?, status='waiting', skip_queue=? WHERE queue_id=?",
                (chat_id, topic_id, msg_id, skip_q_val, queue_id)
            )
        else:
            conn.execute(
                """INSERT INTO active_numbers
                   (queue_id, number, user_id, username, op_username, op_id, phone_type,
                    chat_id, topic_id, group_msg_id, status, skip_queue, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (queue_id, number, user_id, uname, op_username, op_id, ptype,
                 chat_id, topic_id, msg_id, "waiting", skip_q_val, now_msk().isoformat())
            )
async def cb_op_stood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    if not await _check_op_access(q, queue_id): return
    now = now_msk()
    now_str = now.strftime("%H:%M:%S")

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username, op_username, op_id FROM active_numbers WHERE queue_id=?",
            (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='stood', stood_at=? WHERE queue_id=?",
                     (now.isoformat(), queue_id))
        conn.execute("UPDATE queue SET status='stood', stood_at=?, deleted=1 WHERE id=?",
                     (now.isoformat(), queue_id))
        if row:
            number, uid, uname, op_uname, op_id_val = row
            today = date.today().isoformat()
            # Создаём запись только если её нет (нет дублей)
            exists = conn.execute(
                "SELECT id FROM daily_report WHERE number=? AND user_id=? AND report_date=?",
                (number, uid, today)
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO daily_report (number, user_id, username, report_date, payment_status, stood_at) VALUES (?,?,?,?,?,?)",
                    (number, uid, uname, today, "unpaid", now.isoformat())
                )
                log.info(f"daily_report INSERT: number={number} uid={uid} date={today}")
            else:
                conn.execute(
                    "UPDATE daily_report SET stood_at=?, payment_status='unpaid' WHERE number=? AND user_id=? AND report_date=? AND payment_status NOT IN ('paid','pending')",
                    (now.isoformat(), number, uid, today)
                )
                log.info(f"daily_report UPDATE: number={number} uid={uid} date={today}")

    _cancel_job(context, f"sms_timeout_{queue_id}")
    _cancel_job(context, f"qr_timeout_{queue_id}")

    if not row:
        return

    number, uid, uname, op_uname, op_id_val = row
    uname_safe = esc_md(uname) if uname else str(uid)
    op_str     = f"@{esc_md(op_uname)}" if op_uname else "—"
    op_id_str  = str(op_id_val) if op_id_val else "—"

    try:
        await context.bot.send_message(uid, f"✅ *Номер встал* `{number}`", parse_mode="Markdown")
    except: pass

    # Считаем сколько простоял на момент "Встал" (чтобы показать в группе)
    with db() as _c:
        _an = _c.execute("SELECT created_at FROM active_numbers WHERE queue_id=?", (queue_id,)).fetchone()
    _loaded = _an[0] if _an else None

    stood_text = (
        f"⚡ *Номер встал* ⚡\n\n"
        f"Номер: `{number}`\n"
        f"Время: {now_str}\n"
        f"Дроп: @{uname_safe}\n"
        f"Оператор: {op_str} | ID: `{op_id_str}`"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("💥 Слетел", callback_data=f"op_fell_{queue_id}"),
    ]])
    try:
        await qedit(q, stood_text, parse_mode="Markdown", reply_markup=kb)
    except Exception as e:
        if "not modified" not in str(e).lower():
            log.error(f"stood edit error: {e}")


async def cb_op_fell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оператор нажал Слетел."""
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    if not await _check_op_access(q, queue_id): return
    now = now_msk()

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username, stood_at, status FROM active_numbers WHERE queue_id=?",
            (queue_id,)
        ).fetchone()
        if row and row[4] == 'paid':
            await q.answer("⚠️ Номер уже оплачен — слёт не применён.", show_alert=True)
            return
        conn.execute("UPDATE active_numbers SET status='fell', fell_at=? WHERE queue_id=?",
                     (now.isoformat(), queue_id))
        conn.execute("UPDATE queue SET status='fell', deleted=1 WHERE id=?", (queue_id,))
        if row:
            _n, _uid, _uname, _stood, _ = row
            today_iso = date.today().isoformat()
            conn.execute(
                "UPDATE daily_report SET payment_status='fell', fell_at=? WHERE user_id=? AND number=? AND report_date=?",
                (now.isoformat(), _uid, _n, today_iso)
            )

    if not row:
        await q.answer("❌ Не найдено", show_alert=True); return

    number, uid, uname, stood_at_str, _ = row
    uname_safe = esc_md(uname) if uname else str(uid)

    fell_time = now.strftime("%H:%M:%S")
    hold_min = _safe_int(get_setting("hold"), 5)
    if stood_at_str:
        try:
            stood_dt = datetime.fromisoformat(stood_at_str)
            diff = now - stood_dt
            total_secs = diff.total_seconds()
            mins = int(total_secs // 60)
            secs = int(total_secs % 60)
            duration = f"{mins}м {secs}с"
            # Разделяем: до холда = отмена, после = слетел
            if total_secs < hold_min * 60:
                fell_label = "🚫 *Номер отменён*"
                fell_status_msg = f"Слетел до {hold_min} мин — номер отменён"
                # Меняем статус в daily_report на cancelled (не считаем как слёт)
                with db() as _c:
                    _c.execute(
                        "UPDATE daily_report SET payment_status='cancelled' WHERE user_id=? AND number=? AND report_date=?",
                        (uid, number, date.today().isoformat())
                    )
                    _c.execute(
                        "UPDATE active_numbers SET status='cancelled' WHERE queue_id=?",
                        (queue_id,)
                    )
            else:
                fell_label = "💥 *Номер слетел*"
                fell_status_msg = f"Слетел после {hold_min} мин — номер слетел"
        except:
            duration = "—"
            fell_label = "💥 *Номер слетел*"
            fell_status_msg = "Слетел"
    else:
        duration = "—"
        fell_label = "💥 *Номер слетел*"
        fell_status_msg = "Слетел"

    try:
        await context.bot.send_message(
            uid,
            f"{fell_label} `{number}`\n\n"
            f"Слетел: {fell_time} (МСК)\n"
            f"Простоял: {duration}",
            parse_mode="Markdown"
        )
    except: pass

    await safe_edit(
        q.edit_message_text,
        f"{fell_label} `{number}`\n\nДроп: @{uname_safe}\nПростоял: {duration}",
        parse_mode="Markdown"
    )


async def cb_op_not_stood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    if not await _check_op_access(q, queue_id): return

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='not_stood' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))

    _cancel_job(context, f"sms_timeout_{queue_id}")
    _cancel_job(context, f"qr_timeout_{queue_id}")

    if row:
        number, uid, uname = row
        context.bot_data.pop(f"wait_sms_{uid}", None)
        context.bot_data.pop(f"wait_qr_{uid}", None)
        try:
            await context.bot.send_message(uid, f"❌ *Номер не встал* `{number}`", parse_mode="Markdown")
        except: pass
        uname_safe_ns = esc_md(uname) if uname else str(uid)
        await safe_edit(
            q.edit_message_text,
            f"❌ *Номер `{number}` не встал*\nДроп: @{uname_safe_ns}",
            parse_mode="Markdown"
        )


async def cb_op_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("_")[-1])
    if not await _check_op_access(q, queue_id): return

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET status='cancelled' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))

    _cancel_job(context, f"sms_timeout_{queue_id}")
    _cancel_job(context, f"qr_timeout_{queue_id}")

    if row:
        number, uid, uname = row
        context.bot_data.pop(f"wait_sms_{uid}", None)
        context.bot_data.pop(f"wait_qr_{uid}", None)
        # Пишем в daily_report ТОЛЬКО если номер уже встал (stood_at задан)
        # Если отменили до того как код запросили — не пишем
        today_iso = date.today().isoformat()
        with db() as conn2:
            an_stood = conn2.execute(
                "SELECT stood_at FROM active_numbers WHERE queue_id=?", (queue_id,)
            ).fetchone()
            if an_stood and an_stood[0]:
                exists = conn2.execute(
                    "SELECT id FROM daily_report WHERE number=? AND user_id=? AND report_date=?",
                    (number, uid, today_iso)
                ).fetchone()
                if not exists:
                    conn2.execute(
                        "INSERT INTO daily_report (number, user_id, username, report_date, payment_status) VALUES (?,?,?,?,?)",
                        (number, uid, uname, today_iso, "cancelled")
                    )
                else:
                    conn2.execute(
                        "UPDATE daily_report SET payment_status='cancelled' WHERE number=? AND user_id=? AND report_date=?",
                        (number, uid, today_iso)
                    )
        try:
            await context.bot.send_message(uid, "❌ *Номер отменён оператором.*", parse_mode="Markdown")
        except: pass
        uname_safe_cn = esc_md(uname) if uname else str(uid)
        await safe_edit(
            q.edit_message_text,
            f"❌ *Номер отменён*\n\nНомер: `{number}`\nДроп: @{uname_safe_cn}",
            parse_mode="Markdown"
        )


# ═══════════════════════════════════════════════════════════
#  📩  ПЕРЕХВАТ СМС КОДА ОТ ДРОПА
# ═══════════════════════════════════════════════════════════
async def handle_sms_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid     = update.effective_user.id
    pending = context.bot_data.get(f"sms_pending_{uid}", {})

    # Определяем queue_id по replied-to сообщению
    reply    = update.message.reply_to_message
    queue_id = None
    if reply and reply.message_id in pending:
        queue_id = pending[reply.message_id]
    elif len(pending) == 1:
        # Только один активный запрос — принимаем даже без reply
        queue_id = next(iter(pending.values()))

    if not queue_id:
        return False

    code = update.message.text.strip()

    # Только 6 цифр
    if not code.isdigit() or len(code) != 6:
        await update.message.reply_text(
            "❌ Код должен быть ровно *6 цифр*.\n\nОтветьте на сообщение бота и введите 6-значный код.",
            parse_mode="Markdown"
        )
        return True
    with db() as conn:
        row = conn.execute(
            "SELECT number, username, chat_id, topic_id, group_msg_id FROM active_numbers WHERE queue_id=?",
            (queue_id,)
        ).fetchone()
        conn.execute("UPDATE active_numbers SET sms_code=?, status='sms_sent' WHERE queue_id=?", (code, queue_id))

    # Удаляем только этот queue_id из pending dict
    keys_to_remove = [k for k, v in list(pending.items()) if v == queue_id]
    for k in keys_to_remove:
        pending.pop(k, None)
    if not pending:
        context.bot_data.pop(f"sms_pending_{uid}", None)
    context.bot_data.pop(f"wait_sms_{uid}", None)
    context.bot_data.pop(f"wait_sms_msg_{uid}", None)
    _cancel_job(context, f"sms_timeout_{queue_id}")

    await update.message.reply_text("✅ *Код отправлен админу. Жди решения.*", parse_mode="Markdown")

    if not row:
        return True

    number, uname, chat_id, topic_id, group_msg_id = row
    uname_safe = esc_md(uname) if uname else str(uid)
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Встал",        callback_data=f"op_stood_{queue_id}"),
        InlineKeyboardButton("🔁 Повтор кода", callback_data=f"op_repeat_sms_{queue_id}"),
    ], [
        InlineKeyboardButton("❌ Не встал",     callback_data=f"op_not_stood_{queue_id}"),
    ]])
    try:
        send_kw = {
            "chat_id": chat_id,
            "text": (
                f"⚡ 📩 *СМС код введён* ⚡\n\nНомер: `{number}`\nКод: `{code}`\n"
                f"Дроп: @{uname_safe}"
            ),
            "parse_mode": "Markdown",
            "reply_markup": kb,
        }
        if topic_id:
            send_kw["message_thread_id"] = topic_id
        if group_msg_id:
            send_kw["reply_to_message_id"] = group_msg_id
        await context.bot.send_message(**send_kw)
    except Exception as e:
        log.error(f"Не смог отправить код в группу: {e}")

    return True


# ═══════════════════════════════════════════════════════════
#  ✋  ПОДТВЕРЖДЕНИЕ АКТИВНОСТИ (3-й в очереди)
# ═══════════════════════════════════════════════════════════
CONFIRM_TIMEOUT = 15  # секунд на подтверждение

async def _notify_number_taken(context, queue_id: int, number: str, uid: int):
    """Просто уведомляем дропа что номер взят — без подтверждения."""
    try:
        await context.bot.send_message(
            uid,
            f"🟩 Ваш номер `{number}` взят оператором.\n\nОжидайте запроса кода.",
            parse_mode="Markdown"
        )
    except Exception as e:
        log.error(f"Не смог уведомить дропа о взятии номера: {e}")


async def _timeout_confirm(context):
    """Дроп не подтвердил — отменяем номер, ищем замену."""
    data     = context.job.data
    queue_id = data["queue_id"]
    uid      = data["uid"]
    number   = data["number"]

    with db() as conn:
        row = conn.execute(
            "SELECT status FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()
        if row and row[0] == "confirmed":
            # Дроп подтвердил — просто уведомляем нового 3-го
            await _notify_pos3_if_needed(context)
            return
        conn.execute("UPDATE active_numbers SET status='cancelled' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))

    # Чистим флаг pos3_sent для этого queue_id
    context.bot_data.pop(f"pos3_sent_{queue_id}", None)

    msg_id = context.bot_data.pop(f"confirm_msg_{uid}", None)
    if msg_id:
        try:
            await context.bot.delete_message(chat_id=uid, message_id=msg_id)
        except: pass

    try:
        await context.bot.send_message(
            uid,
            f"❌ Номер `{number}` отменён — вы не подтвердили готовность.\n\nСдайте новый номер.",
            parse_mode="Markdown"
        )
    except: pass

    log.info(f"Confirm timeout: номер {number} (qid={queue_id}) отменён")

    # Уведомляем нового 3-го
    await _notify_pos3_if_needed(context)


async def cb_confirm_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дроп нажал Подтверждаю."""
    q = update.callback_query
    await q.answer("✅")
    queue_id = int(q.data.split("_")[-1])
    uid      = q.from_user.id

    _cancel_job(context, f"confirm_timeout_{queue_id}")
    context.bot_data.pop(f"confirm_msg_{uid}", None)

    with db() as conn:
        row = conn.execute("SELECT number FROM active_numbers WHERE queue_id=?", (queue_id,)).fetchone()
        conn.execute("UPDATE active_numbers SET status='confirmed' WHERE queue_id=?", (queue_id,))

    number = row[0] if row else "—"
    await safe_edit(
        q.edit_message_text,
        f"✅ *Подтверждено!*\n\nНомер `{number}` — жди запроса кода.",
        parse_mode="Markdown"
    )


# ═══════════════════════════════════════════════════════════
#  🏠  USER — START
# ═══════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════
#  🆘  ТЕХ ПОДДЕРЖКА
# ═══════════════════════════════════════════════════════════
async def cb_user_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    context.bot_data[f"wait_support_{uid}"] = "waiting_text"
    context.bot_data.pop(f"support_text_{uid}", None)
    kb = [[InlineKeyboardButton("⬅️ В меню", callback_data="back_main")]]
    await qedit(q, 
        "🆘 *Тех поддержка*\n\nОпишите проблему одним сообщением.\nПосле этого появится кнопка *Отправить*.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def handle_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Перехват сообщения от дропа для поддержки."""
    uid   = update.effective_user.id
    state = context.bot_data.get(f"wait_support_{uid}")
    if not state:
        return False

    text = update.message.text.strip()

    # Сохраняем текст и показываем кнопку Отправить
    context.bot_data[f"support_text_{uid}"] = text
    context.bot_data[f"wait_support_{uid}"] = "waiting_confirm"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить", callback_data="support_send")],
        [InlineKeyboardButton("✏️ Изменить текст", callback_data="user_support")],
        [InlineKeyboardButton("❌ Отмена", callback_data="back_main")],
    ])
    await update.message.reply_text(
        f"📝 *Ваше обращение:*\n\n{text}\n\nНажмите *Отправить* для подтверждения.",
        parse_mode="Markdown",
        reply_markup=kb
    )
    return True


async def cb_support_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал кнопку Отправить обращение."""
    q    = update.callback_query
    await q.answer()
    uid  = q.from_user.id
    user = q.from_user
    text = context.bot_data.pop(f"support_text_{uid}", None)
    context.bot_data.pop(f"wait_support_{uid}", None)

    if not text:
        kb = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]]
        await qedit(q, 
            "❌ Текст обращения не найден. Попробуйте снова.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    sup_id = get_setting("support_id")
    kb = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]]
    await qedit(q, 
        "✅ *Обращение отправлено.* Ожидайте ответа.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )

    if not sup_id:
        return

    bot_user  = await context.bot.get_me()
    reply_url = f"https://t.me/{bot_user.username}?start=reply_{uid}"
    ticket_kb = InlineKeyboardMarkup([[InlineKeyboardButton("💬 Ответить", url=reply_url)]])
    ticket_text = (
        f"🆘 *НОВОЕ ОБРАЩЕНИЕ*\n\n"
        f"👤 Пользователь: @{user.username or '—'}\n"
        f"🆔 ID: `{uid}`\n\n"
        f"📝 Проблема: {text}"
    )
    try:
        await context.bot.send_message(
            chat_id=sup_id,
            text=ticket_text,
            parse_mode="Markdown",
            reply_markup=ticket_kb
        )
    except Exception as e:
        log.error(f"Не смог отправить тикет: {e}")


async def cmd_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ответ на обращение: /reply 123456789 текст ответа"""
    if not is_admin(update.effective_user.id):
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "❌ Использование: `/reply ID текст`\n\nПример: `/reply 123456789 Проблема решена`",
            parse_mode="Markdown"
        )
        return
    try:
        target_uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом.")
        return
    answer_text = " ".join(context.args[1:])
    kb = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]]
    try:
        await context.bot.send_message(
            target_uid,
            f"🆘 *От Тех Поддержки*\n\n📜 Ответ: {answer_text}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        await update.message.reply_text(f"✅ Ответ отправлен пользователю `{target_uid}`.", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Не смог отправить: {e}")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    user = update.effective_user
    register_user(user)

    # Сохраняем uid в PicklePersistence — независимо от состояния БД
    all_ids = context.bot_data.setdefault("all_user_ids", set())
    all_ids.add(user.id)

    # Обработка /start reply_{uid} — ответ от поддержки
    if context.args and context.args[0].startswith("reply_"):
        if is_admin(user.id):
            target_uid = int(context.args[0].split("_")[1])
            context.user_data["support_reply_to"] = target_uid
            kb = [[InlineKeyboardButton("❌ Отмена", callback_data="back_main")]]
            await update.message.reply_text(
                f"💬 *Ответ на обращение*\n\nПользователь ID: `{target_uid}`\n\nНапишите ответ сообщением:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb)
            )
            return
        # Не админ — обычный /start
    if is_banned(user.id):
        await update.message.reply_text("🚫 Вы заблокированы.")
        return

    if not await check_sub(user.id, context):
        sub_url = safe_url(get_setting("sub_url"), "https://t.me")
        kb = [[InlineKeyboardButton("📢 Подписаться", url=sub_url)],
              [InlineKeyboardButton("✅ Проверить подписку", callback_data="check_sub")]]
        await update.message.reply_text(
            "⚠️ Для использования бота подпишитесь на канал!",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    await _send_main_menu(update.effective_chat.id, context)

async def cb_check_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if await check_sub(q.from_user.id, context):
        try:
            await q.message.delete()
        except: pass
        await _send_main_menu(q.message.chat_id, context)
    else:
        await q.answer("❌ Вы ещё не подписались!", show_alert=True)

async def cb_back_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    # Чистим все флаги ожидания при возврате в меню
    context.user_data.pop("support_reply_to", None)
    context.bot_data.pop(f"wait_support_{uid}", None)
    context.bot_data.pop(f"wait_phone_{uid}", None)
    await _edit_main_menu(q, context)

async def _main_menu_data():
    bot_on = get_setting("bot_status") == "on"
    cnt    = queue_count()
    tariff = get_setting("tariff") or "4.0"
    emoji_on  = '<tg-emoji emoji-id="5278411813468269386">🟢</tg-emoji>'
    emoji_off = '<tg-emoji emoji-id="5278578973595427038">🔴</tg-emoji>'
    status = f"├ Статус: {emoji_on} Работаем" if bot_on else f"├ Статус: {emoji_off} Не работаем"
    text   = (
        f"⚡ <b>Главное меню</b> ⚡\n\n"
        f'<tg-emoji emoji-id="5276412364458059956">⏳</tg-emoji> Очередь: <b>{cnt}</b> номеров\n\n'
        f"{status}\n"
        f"└ Прайс: <b>{tariff}$</b> за номер"
    )
    ch_url = safe_url(get_setting("channel_url"), "https://t.me")
    kb = [
        [InlineKeyboardButton("📲 Сдать номер", callback_data="user_phone")],
        [InlineKeyboardButton("📋 История", callback_data="user_history"),
         InlineKeyboardButton("👥 Очередь", callback_data="user_stats")],
        [InlineKeyboardButton("🏆 Топеры", callback_data="user_leaders")],
        [InlineKeyboardButton("🆘 Тех поддержка", callback_data="user_support")],
        [InlineKeyboardButton("📢 Канал", url=ch_url)],
    ]
    return text, InlineKeyboardMarkup(kb)

async def _send_main_menu(chat_id, context):
    t, m = await _main_menu_data()
    # Пробуем отправить с фото
    photo_fid = context.bot_data.get("main_menu_photo_fid")
    try:
        if photo_fid:
            await context.bot.send_photo(chat_id, photo=photo_fid, caption=t, parse_mode="HTML", reply_markup=m)
            return
        elif os.path.exists(MAIN_MENU_PHOTO):
            with open(MAIN_MENU_PHOTO, "rb") as f:
                msg = await context.bot.send_photo(chat_id, photo=f, caption=t, parse_mode="HTML", reply_markup=m)
            context.bot_data["main_menu_photo_fid"] = msg.photo[-1].file_id
            return
    except Exception as e:
        log.error(f"_send_main_menu photo error: {e}")
    await context.bot.send_message(chat_id, t, parse_mode="HTML", reply_markup=m)

async def _edit_main_menu(q, context):
    t, m = await _main_menu_data()
    # Определяем тип текущего сообщения и редактируем соответственно
    is_photo = bool(q.message.photo if q.message else False)
    try:
        if is_photo:
            await q.edit_message_caption(caption=t, parse_mode="HTML", reply_markup=m)
        else:
            await q.edit_message_text(t, parse_mode="HTML", reply_markup=m)
    except Exception as e:
        if "not modified" in str(e).lower():
            return
        # Fallback — удаляем и шлём новое
        log.warning(f"_edit_main_menu fallback: {e}")
        try:
            await q.message.delete()
        except: pass
        await _send_main_menu(q.message.chat_id, context)

# ── User: Phone (без ConversationHandler — через флаги) ───
async def cb_user_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if get_setting("bot_status") != "on":
        await qedit(q, 
            "❌ *Приёмка не работает.*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ В меню", callback_data="back_main")
            ]])
        )
        return

    kb = [
        [InlineKeyboardButton("💬 СМС", callback_data="phone_type_sms"),
         InlineKeyboardButton("📷 QR",  callback_data="phone_type_qr")],
        [InlineKeyboardButton("⚡ Без очереди (СМС)", callback_data="phone_type_sms_skip"),
         InlineKeyboardButton("⚡ Без очереди (QR)",  callback_data="phone_type_qr_skip")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="back_main")],
    ]
    await qedit(q,
        "⚡ *Сдать номер* ⚡\n\nВыбери способ сдачи:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def cb_phone_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid   = q.from_user.id
    skip  = q.data.endswith("_skip")
    ptype = "sms" if "sms" in q.data else "qr"

    if ptype == "sms" and get_setting("accept_sms") != "on":
        await q.answer("⛔ Приём СМС отключён!", show_alert=True)
        return
    if ptype == "qr" and get_setting("accept_qr") != "on":
        await q.answer("⛔ Приём QR отключён!", show_alert=True)
        return

    await q.answer()
    # skip=True — сдаём без очереди (приоритет)
    context.bot_data[f"wait_phone_{uid}"] = ptype
    context.bot_data[f"skip_queue_{uid}"] = skip

    tariff_skip = get_setting("tariff_skip") or "3.5"
    skip_note = f"\n\n⚡ *Режим: Без Очереди* ⚡\n\n⚠️ Внимание: прайс за услугу — {tariff_skip}$" if skip else ""
    kb = [[InlineKeyboardButton("⬅️ В меню", callback_data="back_main")]]
    await qedit(q,
        f"⚡ *Номер* ⚡\n\nОтправь номер в формате:\n`+79991234567`{skip_note}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def handle_phone_number(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Перехват номера от пользователя. Возвращает True если обработал."""
    uid   = update.effective_user.id
    ptype = context.bot_data.get(f"wait_phone_{uid}")
    if not ptype:
        return False

    user   = update.effective_user
    number = update.message.text.strip()

    # Убираем флаг сразу
    context.bot_data.pop(f"wait_phone_{uid}", None)

    if is_banned(uid):
        await update.message.reply_text("🚫 Вы заблокированы.")
        return True

    # Валидация — убираем флаг сразу, при ошибке не возвращаем
    cleaned = number.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    digits  = cleaned.lstrip("+")
    if not digits.isdigit() or len(digits) < 7:
        has_digits = any(c.isdigit() for c in number)
        if has_digits:
            # Есть цифры но формат неверный — говорим об ошибке и продолжаем ждать
            context.bot_data[f"wait_phone_{uid}"] = ptype
            await update.message.reply_text(
                "❌ Не похоже на номер телефона.\n\nОтправь в формате: `+79991234567`",
                parse_mode="Markdown"
            )
        else:
            # Текст без цифр (буквы, стикеры и т.д.) — тихо игнорируем, флаг НЕ сбрасываем
            context.bot_data[f"wait_phone_{uid}"] = ptype
        return True

    # Нормализуем: 89xxx → +79xxx
    if cleaned.startswith("8") and not cleaned.startswith("+"):
        cleaned = "+7" + cleaned[1:]
    if not cleaned.startswith("+"):
        cleaned = "+" + cleaned

    # Только российские мобильные +79xxxxxxxxx (12 символов с +)
    if not (cleaned.startswith("+79") and len(cleaned) == 12 and cleaned[1:].isdigit()):
        context.bot_data[f"wait_phone_{uid}"] = ptype
        await update.message.reply_text(
            "❌ Принимаются только российские номера в формате: `+79991234567`",
            parse_mode="Markdown"
        )
        return True

    now    = now_msk()
    status = "wait_sms" if ptype == "sms" else "wait_qr"
    # Без очереди — вставляем с минимальным id (приоритет)

    # Проверка — включён ли приём этого типа
    if ptype == "sms" and get_setting("accept_sms") != "on":
        await update.message.reply_text("⛔ *Приём СМС номеров отключён.*", parse_mode="Markdown")
        return True
    if ptype == "qr" and get_setting("accept_qr") != "on":
        await update.message.reply_text("⛔ *Приём QR номеров отключён.*", parse_mode="Markdown")
        return True

    uid_int = user.id
    today_iso = date.today().isoformat()
    skip_queue = context.bot_data.pop(f"skip_queue_{uid_int}", False)


    # 2) Этот номер уже был сегодня (блок повтора)
    #    Проверяем и daily_report, и queue (включая удалённые/отменённые за сегодня)
    with db() as conn:
        already_today = conn.execute(
            "SELECT id FROM daily_report WHERE number=? AND report_date=?",
            (cleaned, today_iso)
        ).fetchone()
        if not already_today:
            already_today = conn.execute(
                "SELECT id FROM queue WHERE number=? AND user_id=? AND DATE(loaded_at)=?",
                (cleaned, uid_int, today_iso)
            ).fetchone()
    if already_today:
        await update.message.reply_text(
            f"⛔ Номер `{cleaned}` уже был сегодня.",
            parse_mode="Markdown"
        )
        return True

    # 3) Дубль — номер уже в очереди (от другого дропа)
    with db() as conn:
        existing = conn.execute(
            "SELECT id FROM queue WHERE number=? AND deleted=0 AND status IN ('wait_sms','wait_qr','taken')",
            (cleaned,)
        ).fetchone()
    if existing:
        await update.message.reply_text(
            f"❌ *Номер `{cleaned}` уже стоит в очереди.*\n\nДождись его обработки.",
            parse_mode="Markdown"
        )
        return True

    with db() as conn:
        if skip_queue:
            old_time = (now - timedelta(hours=24)).isoformat()
            cursor = conn.execute(
                "INSERT INTO queue (number, status, user_id, operator, loaded_at, skip_queue) VALUES (?,?,?,?,?,1)",
                (cleaned, status, user.id, user.username or str(user.id), old_time)
            )
        else:
            cursor = conn.execute(
                "INSERT INTO queue (number, status, user_id, operator, loaded_at, skip_queue) VALUES (?,?,?,?,?,0)",
                (cleaned, status, user.id, user.username or str(user.id), now.isoformat())
            )
        queue_id = cursor.lastrowid
        # daily_report создаётся ТОЛЬКО при "Встал" (cb_op_stood)

    kb = [[InlineKeyboardButton("🏠 Главное меню", callback_data="back_main")]]
    skip_msg = " ⚡ *Без очереди — будешь первым!*" if skip_queue else ""
    await update.message.reply_text(
        f"✅ *Номер принят. Жди когда оператор возьмёт.*{skip_msg}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )

    # Проверка позиции в очереди — уведомление если ровно 3-й
    try:
        with db() as conn:
            pos_rows = conn.execute(
                "SELECT id, user_id FROM queue WHERE status IN ('wait_sms','wait_qr') AND deleted=0 ORDER BY id ASC"
            ).fetchall()
        pos = next((i+1 for i, (qid, _) in enumerate(pos_rows) if qid == queue_id), None)
        log.info(f"Новый номер qid={queue_id} встал на позицию {pos} из {len(pos_rows)}")
        if pos == 3:
            kb3 = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Я здесь", callback_data=f"confirm_active_{queue_id}"),
            ]])
            sent_c = await context.bot.send_message(
                user.id,
                f"⚡ *Твой номер 3-й в очереди*\n\nНомер: `{cleaned}`\n\n"
                f"Подтверди присутствие за *15 секунд*, иначе номер уйдёт в конец.",
                parse_mode="Markdown",
                reply_markup=kb3
            )
            context.bot_data[f"confirm_msg_{user.id}"] = sent_c.message_id
            _cancel_job(context, f"confirm_timeout_{queue_id}")
            context.job_queue.run_once(
                _timeout_confirm, 15,
                data={"queue_id": queue_id, "uid": user.id, "number": cleaned},
                name=f"confirm_timeout_{queue_id}"
            )
            log.info(f"✅ Отправлено подтверждение дропу uid={user.id} на позицию 3")
    except Exception as e:
        log.error(f"Ошибка проверки позиции: {e}")

    # Уведомления отключены

    return True




async def cb_user_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    with db() as conn:
        rows = conn.execute(
            """SELECT an.number, COALESCE(an.stood_at, an.created_at), COALESCE(dr.paid, 0)
               FROM active_numbers an
               LEFT JOIN daily_report dr ON dr.user_id=an.user_id AND dr.number=an.number
               WHERE an.user_id=? AND an.status IN ('stood','paid')
               ORDER BY COALESCE(an.stood_at, an.created_at) DESC LIMIT 50""",
            (uid,)
        ).fetchall()
        total_paid = conn.execute(
            "SELECT COALESCE(SUM(paid),0) FROM daily_report WHERE user_id=? AND paid>0", (uid,)
        ).fetchone()[0]
        pending = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE user_id=? AND status IN ('stood','paid')",
            (uid,)
        ).fetchone()[0]

    kb = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]]
    if not rows:
        await qedit(q, 
            "*История номеров* 📜\n\nПока нет встоявших номеров.",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    lines = ["*История номеров* 📜\n"]
    for number, stood_at, paid in rows:
        t = "—"
        if stood_at:
            try: t = datetime.fromisoformat(stood_at).strftime("%d.%m %H:%M")
            except: pass
        lines.append(f"`{number}` | {t} | 🕛")

    lines.append(
        f"\n📱 Всего номеров: *{len(rows)}*\n"
        f"✅ Получено выплат: *${total_paid:.2f}*\n"
        f"⏳ Ожидают выплаты: *{pending}*"
    )
    await qedit(q, "\n".join(lines), parse_mode="Markdown",
                              reply_markup=InlineKeyboardMarkup(kb))


async def cb_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очередь — дроп видит позицию своих номеров в общей очереди."""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

    with db() as conn:
        # Все номера в очереди с их позицией
        all_rows = conn.execute(
            """SELECT id, user_id FROM queue
               WHERE status IN ('wait_sms','wait_qr') AND deleted=0
               ORDER BY id ASC"""
        ).fetchall()

        # Номера именно этого дропа
        my_rows = conn.execute(
            """SELECT q.id, q.number, q.loaded_at, q.status
               FROM queue q
               WHERE q.user_id=? AND q.deleted=0
               AND q.status IN ('wait_sms','wait_qr')
               ORDER BY q.id ASC""",
            (uid,)
        ).fetchall()

    kb = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]]

    if not my_rows:
        await qedit(q, 
            "👥 *ОЧЕРЕДЬ*\n\nУ тебя нет номеров в очереди.",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    # Строим карту позиций
    pos_map = {row[0]: i+1 for i, row in enumerate(all_rows)}
    total = len(all_rows)

    lines = [f"👥 *ОЧЕРЕДЬ* (всего: {total})\n"]
    for qid, number, loaded_at, status in my_rows:
        pos = pos_map.get(qid, "?")
        t = "—"
        if loaded_at:
            try:
                t = datetime.fromisoformat(loaded_at).strftime("%H:%M")
            except: pass
        type_icon = "💬" if status == "wait_sms" else "📷"
        lines.append(f"*#{pos}* {type_icon} `{number}` | {t}")

    await qedit(q, 
        "\n".join(lines), parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )



# ═══════════════════════════════════════════════════════════
#  🏆  ЛИДЕРБОРД (Топеры)
# ═══════════════════════════════════════════════════════════
def _mask_username(uname: str) -> str:
    """Маскирует юзернейм: @penisgt → @peni####"""
    if not uname:
        return "—"
    visible = max(4, len(uname) // 2)
    return "@" + uname[:visible] + "#" * (len(uname) - visible)

async def cb_user_leaders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    today = date.today().isoformat()
    with db() as conn:
        # Топ по встоявшим за сегодня
        rows = conn.execute(
            """SELECT dr.username, COUNT(*) as cnt
               FROM daily_report dr
               WHERE dr.report_date=? AND dr.payment_status NOT IN ('cancelled')
               AND dr.username IS NOT NULL
               GROUP BY dr.user_id
               ORDER BY cnt DESC LIMIT 10""",
            (today,)
        ).fetchall()
        # Позиция текущего юзера
        my_row = conn.execute(
            """SELECT COUNT(*) FROM daily_report
               WHERE report_date=? AND user_id=? AND payment_status NOT IN ('cancelled')""",
            (today, uid)
        ).fetchone()

    my_cnt = my_row[0] if my_row else 0
    kb = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]]

    if not rows:
        await qedit(q,
            "🏆 *Топеры*\n\nСегодня никто ещё не встал.",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    medals = ["🥇", "🥈", "🥉"] + ["4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["🏆 *Топеры за сегодня*\n"]
    for i, (uname, cnt) in enumerate(rows):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        masked = _mask_username(uname)
        lines.append(f"{medal} {masked} — *{cnt}* номеров")

    lines.append(f"\n👤 Ты сегодня: *{my_cnt}* номеров")

    await qedit(q, "\n".join(lines), parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb))


async def cb_adm_leaders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топеры для админа — полные юзернеймы + кол-во оплаченных."""
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    today = date.today().isoformat()
    with db() as conn:
        rows = conn.execute(
            """SELECT dr.username, dr.user_id, COUNT(*) as cnt,
                      SUM(CASE WHEN dr.payment_status='paid' THEN 1 ELSE 0 END) as paid_cnt
               FROM daily_report dr
               WHERE dr.report_date=? AND dr.payment_status NOT IN ('cancelled')
               GROUP BY dr.user_id
               ORDER BY cnt DESC LIMIT 20""",
            (today,)
        ).fetchall()

    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_admin")]]
    if not rows:
        await qedit(q, "🏆 *Топеры*\n\nСегодня пусто.", parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(kb))
        return

    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 21)]
    lines2 = [f"🏆 *Топеры за {today}*\n"]
    for i, (uname, uid2, cnt, paid_cnt) in enumerate(rows):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        name = f"@{esc_md(uname)}" if uname else f"ID:{uid2}"
        lines2.append(f"{medal} {name} — *{cnt}* номеров \\(💰 {paid_cnt} опл\\.\\)")

    await qedit(q, "\n".join(lines2), parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup(kb))


async def cb_adm_topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список топиков с инфой."""
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    today = date.today().isoformat()

    with db() as conn:
        chats = conn.execute(
            "SELECT id, chat_id, topic_id, name, added_at FROM work_chats ORDER BY id ASC"
        ).fetchall()

    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_admin")]]
    if not chats:
        await qedit(q,
            "📡 *Топики*\n\nНет добавленных ворк-чатов.\n\nИспользуй `/setmax` в нужном топике.",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return

    parts = ["📡 *Ворк-топики*\n"]
    for _, chat_id, topic_id, name, added_at in chats:
        with db() as conn2:
            taken = conn2.execute(
                "SELECT COUNT(*) FROM active_numbers WHERE chat_id=? AND topic_id=? AND DATE(created_at)=?",
                (chat_id, topic_id, today)
            ).fetchone()[0]
            stood = conn2.execute(
                "SELECT COUNT(*) FROM active_numbers WHERE chat_id=? AND topic_id=? AND status IN ('stood','paid') AND DATE(stood_at)=?",
                (chat_id, topic_id, today)
            ).fetchone()[0]
            fell = conn2.execute(
                "SELECT COUNT(*) FROM active_numbers WHERE chat_id=? AND topic_id=? AND status='fell' AND DATE(stood_at)=?",
                (chat_id, topic_id, today)
            ).fetchone()[0]
            in_q = conn2.execute(
                """SELECT COUNT(*) FROM queue q
                   JOIN active_numbers an ON an.queue_id=q.id
                   WHERE an.chat_id=? AND an.topic_id=? AND q.deleted=0
                   AND q.status IN ('wait_sms','wait_qr','taken')""",
                (chat_id, topic_id)
            ).fetchone()[0]

        label = name if name else f"topic_id {topic_id}"
        added = added_at[:10] if added_at else "—"
        parts.append(
            f"\n📌 *{esc_md(label)}*\n"
            f"  chat\\_id: `{chat_id}` | topic\\_id: `{topic_id}`\n"
            f"  Добавлен: {added}\n"
            f"  Очередь: *{in_q}* | Взято: *{taken}* | Встало: *{stood}* | Слетело: *{fell}*"
        )

    parts.append("\n\n_/settopic Название — переименовать текущий топик_")
    await qedit(q, "\n".join(parts), parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb))


# ═══════════════════════════════════════════════════════════
#  🔧  ADMIN PANEL
# ═══════════════════════════════════════════════════════════
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    await _send_admin(update.message.reply_text)

async def cb_back_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    await _edit_admin(q.edit_message_text)

async def _admin_data():
    bot_on = get_setting("bot_status") == "on"
    notif  = get_setting("notifications") == "on"
    status_txt = "✅ РАБОТАЕМ" if bot_on else "⛔ НЕ РАБОТАЕМ"
    notif_txt  = "🔔 Вкл" if notif else "🔕 Выкл"
    text = (
        "⚡ *Админ панель* ⚡\n\n"
        "Команды\n`/ban @user // ID`\n`/unban @user // ID`\n`/number`\n\n"
        f"Статус: {status_txt}"
    )
    kb = [
        [InlineKeyboardButton(f"Статус: {status_txt}", callback_data="adm_toggle_status")],
        [InlineKeyboardButton("✉️ Сообщение пользователю", callback_data="adm_msg_user")],
        [InlineKeyboardButton("📣 Рассылка / База", callback_data="adm_broadcast_menu")],
        [InlineKeyboardButton("📋 Очередь", callback_data="adm_queue")],
        [InlineKeyboardButton("🚫 Бан / Разбан", callback_data="adm_ban")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="adm_settings")],
        [InlineKeyboardButton("💸 Выплаты", callback_data="adm_payments")],
        [InlineKeyboardButton("🏆 Топеры", callback_data="adm_leaders"),
         InlineKeyboardButton("📡 Топики", callback_data="adm_topics")],
    ]
    return text, InlineKeyboardMarkup(kb)

async def _send_admin(send_fn):
    t, m = await _admin_data()
    await send_fn(t, parse_mode="Markdown", reply_markup=m)

async def _edit_admin(edit_fn):
    t, m = await _admin_data()
    await edit_fn(t, parse_mode="Markdown", reply_markup=m)

# ── Toggle Status ──────────────────────────────────────────
async def cb_adm_toggle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer("Нет доступа!", show_alert=True); return
    await q.answer()
    set_setting("bot_status", "off" if get_setting("bot_status") == "on" else "on")
    await _edit_admin(q.edit_message_text)

async def cb_adm_toggle_notif(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer("Нет доступа!", show_alert=True); return
    await q.answer()
    set_setting("notifications", "off" if get_setting("notifications") == "on" else "on")
    await _edit_admin(q.edit_message_text)

# ── Message to User ────────────────────────────────────────
async def cb_adm_msg_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    kb = [[InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_admin")]]
    await qedit(q, 
        "⚡ *Сообщение пользователю* ⚡\n\n"
        "Отправьте `@username` // `ID`\n"
        "Пример: `@username` или `123456789`",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
    )
    return WAIT_MSG_TARGET

async def wait_msg_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return ConversationHandler.END
    context.user_data["msg_target"] = update.message.text.strip()
    kb = [[InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_admin")]]
    await update.message.reply_text("✉️ Теперь введите текст сообщения:", reply_markup=InlineKeyboardMarkup(kb))
    return WAIT_MSG_TEXT

async def wait_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return ConversationHandler.END
    target = context.user_data.get("msg_target", "")
    text   = update.message.text
    with db() as conn:
        if target.startswith("@"):
            row = conn.execute("SELECT user_id FROM users WHERE username=?", (target.lstrip("@"),)).fetchone()
        else:
            row = conn.execute("SELECT user_id FROM users WHERE user_id=?", (int(target) if target.isdigit() else -1,)).fetchone()
    if not row:
        await update.message.reply_text("❌ Пользователь не найден.")
    else:
        try:
            await context.bot.send_message(row[0], f"📨 *Сообщение от администратора:*\n\n{text}", parse_mode="Markdown")
            await update.message.reply_text("✅ Сообщение доставлено!")
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}")
    kb = [[InlineKeyboardButton("🏠 В меню", callback_data="back_admin")]]
    await update.message.reply_text("⚡", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

# ── Broadcast ──────────────────────────────────────────────
async def cb_adm_broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    kb = [
        [InlineKeyboardButton("📣 Сделать рассылку",     callback_data="adm_broadcast")],
        [InlineKeyboardButton("💾 Скачать базу данных",  callback_data="adm_db_download")],
        [InlineKeyboardButton("📤 Загрузить базу данных",callback_data="adm_db_upload")],
        [InlineKeyboardButton("⬅️ Назад",                callback_data="back_admin")],
    ]
    await qedit(q, "⚡ *Рассылка / База данных* ⚡", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def cb_adm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="adm_broadcast_menu")]]
    await qedit(q,
        "⚡ *Рассылка* ⚡\n\nОтправьте текст рассылки.\nMarkdown поддерживается.",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
    )
    return WAIT_BROADCAST

async def wait_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return ConversationHandler.END
    text = update.message.text
    persistent_ids: set = context.bot_data.get("all_user_ids", set())
    with db() as conn:
        db_ids = {r[0] for r in conn.execute("SELECT user_id FROM users WHERE is_banned=0").fetchall()}
        banned = {r[0] for r in conn.execute("SELECT user_id FROM users WHERE is_banned=1").fetchall()}
    users = list((persistent_ids | db_ids) - banned)
    sent = failed = 0
    for uid in users:
        try:
            await context.bot.send_message(uid, text, parse_mode="Markdown")
            sent += 1
        except:
            failed += 1
    kb = [[InlineKeyboardButton("🏠 В меню", callback_data="back_admin")]]
    await update.message.reply_text(
        f"✅ Рассылка завершена!\n📤 Отправлено: {sent}\n❌ Ошибок: {failed}",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return ConversationHandler.END

async def cb_adm_db_download(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    if not os.path.exists(DB_PATH):
        await context.bot.send_message(q.from_user.id, "❌ База данных не найдена.")
        return
    with open(DB_PATH, "rb") as f:
        await context.bot.send_document(
            q.from_user.id, document=f,
            filename="bot_backup.db",
            caption=f"💾 База данных | {now_msk().strftime('%d.%m.%Y %H:%M')}"
        )

async def cb_adm_db_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    context.bot_data[f"wait_db_upload_{q.from_user.id}"] = True
    kb = [[InlineKeyboardButton("❌ Отмена", callback_data="adm_broadcast_menu")]]
    await qedit(q,
        "📤 *Загрузка базы*\n\nОтправьте файл `bot_backup.db` следующим сообщением.\n\n⚠️ Текущая база будет заменена!",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
    )

async def handle_db_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.effective_user.id
    if not is_admin(uid): return False
    if not context.bot_data.get(f"wait_db_upload_{uid}"): return False
    if not update.message or not update.message.document: return False
    doc = update.message.document
    if not doc.file_name or not doc.file_name.endswith(".db"):
        await update.message.reply_text("❌ Нужен файл с расширением .db")
        return True
    context.bot_data.pop(f"wait_db_upload_{uid}", None)
    try:
        file = await context.bot.get_file(doc.file_id)
        if os.path.exists(DB_PATH):
            os.replace(DB_PATH, DB_PATH + ".bak")
        await file.download_to_drive(DB_PATH)
        kb = [[InlineKeyboardButton("🏠 В меню", callback_data="back_admin")]]
        await update.message.reply_text(
            "✅ *База данных восстановлена!*",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
    return True

# ── Queue ──────────────────────────────────────────────────
async def cb_adm_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    await _edit_queue_menu(q.edit_message_text)

async def _edit_queue_menu(edit_fn):
    cnt = queue_count()
    kb = [
        [InlineKeyboardButton("🧹 Очистить очередь", callback_data="queue_clear")],
        [InlineKeyboardButton("🆕 Новые",     callback_data="qview_new"),
         InlineKeyboardButton("📩 Ждём СМС", callback_data="qview_wait_sms")],
        [InlineKeyboardButton("📷 Ждём QR",   callback_data="qview_wait_qr"),
         InlineKeyboardButton("✅ Встали",    callback_data="qview_stood")],
        [InlineKeyboardButton("📋 Вся очередь", callback_data="qview_all")],
        [InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_admin")],
    ]
    await edit_fn(
        f"⚡ *Очередь* ⚡\n\nВсего: *{cnt}* номеров\n\nВыберите действие:",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
    )

async def cb_queue_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    kb = [[InlineKeyboardButton("✅ Да, очистить!", callback_data="queue_clear_ok"),
           InlineKeyboardButton("❌ Отмена", callback_data="adm_queue")]]
    await qedit(q, "⚠️ Удалить очередь и всю историю?", reply_markup=InlineKeyboardMarkup(kb))

async def cb_queue_clear_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    with db() as conn:
        conn.execute("DELETE FROM queue")
    await q.answer("✅ Очередь очищена!")
    await _edit_queue_menu(q.edit_message_text)

QVIEW_FILTER = {
    "qview_all": None, "qview_new": "new",
    "qview_wait_sms": "wait_sms", "qview_wait_qr": "wait_qr", "qview_stood": "stood",
}

async def cb_queue_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    sf = QVIEW_FILTER.get(q.data)
    with db() as conn:
        if sf:
            rows = conn.execute(
                "SELECT id,number,status,operator,loaded_at FROM queue WHERE deleted=0 AND status=? ORDER BY id DESC LIMIT 30", (sf,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id,number,status,operator,loaded_at FROM queue WHERE deleted=0 ORDER BY id DESC LIMIT 30"
            ).fetchall()
    if not rows:
        kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="adm_queue")]]
        await qedit(q, "📭 Очередь пуста.", reply_markup=InlineKeyboardMarkup(kb)); return
    kb = [[InlineKeyboardButton(f"{s_emoji(r[2])} {r[1] or '(без номера)'} — {s_name(r[2])}", callback_data=f"qitem_{r[0]}")] for r in rows]
    kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm_queue")])
    title = "Вся очередь" if not sf else s_name(sf)
    await qedit(q, f"📋 *{title}*\n\nНажми на номер для подробностей:",
                              parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def cb_queue_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    qid = int(q.data.split("_")[1])
    with db() as conn:
        row = conn.execute("SELECT id,number,status,operator,loaded_at,stood_at FROM queue WHERE id=?", (qid,)).fetchone()
    if not row:
        await qedit(q, "❌ Номер не найден."); return
    _, number, status, operator, loaded_at, stood_at = row
    text = (
        f"📱 *Номер:* `{number}`\n"
        f"📊 *Статус:* {s_emoji(status)} {s_name(status)}\n"
        f"⏰ *Загружен:* {loaded_at or '—'}\n"
        f"✅ *Встал:* {stood_at or '—'}\n"
        f"👤 *Оператор:* {operator or '—'}"
    )
    status_btns = [
        InlineKeyboardButton(f"{s_emoji(s)} {s_name(s)}", callback_data=f"qst_{qid}_{s}")
        for s in ("new","wait_sms","wait_qr","stood") if s != status
    ]
    kb = [status_btns] if status_btns else []
    kb.append([InlineKeyboardButton("🗑 Удалить из очереди", callback_data=f"qdel_{qid}")])
    kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="qview_all")])
    await qedit(q, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def cb_queue_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    _, qid_s, new_status = q.data.split("_", 2)
    qid = int(qid_s)
    now = now_msk().isoformat()
    with db() as conn:
        if new_status == "stood":
            conn.execute("UPDATE queue SET status=?,stood_at=? WHERE id=?", (new_status, now, qid))
            row = conn.execute(
                "SELECT number, operator, user_id FROM queue WHERE id=?", (qid,)
            ).fetchone()
            if row:
                number, operator, q_uid = row
                # resolve username
                urow = conn.execute("SELECT username FROM users WHERE user_id=?", (q_uid or -1,)).fetchone()
                uname = urow[0] if urow else None
                conn.execute(
                    """INSERT INTO daily_report
                       (number, user_id, username, operator, stood_at, report_date)
                       VALUES (?,?,?,?,?,?)""",
                    (number, q_uid, uname, operator, now, date.today().isoformat())
                )
        else:
            conn.execute("UPDATE queue SET status=? WHERE id=?", (new_status, qid))
    await q.answer(f"✅ {s_name(new_status)}")
    q.data = f"qitem_{qid}"
    await cb_queue_item(update, context)

async def cb_queue_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    qid = int(q.data.split("_")[1])
    with db() as conn:
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (qid,))
    await q.answer("✅ Удалён!")
    await _edit_queue_menu(q.edit_message_text)

# ── Ban / Unban ────────────────────────────────────────────
async def cb_adm_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    kb = [
        [InlineKeyboardButton("🚫 Бан",            callback_data="ban_do")],
        [InlineKeyboardButton("✅ Разбанить",        callback_data="unban_do")],
        [InlineKeyboardButton("📄 Список забаненных", callback_data="ban_list")],
        [InlineKeyboardButton("⬅️ В меню",           callback_data="back_admin")],
    ]
    await qedit(q, "⚡ *Бан/Разбан* ⚡\n\nВыберите действие:",
                              parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def cb_ban_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    context.user_data["ban_action"] = "ban"
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="adm_ban")]]
    await qedit(q, "🚫 *Бан*\n\nОтправьте `@username` или `ID`:",
                              parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    return WAIT_BAN_TARGET

async def cb_unban_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    context.user_data["ban_action"] = "unban"
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="adm_ban")]]
    await qedit(q, "✅ *Разбан*\n\nОтправьте `@username` или `ID`:",
                              parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    return WAIT_UNBAN_TARGET

async def wait_ban_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return ConversationHandler.END
    target = update.message.text.strip()
    action = context.user_data.get("ban_action", "ban")
    with db() as conn:
        if target.startswith("@"):
            row = conn.execute("SELECT user_id,username FROM users WHERE username=?", (target.lstrip("@"),)).fetchone()
        else:
            row = conn.execute("SELECT user_id,username FROM users WHERE user_id=?", (int(target) if target.isdigit() else -1,)).fetchone()
        if not row:
            await update.message.reply_text("❌ Пользователь не найден.")
            return ConversationHandler.END
        uid, uname = row
        if action == "ban":
            conn.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
            conn.execute("INSERT INTO bans (user_id,username,banned_at,banned_by) VALUES (?,?,?,?)",
                        (uid, uname, now_msk().isoformat(), update.effective_user.id))
            msg = f"✅ @{uname or uid} заблокирован."
        else:
            conn.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (uid,))
            msg = f"✅ @{uname or uid} разбанен."
    kb = [[InlineKeyboardButton("🏠 В меню", callback_data="back_admin")]]
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def cb_ban_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    with db() as conn:
        rows = conn.execute("SELECT user_id,username FROM users WHERE is_banned=1").fetchall()
    if not rows:
        text = "📄 *Список забаненных*\n\nСписок пуст."
    else:
        lines = ["📄 *Чёрный список:*\n"] + [f"• @{r[1] or '—'} | `{r[0]}`" for r in rows]
        text = "\n".join(lines)
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="adm_ban")]]
    await qedit(q, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

# ── Settings ───────────────────────────────────────────────
SETTING_KEYS = {
    "set_ch_url":  ("channel_url", "Введите новую ссылку канала бота:"),
    "set_sup_id":  ("support_id",  "Введите ID канала поддержки:"),
    "set_sub_url": ("sub_url",     "Введите ссылку обязательной подписки:"),
    "set_sub_id":  ("sub_id",      "Введите ID канала обязательной подписки:"),
    "set_tariff":  ("tariff",      "Введите новый тариф (число, например 4.5):"),
    "set_tariff_skip": ("tariff_skip", "Введите тариф для 'Без очереди' (число, например 3.5):"),
    "set_hold":       ("hold",        "Введите холд выплат в минутах (например 5):"),
    "set_pay_log_id": ("pay_log_id",  "Введите ID канала для логов выплат (например -1001234567890):"),
}

async def cb_adm_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    await _edit_settings(q.edit_message_text)

async def cb_toggle_accept_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    set_setting("accept_sms", "off" if get_setting("accept_sms") == "on" else "on")
    await q.answer("✅")
    await _edit_settings(q.edit_message_text)

async def cb_toggle_accept_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    set_setting("accept_qr", "off" if get_setting("accept_qr") == "on" else "on")
    await q.answer("✅")
    await _edit_settings(q.edit_message_text)

async def _edit_settings(edit_fn):
    cfg = {k: get_setting(k) for k in ("channel_url","support_id","sub_url","sub_id","tariff","tariff_skip","hold","accept_sms","accept_qr","pay_log_id")}
    acc_sms = "✅ ВКЛ" if cfg["accept_sms"] == "on" else "❌ ВЫКЛ"
    acc_qr      = "✅ ВКЛ" if cfg["accept_qr"]  == "on" else "❌ ВЫКЛ"
    pay_log_val = cfg["pay_log_id"] or "не задан"
    text = (
        "⚡ *Настройки* ⚡\n\n"
        f"Канал бота: `{cfg['channel_url']}`\n"
        f"ID поддержки: `{cfg['support_id']}`\n"
        f"Подписка (ссылка): `{cfg['sub_url']}`\n"
        f"Подписка (ID): `{cfg['sub_id']}`\n"
        f"Тариф: `{cfg['tariff']}`\n"
        f"Тариф (без очереди): `{cfg['tariff_skip']}`\n"
        f"Холд (мин): `{cfg['hold']}`\n"
        f"Приём СМС: {acc_sms}\n"
        f"Приём QR: {acc_qr}\n"
        f"Канал выплат: `{pay_log_val}`\n\n"
        "Выбери что изменить:"
    )
    kb = [
        [InlineKeyboardButton("📣 Ссылка канала бота",      callback_data="set_ch_url")],
        [InlineKeyboardButton("🆘 ID поддержки",             callback_data="set_sup_id")],
        [InlineKeyboardButton("✅ Подписка: ссылка",         callback_data="set_sub_url")],
        [InlineKeyboardButton("✅ Подписка: ID",             callback_data="set_sub_id")],
        [InlineKeyboardButton("💰 Тариф",                   callback_data="set_tariff"),
         InlineKeyboardButton("⚡ Тариф без очереди",       callback_data="set_tariff_skip")],
        [InlineKeyboardButton(f"⏳ Холд выплат (мин)",       callback_data="set_hold")],
        [InlineKeyboardButton(f"💬 Приём СМС: {acc_sms}",   callback_data="toggle_accept_sms")],
        [InlineKeyboardButton(f"📷 Приём QR: {acc_qr}",     callback_data="toggle_accept_qr")],
        [InlineKeyboardButton("💸 Канал выплат",             callback_data="set_pay_log_id")],
        [InlineKeyboardButton("⬅️ Назад в меню",            callback_data="back_admin")],
    ]
    await edit_fn(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def cb_setting_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    info = SETTING_KEYS.get(q.data)
    if not info: return
    key, prompt = info
    context.user_data["setting_key"] = key
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="adm_settings")]]
    await qedit(q, f"⚙️ *Изменение*\n\n{prompt}", parse_mode="Markdown",
                              reply_markup=InlineKeyboardMarkup(kb))
    return WAIT_SETTING_VALUE

async def wait_setting_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return ConversationHandler.END
    key   = context.user_data.get("setting_key")
    value = update.message.text.strip()
    set_setting(key, value)
    kb = [[InlineKeyboardButton("⚙️ Настройки", callback_data="adm_settings"),
           InlineKeyboardButton("🏠 Меню", callback_data="back_admin")]]
    await update.message.reply_text(f"✅ Сохранено: `{value}`", parse_mode="Markdown",
                                    reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

def _safe_int(val, default: int) -> int:
    try:
        return int(str(val).strip().split('.')[0]) if val else default
    except:
        return default

def _safe_float(val, default: float) -> float:
    try:
        return float(str(val).strip()) if val else default
    except:
        return default

# ── Payments ───────────────────────────────────────────────
def _payments_stats() -> dict:
    today        = now_msk().date().isoformat()          # МСК дата, совпадает с форматом stood_at
    today_prefix = today + "%"
    hold_min     = _safe_int(get_setting("hold"), 5)
    hold_sec     = hold_min * 60
    tariff       = _safe_float(get_setting("tariff"), 4.0)
    tariff_skip  = _safe_float(get_setting("tariff_skip"), tariff)
    now          = now_msk()

    with db() as conn:
        # Встало = любой номер у которого stood_at за сегодня
        stood_cnt = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE stood_at LIKE ?",
            (today_prefix,)
        ).fetchone()[0]

        # Слетело после холда = status='fell' AND stood_at today
        fell_after_cnt = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE status='fell' AND stood_at LIKE ?",
            (today_prefix,)
        ).fetchone()[0]

        # Слетело до холда = status='cancelled' И stood_at задан (встал, но упал до холда)
        fell_early_cnt = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE status='cancelled' AND stood_at IS NOT NULL AND stood_at LIKE ?",
            (today_prefix,)
        ).fetchone()[0]

        # Слетело ВСЕГО = после холда + до холда
        fell_cnt = fell_after_cnt + fell_early_cnt

        # Оплачено
        paid_cnt = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE status='paid' AND stood_at LIKE ?",
            (today_prefix,)
        ).fetchone()[0]

        # Номера в статусе 'stood' — для подсчёта в холде / не оплачено
        unpaid_rows = conn.execute(
            """SELECT an.id, an.number, an.user_id, an.username, an.stood_at,
                      COALESCE(an.skip_queue, 0) as skip_q,
                      COALESCE(dr.paid, 0) as paid_amt,
                      COALESCE(dr.payment_status,'unpaid') as pay_status
               FROM active_numbers an
               LEFT JOIN daily_report dr ON dr.user_id=an.user_id
                   AND dr.number=an.number AND dr.report_date=? AND dr.deleted_from_report=0
               WHERE an.status='stood' AND an.stood_at LIKE ?""",
            (today, today_prefix)
        ).fetchall()

    hold_cnt    = 0
    expired_cnt = 0
    unpaid_cnt  = 0
    pending_list = []

    for r in unpaid_rows:
        an_id, number, uid, uname, stood_at_str, skip_q, paid_amt, pay_status = r
        already_paid = (float(paid_amt or 0) > 0 or pay_status in ('paid', 'pending'))
        if already_paid:
            continue
        try:
            stood_dt = datetime.fromisoformat(stood_at_str)
        except:
            stood_dt = now
        elapsed = (now - stood_dt).total_seconds()
        if elapsed < hold_sec:
            hold_cnt += 1
        else:
            expired_cnt += 1
            unpaid_cnt  += 1
            with db() as conn2:
                dr_row = conn2.execute(
                    "SELECT id FROM daily_report WHERE user_id=? AND number=? AND report_date=?",
                    (uid, number, today)
                ).fetchone()
            dr_id = dr_row[0] if dr_row else None
            num_tariff = tariff_skip if skip_q else tariff
            pending_list.append((dr_id, number, uid, uname, num_tariff))

    # Отстояло холд = оплачено + слетело ПОСЛЕ холда + те кто стоит и холд истёк
    expired_total = paid_cnt + fell_after_cnt + expired_cnt

    return {
        "tariff":     tariff,
        "hold_sec":   hold_sec,
        "stood":      stood_cnt,
        "fell":       fell_cnt,
        "fell_early": fell_early_cnt,
        "expired":    expired_total,
        "paid":       paid_cnt,
        "unpaid":     unpaid_cnt,
        "in_hold":    hold_cnt,
        "pending":    pending_list,
    }


async def crypto_create_check(amount: float, currency: str = "USDT") -> dict:
    """Создаёт чек CryptoPay для выплаты дропу."""
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    payload = {"asset": currency, "amount": f"{amount:.2f}"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CRYPTO_PAY_API}/createCheck",
                              json=payload, headers=headers) as r:
                data = await r.json()
                log.info(f"createCheck response: {data}")
                if data.get("ok"):
                    result = data["result"]
                    log.info(f"createCheck OK, fields: {list(result.keys())}")
                    return {"type": "check", **result}
                err = data.get("error", {})
                log.error(f"createCheck не удался: {err}")
                return {}
    except Exception as e:
        log.error(f"createCheck exception: {e}")
        return {}


def _mask_number(number: str) -> str:
    n = number.replace(" ", "").replace("-", "")
    if len(n) >= 8:
        return n[:4] + "***" + n[-4:]
    return number


async def _send_check_to_user(context, uid: int, number: str, amount: float) -> bool:
    """Отправляет выплату юзеру: чек или инвойс."""
    created_at = now_msk()
    result = await crypto_create_check(amount)
    if not result:
        log.error(f"_send_check_to_user: пустой ответ для uid={uid}")
        return False

    pay_type  = result.get("type", "check")
    check_url = (
        result.get("bot_check_url")
        or result.get("check_url")
        or result.get("bot_invoice_url")
        or result.get("mini_app_invoice_url")
        or result.get("pay_url")
        or result.get("url")
        or ""
    )
    log.info(f"pay_type={pay_type}, url={check_url!r}")

    if not check_url:
        log.error(f"URL пустой! Полный ответ: {result}")
        return False

    kb = [[InlineKeyboardButton("🎁 Получить чек", url=check_url)]]
    try:
        await context.bot.send_message(
            uid,
            f'<tg-emoji emoji-id="5276422526350681413">🎁</tg-emoji> <b>Выплата</b>\n\n'
            f"Сумма: <b>${amount:.2f} USDT</b>\n\n"
            f"Нажми кнопку ниже 👇",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        log.info(f"✅ Выплата отправлена uid={uid}")
    except Exception as e:
        log.error(f"send_message uid={uid}: {e}")
        return False

    # Лог в канал выплат
    pay_log_id = get_setting("pay_log_id")
    if pay_log_id:
        try:
            sent_at = now_msk()
            masked  = _mask_number(number)
            log_text = (
                f"💸 *Выплата отправлена*\n\n"
                f"💰 Сумма: *{amount:.2f}$*\n\n"
                f"📱 Номера:\n"
                f"• `{masked}`\n\n"
                f"🕒 Создано: {created_at.strftime('%H:%M  %d.%m.%Y')}\n"
                f"✅ Отправлено: {sent_at.strftime('%H:%M  %d.%m.%Y')}"
            )
            await context.bot.send_message(pay_log_id, log_text, parse_mode="Markdown")
        except Exception as e:
            log.error(f"Лог выплаты в канал не отправлен: {e}")

    return True


async def safe_edit(edit_fn, text: str, **kwargs):
    """Обёртка над edit_message_text — игнорирует 'not modified'."""
    try:
        await edit_fn(text, **kwargs)
    except Exception as e:
        if "not modified" in str(e).lower():
            pass  # Сообщение не изменилось — ок
        else:
            raise

async def qedit(q, text: str, **kwargs):
    """Умное редактирование — caption если фото, text если текст."""
    try:
        if q.message and q.message.photo:
            await q.edit_message_caption(caption=text, **kwargs)
        else:
            await q.edit_message_text(text, **kwargs)
    except Exception as e:
        if "not modified" in str(e).lower():
            pass
        else:
            raise


async def cb_adm_payments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    await _edit_payments(q.edit_message_text)


async def _edit_payments(edit_fn):
    s      = _payments_stats()
    moment = get_setting("moment_payment") == "on"
    moment_label = "⚡ Моментальная выплата: ✅ Вкл" if moment else "⚡ Моментальная выплата: 🔕 Выкл"
    hold_m = s['hold_sec'] // 60

    text = (
        "⚡ *Выплаты* ⚡\n\n"
        f"Тариф: *${s['tariff']:.2f}* | Без очереди: *${_safe_float(get_setting('tariff_skip'), s['tariff']):.2f}*\n"
        f"Холд: *{hold_m}* мин\n\n"
        f"Встало: *{s['stood']}*\n"
        f"Слетело: *{s['fell']}*\n"
        f"Отстояло холд: *{s['expired']}*\n"
        f"Слетело до холда: *{s['fell_early']}*\n\n"
        f"Оплачено: *{s['paid']}*\n"
        f"Не оплачено: *{s['unpaid']}*\n"
        f"В холде: *{s['in_hold']}*\n"
    )

    if moment and s['pending']:
        text += "\n*Ожидают выплаты:*\n"
        for (rid, number, uid, uname, amount) in s['pending']:
            name = f"@{uname}" if uname else f"ID:{uid}"
            text += f"• {name} — *${amount:.2f}*\n"

    kb = [
        [InlineKeyboardButton(moment_label, callback_data="toggle_moment")],
        [InlineKeyboardButton(f"⏳ Холд выплат: {hold_m} мин", callback_data="pay_set_hold")],
        [InlineKeyboardButton("🎁 Выплатить всем вставшим", callback_data="pay_all_pending")],
        [InlineKeyboardButton("🗑 Удалить номер из отчёта", callback_data="pay_del_report")],
        [InlineKeyboardButton("📄 Отчёт за день (.txt)",   callback_data="pay_daily")],
        [InlineKeyboardButton("🧹 Очистить отчёт",         callback_data="pay_clear_report")],
        [InlineKeyboardButton("⬅️ Назад в меню",           callback_data="back_admin")],
    ]
    await safe_edit(edit_fn, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


async def cb_toggle_moment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    set_setting("moment_payment", "off" if get_setting("moment_payment") == "on" else "on")
    await q.answer("✅")
    await _edit_payments(q.edit_message_text)


async def cb_pay_set_hold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Быстрая смена холда прямо из выплат."""
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    context.user_data["setting_key"] = "hold"
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="adm_payments")]]
    await qedit(q, 
        "⏳ *Холд выплат*\n\nВведите новое значение в минутах (например 5 = 5 мин):",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
    )
    return WAIT_SETTING_VALUE


async def cb_pay_one(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выплатить одному через чек CryptoPay."""
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    rid = int(q.data.split("_")[2])

    with db() as conn:
        row = conn.execute(
            "SELECT number, user_id, username FROM daily_report WHERE id=?", (rid,)
        ).fetchone()

    if not row:
        await q.answer("❌ Запись не найдена", show_alert=True); return

    number, uid, uname = row
    with db() as conn:
        sq = conn.execute(
            "SELECT skip_queue FROM active_numbers WHERE number=? AND user_id=? ORDER BY id DESC LIMIT 1",
            (number, uid)
        ).fetchone()
    skip_q = sq[0] if sq else 0
    tariff_base = _safe_float(get_setting("tariff"), 4.0)
    tariff_s    = _safe_float(get_setting("tariff_skip"), tariff_base)
    tariff = tariff_s if skip_q else tariff_base

    if not uid:
        await q.answer("❌ user_id не привязан к номеру", show_alert=True); return

    await q.answer("⏳ Создаю чек...")
    ok = await _send_check_to_user(context, uid, number, tariff)

    if ok:
        with db() as conn:
            conn.execute(
                "UPDATE daily_report SET paid=?, payment_status='paid' WHERE id=?",
                (tariff, rid)
            )
            conn.execute(
                "INSERT INTO payments (user_id,number,amount,currency,status,created_at,paid_at) VALUES (?,?,?,?,?,?,?)",
                (uid, number, tariff, "USDT", "paid", now_msk().isoformat(), now_msk().isoformat())
            )
        await q.answer(f"✅ Чек отправлен @{uname or uid}!", show_alert=True)
    else:
        await q.answer("❌ Ошибка отправки чека. Проверь баланс CryptoPay.", show_alert=True)

    await _edit_payments(q.edit_message_text)


async def cb_pay_all_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выплатить всем вставшим (отстоявшим холд) через чеки."""
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return

    if get_setting("moment_payment") != "on":
        await q.answer("⛔ Моментальная выплата выключена!", show_alert=True)
        return

    s = _payments_stats()
    if not s['pending']:
        await q.answer("✅ Нет ожидающих выплаты!", show_alert=True)
        return

    await q.answer(f"⏳ Создаю {len(s['pending'])} чеков...", show_alert=False)

    success = 0
    errors  = []

    for (rid, number, uid, uname, amount) in s['pending']:
        if not uid:
            errors.append(f"• {number} — нет user_id")
            continue

        ok = await _send_check_to_user(context, uid, number, amount)

        if ok:
            with db() as conn:
                conn.execute(
                    "UPDATE daily_report SET paid=?, payment_status='paid' WHERE id=?",
                    (amount, rid)
                )
                conn.execute(
                    """INSERT INTO payments
                       (user_id, number, amount, currency, status, created_at, paid_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (uid, number, amount, "USDT", "paid",
                     now_msk().isoformat(), now_msk().isoformat())
                )
            success += 1
        else:
            errors.append(f"• {number} (@{uname or uid}) — ошибка CryptoPay")

        await asyncio.sleep(0.3)

    # Отправляем итог отдельным сообщением (не edit — чтобы не упасть)
    result_text = f"✅ Выплачено: *{success}*"
    if errors:
        result_text += f"\n❌ Ошибки ({len(errors)}):\n" + "\n".join(errors)
        result_text += "\n\n_Проверь логи бота и баланс CryptoPay_"

    try:
        await context.bot.send_message(
            q.from_user.id, result_text, parse_mode="Markdown"
        )
    except: pass

    await _edit_payments(q.edit_message_text)


async def cb_pay_clear_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистить весь отчёт за сегодня."""
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    kb = [
        [InlineKeyboardButton("✅ Да, очистить!", callback_data="pay_clear_ok"),
         InlineKeyboardButton("❌ Отмена",        callback_data="adm_payments")],
    ]
    await qedit(q, 
        "⚠️ *Очистить отчёт за сегодня?*\n\nВсе записи будут удалены.",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
    )


async def cb_pay_clear_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    today = date.today().isoformat()
    with db() as conn:
        conn.execute("DELETE FROM daily_report WHERE report_date=?", (today,))
    await q.answer("✅ Отчёт очищен!")
    await _edit_payments(q.edit_message_text)

async def cb_pay_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    today     = date.today()
    today_str = today.strftime("%d.%m.%Y")
    today_iso = today.isoformat()

    with db() as conn:
        rows = conn.execute(
            """SELECT dr.id, dr.number, dr.user_id, dr.username,
                      an.op_username, an.op_id, an.phone_type,
                      an.created_at, dr.stood_at, dr.payment_status,
                      dr.paid, dr.fell_at, an.fell_at as an_fell_at, an.status as an_status
               FROM daily_report dr
               LEFT JOIN active_numbers an
                   ON an.number=dr.number AND an.user_id=dr.user_id
                   AND an.id = (
                       SELECT id FROM active_numbers
                       WHERE number=dr.number AND user_id=dr.user_id
                       ORDER BY id DESC LIMIT 1
                   )
               WHERE dr.report_date=? AND dr.deleted_from_report=0
               ORDER BY dr.rowid ASC""",
            (today_iso,)
        ).fetchall()

    if not rows:
        await qedit(q, 
            "📊 Отчёт за сегодня пуст.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm_payments")]])
        )
        return

    STATUS_RU = {
        "stood":           "Встал ✅",
        "fell":            "Слетел 💥",
        "not_stood":       "Не встал ❌",
        "cancelled":       "Отменён 🚫",
        "timeout":         "Тайм-аут ⏰",
        "waiting":         "Ожидает ⏳",
        "confirmed":       "Подтверждён ✅",
        "wait_sms_code":   "Ждём СМС 📩",
        "wait_sms_confirm":"Ждём подтверждения ⏳",
        "sms_sent":        "СМС отправлен 📨",
        "wait_qr_photo":   "Ждём QR 📷",
        "qr_sent":         "QR отправлен 📤",
        "qr_scanned_wait": "Сканирует ⏳",
    }

    lines = []
    num_cnt  = 0
    paid_sum = 0.0

    for row in rows:
        dr_id, number, uid, uname, op_uname, op_id, ptype, created_at, stood_at, dr_status, paid, dr_fell_at, an_fell_at, an_status = row
        # fell_at: сначала из dr, потом из an
        fell_at = dr_fell_at or an_fell_at
        # Статус: dr_status приоритетнее для "fell", an_status для остального
        if dr_status == "fell":
            display_status = "fell"
        elif an_status:
            display_status = an_status
        else:
            display_status = dr_status or "stood"
        # Пропускаем только не вставшие и таймаут (не имеющие записи в daily_report)
        if display_status in ("not_stood", "timeout"):
            continue
        if not number:
            continue
        num_cnt += 1
        method    = "СМС" if ptype == "sms" else "QR" if ptype == "qr" else "—"
        status_ru = STATUS_RU.get(display_status, display_status or "—")

        def fmt_t(ts):
            if not ts: return "—"
            try: return now_msk().__class__.fromisoformat(ts).strftime("%H:%M:%S")
            except: return "—"

        def fmt_ts(ts):
            if not ts: return "—"
            try:
                dt = datetime.fromisoformat(ts)
                return dt.strftime("%H:%M:%S")
            except: return "—"

        # Время простоя — от stood_at до fell_at (слёт) или до сейчас (стоит)
        def _calc_duration(start_str, end_str=None):
            """Считает простой между двумя временными метками."""
            if not start_str:
                return "—"
            try:
                s_dt = datetime.fromisoformat(start_str)
                e_dt = datetime.fromisoformat(end_str) if end_str else now_msk()
                diff = e_dt - s_dt
                total = int(diff.total_seconds())
                if total <= 0:
                    return "—"
                h, rem = divmod(total, 3600)
                m2, sc = divmod(rem, 60)
                if h:
                    return f"{h}ч {m2}м {sc}с"
                return f"{m2}м {sc}с"
            except:
                return "—"

        fell_time_str = "—"
        if fell_at:
            try:
                fell_time_str = datetime.fromisoformat(fell_at).strftime("%H:%M:%S")
            except: pass

        if display_status in ("fell", "cancelled"):
            duration = _calc_duration(stood_at, fell_at)
        elif display_status in ("stood", "paid"):
            duration = _calc_duration(stood_at)
        else:
            duration = "—"

        block = [
            f"#{num_cnt:<5} Номер: {number}",
            f"  Метод: {method}",
            f"  Статус: {status_ru}",
            f"  Дроп: @{uname or '—'} (ID: {uid})",
            f"  Оператор: @{op_uname or '—'} (ID: {op_id or '—'})",
            f"  Сдан: {fmt_ts(created_at)}",
            f"  Встал: {fmt_ts(stood_at)}",
            f"  Слетел: {fell_time_str}",
            f"  Простоял: {duration}",
            "-" * 40,
            "",
        ]
        lines.extend(block)
        if paid and paid > 0:
            paid_sum += paid

    # Шапка
    header = [
        f"Отчёт за {today_str}",
        "=" * 50,
        f"Всего номеров: {num_cnt}  |  Выплачено: ${paid_sum:.2f}",
        "",
    ]
    report_text = "\n".join(header + lines)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    fname = f"{REPORTS_DIR}/report_{today_iso}.txt"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(report_text)
    with open(fname, "rb") as f:
        await context.bot.send_document(
            q.from_user.id, document=f,
            filename=f"report_{today_iso}.txt",
            caption=f"📊 {today_str} — {num_cnt} номеров | ${paid_sum:.2f}"
        )
    await _edit_payments(q.edit_message_text)

async def cb_pay_del_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.answer()
    today = date.today().isoformat()
    with db() as conn:
        rows = conn.execute(
            "SELECT id,number,operator FROM daily_report WHERE report_date=? AND deleted_from_report=0", (today,)
        ).fetchall()
    if not rows:
        await qedit(q, "📊 Отчёт пуст.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm_payments")]]))
        return
    kb = [[InlineKeyboardButton(f"🗑 {r[1]} ({r[2] or '—'})", callback_data=f"delrep_{r[0]}")] for r in rows]
    kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm_payments")])
    await qedit(q, "🗑 *Удалить номер из отчёта:*",
                              parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def cb_del_report_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id): await q.answer(); return
    rid = int(q.data.split("_")[1])
    with db() as conn:
        conn.execute("UPDATE daily_report SET deleted_from_report=1 WHERE id=?", (rid,))
    await q.answer("✅ Удалено!")
    q.data = "pay_del_report"
    await cb_pay_del_report(update, context)

# ── CryptoPay — User payment flow ──────────────────────────
async def cb_user_pay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Example: user requests payout via CryptoPay"""
    q = update.callback_query
    await q.answer()
    if get_setting("moment_payment") != "on":
        await qedit(q, 
            "⏳ Момент оплаты сейчас *выключен*.\nОжидайте включения.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_main")]])
        )
        return
    tariff = _safe_float(get_setting("tariff"), 4.0)
    invoice = await crypto_create_invoice(tariff, "USDT", "Оплата номера")
    if not invoice:
        await qedit(q, "❌ Ошибка создания счёта. Попробуйте позже.")
        return
    invoice_id = invoice.get("invoice_id")
    pay_url     = invoice.get("bot_invoice_url") or invoice.get("pay_url")
    with db() as conn:
        conn.execute(
            "INSERT INTO payments (user_id,amount,currency,invoice_id,status,created_at) VALUES (?,?,?,?,?,?)",
            (q.from_user.id, tariff, "USDT", invoice_id, "pending", now_msk().isoformat())
        )
    kb = [
        [InlineKeyboardButton(f"💳 Оплатить {tariff} USDT", url=pay_url)],
        [InlineKeyboardButton("✅ Проверить оплату", callback_data=f"check_pay_{invoice_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")],
    ]
    await qedit(q, 
        f"💳 *Оплата*\n\nСумма: `{tariff} USDT`\nНомер счёта: `{invoice_id}`\n\n"
        "Нажмите кнопку оплаты, затем проверьте статус.",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
    )

async def cb_check_pay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    invoice_id = q.data.split("_", 2)[2]
    inv = await crypto_check_invoice(invoice_id)
    status = inv.get("status", "unknown")
    if status == "paid":
        amount = float(inv.get("amount", 0))
        with db() as conn:
            conn.execute("UPDATE payments SET status='paid', paid_at=? WHERE invoice_id=?",
                        (now_msk().isoformat(), invoice_id))
            conn.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (amount, q.from_user.id))
        await qedit(q, 
            f"✅ *Оплата подтверждена!*\n\nПополнено: `{amount} USDT`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="back_main")]])
        )
    else:
        kb = [[InlineKeyboardButton("🔄 Проверить снова", callback_data=f"check_pay_{invoice_id}")],
              [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")]]
        await qedit(q, 
            f"⏳ Статус: *{status}*\n\nОплата ещё не поступила.",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)
        )

# ═══════════════════════════════════════════════════════════
#  🚀  MAIN
# ═══════════════════════════════════════════════════════════

async def cmd_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика по топику в рабочем чате."""
    if update.effective_chat.type == "private":
        return
    chat_id  = update.effective_chat.id
    topic_id = update.message.message_thread_id or 0
    today    = date.today().isoformat()
    with db() as conn:
        wc = conn.execute(
            "SELECT id, name FROM work_chats WHERE chat_id=? AND topic_id=?", (chat_id, topic_id)
        ).fetchone()
        if not wc:
            return
        _, wc_name = wc
        taken = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE chat_id=? AND DATE(created_at)=?",
            (chat_id, today)
        ).fetchone()[0]
        stood = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE chat_id=? AND status IN ('stood','paid') AND DATE(stood_at)=?",
            (chat_id, today)
        ).fetchone()[0]
        fell = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE chat_id=? AND status='fell' AND DATE(stood_at)=?",
            (chat_id, today)
        ).fetchone()[0]
        cancelled = conn.execute(
            "SELECT COUNT(*) FROM active_numbers WHERE chat_id=? AND status='cancelled' AND DATE(created_at)=?",
            (chat_id, today)
        ).fetchone()[0]
        in_queue = conn.execute(
            "SELECT COUNT(*) FROM queue WHERE deleted=0 AND status IN ('wait_sms','wait_qr')"
        ).fetchone()[0]

    name_str = f"«{wc_name}» " if wc_name else ""
    text = (
        f"📊 *Статистика {name_str}за сегодня*\n\n"
        f"В очереди: *{in_queue}*\n"
        f"Взято: *{taken}*\n"
        f"Встало: *{stood}*\n"
        f"Слетело: *{fell}*\n"
        f"Отменено: *{cancelled}*\n\n"
        f"`chat_id={chat_id}` | `topic_id={topic_id}`"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

def main():
    # ── Защита от двойного запуска (PID-файл) ─────────────
    PID_FILE = "bot.pid"
    current_pid = os.getpid()

    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as f:
                old_pid = int(f.read().strip())
            # Проверяем жив ли старый процесс
            import signal
            os.kill(old_pid, signal.SIGTERM)
            log.warning(f"Убит старый процесс бота PID={old_pid}")
            import time; time.sleep(1)
        except (ProcessLookupError, ValueError):
            pass  # Процесс уже не существует

    with open(PID_FILE, "w") as f:
        f.write(str(current_pid))

    import atexit
    atexit.register(lambda: os.path.exists(PID_FILE) and os.remove(PID_FILE))
    # ──────────────────────────────────────────────────────

    os.makedirs(REPORTS_DIR, exist_ok=True)
    init_db()

    persistence = PicklePersistence(filepath="bot_persistence")
    app = Application.builder().token(BOT_TOKEN).persistence(persistence).build()

    # ── Conversation handlers ──────────────────────────────
    msg_user_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_adm_msg_user, pattern="^adm_msg_user$")],
        states={
            WAIT_MSG_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, wait_msg_target)],
            WAIT_MSG_TEXT:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, wait_msg_text)],
        },
        fallbacks=[CallbackQueryHandler(cb_back_admin, pattern="^back_admin$")],
        per_message=False, per_chat=False, per_user=True,
    )

    broadcast_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_adm_broadcast, pattern="^adm_broadcast$")],
        states={WAIT_BROADCAST: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, wait_broadcast)]},
        fallbacks=[CallbackQueryHandler(cb_back_admin, pattern="^back_admin$")],
        per_message=False, per_chat=False, per_user=True,
    )

    ban_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cb_ban_do,   pattern="^ban_do$"),
            CallbackQueryHandler(cb_unban_do, pattern="^unban_do$"),
        ],
        states={
            WAIT_BAN_TARGET:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, wait_ban_target)],
            WAIT_UNBAN_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, wait_ban_target)],
        },
        fallbacks=[CallbackQueryHandler(cb_adm_ban, pattern="^adm_ban$")],
        per_message=False, per_chat=False, per_user=True,
    )

    settings_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_setting_edit,
                      pattern="^(set_ch_url|set_sup_id|set_sub_url|set_sub_id|set_tariff|set_tariff_skip|set_hold|pay_set_hold|set_pay_log_id)$")],
        states={WAIT_SETTING_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, wait_setting_value)]},
        fallbacks=[CallbackQueryHandler(cb_adm_settings, pattern="^adm_settings$")],
        per_message=False, per_chat=False, per_user=True,
    )

    for conv in (msg_user_conv, broadcast_conv, ban_conv, settings_conv):
        app.add_handler(conv)

    # ── Commands ───────────────────────────────────────────
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("admin",    cmd_admin))
    app.add_handler(CommandHandler("setmax",   cmd_setmax))
    app.add_handler(CommandHandler("unsetmax", cmd_unsetmax))
    app.add_handler(CommandHandler("settopic", cmd_settopic))
    app.add_handler(CommandHandler("reply",    cmd_reply))
    app.add_handler(CommandHandler("info",     cmd_info))

    # ── Admin main panel ───────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_back_admin,          pattern="^back_admin$"))
    app.add_handler(CallbackQueryHandler(cb_adm_broadcast_menu, pattern="^adm_broadcast_menu$"))
    app.add_handler(CallbackQueryHandler(cb_adm_db_download,    pattern="^adm_db_download$"))
    app.add_handler(CallbackQueryHandler(cb_adm_db_upload,      pattern="^adm_db_upload$"))
    app.add_handler(CallbackQueryHandler(cb_adm_toggle_status,  pattern="^adm_toggle_status$"))
    app.add_handler(CallbackQueryHandler(cb_adm_toggle_notif,   pattern="^adm_toggle_notif$"))

    # ── Queue ──────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_adm_queue,          pattern="^adm_queue$"))
    app.add_handler(CallbackQueryHandler(cb_queue_clear,        pattern="^queue_clear$"))
    app.add_handler(CallbackQueryHandler(cb_queue_clear_ok,     pattern="^queue_clear_ok$"))
    app.add_handler(CallbackQueryHandler(cb_queue_view,         pattern="^qview_"))
    app.add_handler(CallbackQueryHandler(cb_queue_item,         pattern="^qitem_"))
    app.add_handler(CallbackQueryHandler(cb_queue_status,       pattern="^qst_"))
    app.add_handler(CallbackQueryHandler(cb_queue_del,          pattern="^qdel_"))

    # ── Ban ────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_adm_ban,            pattern="^adm_ban$"))
    app.add_handler(CallbackQueryHandler(cb_ban_list,           pattern="^ban_list$"))

    # ── Settings ───────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_adm_settings,       pattern="^adm_settings$"))
    app.add_handler(CallbackQueryHandler(cb_toggle_accept_sms,  pattern="^toggle_accept_sms$"))
    app.add_handler(CallbackQueryHandler(cb_toggle_accept_qr,   pattern="^toggle_accept_qr$"))

    # ── Payments ───────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_adm_payments,       pattern="^adm_payments$"))
    app.add_handler(CallbackQueryHandler(cb_toggle_moment,      pattern="^toggle_moment$"))
    app.add_handler(CallbackQueryHandler(cb_pay_set_hold,       pattern="^pay_set_hold$"))
    app.add_handler(CallbackQueryHandler(cb_pay_daily,          pattern="^pay_daily$"))
    app.add_handler(CallbackQueryHandler(cb_pay_del_report,     pattern="^pay_del_report$"))
    app.add_handler(CallbackQueryHandler(cb_del_report_item,    pattern="^delrep_"))
    app.add_handler(CallbackQueryHandler(cb_pay_one,            pattern="^pay_one_"))
    app.add_handler(CallbackQueryHandler(cb_pay_all_pending,    pattern="^pay_all_pending$"))
    app.add_handler(CallbackQueryHandler(cb_pay_clear_report,   pattern="^pay_clear_report$"))
    app.add_handler(CallbackQueryHandler(cb_pay_clear_ok,       pattern="^pay_clear_ok$"))

    # ── Сброс старого холда (если в БД осталось 300 сек вместо минут) ──
    old_hold = get_setting("hold")
    if _safe_int(old_hold, 0) >= 60:
        set_setting("hold", "5")
        log.info(f"Холд сброшен с {old_hold} на 5 минут")

    # ── Авто-выплата по холду ──────────────────────────────
    async def auto_payment_job(ctx):
        if get_setting("moment_payment") != "on":
            log.debug("Авто-выплата: момент оплата выключена — пропускаем")
            return
        s = _payments_stats()
        if not s['pending']:
            return
        log.info(f"Авто-выплата: найдено {len(s['pending'])} к выплате")
        today = date.today().isoformat()

        for (rid, number, uid, uname, amount) in s['pending']:
            if not uid:
                continue

            # Сразу блокируем — меняем статус ДО отправки чтобы не было дублей
            with db() as conn:
                updated = conn.execute(
                    "UPDATE active_numbers SET status='paid' WHERE user_id=? AND number=? AND status='stood'",
                    (uid, number)
                ).rowcount
            if updated == 0:
                log.warning(f"Номер {number} uid={uid} уже обработан — пропускаем")
                continue

            with db() as conn:
                if rid:
                    conn.execute("UPDATE daily_report SET payment_status='pending' WHERE id=?", (rid,))
                else:
                    conn.execute(
                        "INSERT OR IGNORE INTO daily_report (number,user_id,username,payment_status,report_date) VALUES (?,?,?,?,?)",
                        (number, uid, uname, "pending", today)
                    )

            ok = await _send_check_to_user(ctx, uid, number, amount)
            if ok:
                with db() as conn:
                    if rid:
                        conn.execute("UPDATE daily_report SET paid=?, payment_status='paid' WHERE id=?", (amount, rid))
                    else:
                        conn.execute(
                            "UPDATE daily_report SET paid=?, payment_status='paid' WHERE user_id=? AND number=? AND report_date=?",
                            (amount, uid, number, today)
                        )
                    conn.execute(
                        "INSERT INTO payments (user_id,number,amount,currency,status,created_at,paid_at) VALUES (?,?,?,?,?,?,?)",
                        (uid, number, amount, "USDT", "paid", now_msk().isoformat(), now_msk().isoformat())
                    )
                log.info(f"✅ Авто-выплата: {number} → @{uname or uid} ${amount}")
            else:
                # Откат
                with db() as conn:
                    conn.execute("UPDATE active_numbers SET status='stood' WHERE user_id=? AND number=? AND status='paid'", (uid, number))
                    if rid:
                        conn.execute("UPDATE daily_report SET payment_status='unpaid' WHERE id=?", (rid,))
                log.error(f"❌ Авто-выплата не удалась: {number} uid={uid}")
            await asyncio.sleep(0.5)

    # ── Ежедневное уведомление дропам об обновлении истории (в полночь) ──
    async def daily_history_notify(ctx):
        """Каждый день сообщаем дропам что история обновилась."""
        today = date.today().isoformat()
        with db() as conn:
            uids = [r[0] for r in conn.execute(
                "SELECT DISTINCT user_id FROM daily_report WHERE report_date=? AND user_id IS NOT NULL",
                (today,)
            ).fetchall()]
        for uid in uids:
            try:
                await ctx.bot.send_message(
                    uid,
                    "📋 *История обновлена*\n\nДанные за вчера доступны в разделе История.",
                    parse_mode="Markdown"
                )
            except: pass

    app.job_queue.run_repeating(auto_payment_job, interval=30, first=10)

    # ── Operator buttons ───────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_op_req_sms,       pattern="^op_req_sms_"))
    app.add_handler(CallbackQueryHandler(cb_op_req_qr,        pattern="^op_req_qr_"))
    app.add_handler(CallbackQueryHandler(cb_op_cancel,        pattern="^op_cancel_"))
    app.add_handler(CallbackQueryHandler(cb_op_stood,         pattern="^op_stood_"))
    app.add_handler(CallbackQueryHandler(cb_op_not_stood,     pattern="^op_not_stood_"))
    app.add_handler(CallbackQueryHandler(cb_op_repeat_sms,    pattern="^op_repeat_sms_"))
    app.add_handler(CallbackQueryHandler(cb_op_qr_repeat,     pattern="^op_qr_repeat_"))
    app.add_handler(CallbackQueryHandler(cb_drop_sms_ready,   pattern="^drop_sms_ready_"))
    app.add_handler(CallbackQueryHandler(cb_drop_qr_confirm,  pattern="^drop_qr_confirm_"))
    app.add_handler(CallbackQueryHandler(cb_confirm_active,   pattern="^confirm_active_"))
    app.add_handler(CallbackQueryHandler(cb_op_fell,          pattern="^op_fell_"))

    # ── Drop QR buttons ────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_drop_qr_scanned,  pattern="^drop_qr_scanned_"))
    app.add_handler(CallbackQueryHandler(cb_drop_qr_repeat,   pattern="^drop_qr_repeat_"))
    app.add_handler(CallbackQueryHandler(cb_drop_qr_cancel,   pattern="^drop_qr_cancel_"))
    app.add_handler(CallbackQueryHandler(cb_drop_qr_back,     pattern="^drop_qr_back_"))

    # ── User ───────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_user_support,     pattern="^user_support$"))
    app.add_handler(CallbackQueryHandler(cb_support_send,     pattern="^support_send$"))
    app.add_handler(CallbackQueryHandler(cb_back_main,        pattern="^back_main$"))
    app.add_handler(CallbackQueryHandler(cb_check_sub,        pattern="^check_sub$"))
    app.add_handler(CallbackQueryHandler(cb_user_phone,       pattern="^user_phone$"))
    app.add_handler(CallbackQueryHandler(cb_phone_type,       pattern="^phone_type_(sms|qr|sms_skip|qr_skip)$"))
    app.add_handler(CallbackQueryHandler(cb_user_history,     pattern="^user_history$"))
    app.add_handler(CallbackQueryHandler(cb_user_stats,       pattern="^user_stats$"))
    app.add_handler(CallbackQueryHandler(cb_user_pay,         pattern="^user_pay$"))
    app.add_handler(CallbackQueryHandler(cb_user_leaders,      pattern="^user_leaders$"))
    app.add_handler(CallbackQueryHandler(cb_adm_leaders,       pattern="^adm_leaders$"))
    app.add_handler(CallbackQueryHandler(cb_adm_topics,        pattern="^adm_topics$"))
    app.add_handler(CallbackQueryHandler(cb_check_pay,         pattern="^check_pay_"))

    # ── Глобальный текст: личка = СМС код, группа = "Номер смс/куар" ──
    async def global_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.text:
            return
        log.info(f"TEXT in={update.effective_chat.type} chat={update.effective_chat.id} topic={getattr(update.message,'message_thread_id',None)} [{update.message.text[:20]!r}]")
        if update.effective_chat.type == "private":
            # Ответ поддержки (только для админов)
            reply_to_uid = context.user_data.get("support_reply_to")
            if reply_to_uid and is_admin(update.effective_user.id):
                context.user_data.pop("support_reply_to", None)
                # Чистим все флаги ожидания чтобы ничего не перехватило
                uid = update.effective_user.id
                context.bot_data.pop(f"wait_support_{uid}", None)
                context.bot_data.pop(f"wait_phone_{uid}", None)
                answer_text = update.message.text.strip()
                kb = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back_main")]]
                try:
                    await context.bot.send_message(
                        reply_to_uid,
                        f"🆘 *От Тех Поддержки*\n\n📜 Ответ: {answer_text}",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup(kb)
                    )
                    await update.message.reply_text("✅ Ответ отправлен пользователю.")
                except Exception as e:
                    await update.message.reply_text(f"❌ Не смог отправить: {e}")
                return
            if await handle_phone_number(update, context):
                return
            if await handle_support_message(update, context):
                return
            await handle_sms_code(update, context)
        else:
            await cmd_get_number(update, context)

    # ── Фото: личка = (не используется), группа = QR от оператора ──
    async def global_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.photo:
            return
        if update.effective_chat.type != "private":
            await handle_qr_photo_from_op(update, context)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, global_text_handler))
    app.add_handler(MessageHandler(filters.PHOTO, global_photo_handler))

    async def global_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or update.effective_chat.type != "private":
            return
        await handle_db_upload(update, context)

    app.add_handler(MessageHandler(filters.Document.ALL, global_document_handler))

    log.info("🚀 Бот запущен! Ctrl+C для остановки.")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
