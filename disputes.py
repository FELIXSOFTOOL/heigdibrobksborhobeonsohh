"""
disputes.py — обработка диспутов (номер не встал).
Все зависимости передаются через init_disputes() один раз при старте.
"""
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes, CallbackQueryHandler
import logging

log = logging.getLogger(__name__)

# Зависимости — заполняются через init_disputes()
_db = None
_get_setting = None
_now_msk = None
_esc_md = None
_is_admin = None
_is_buyer = None
_ADMIN_IDS = []
_buyer_prepay_on = None
_buyer_prepay_price = None
_buyer_panel_markup = None
_refund_buyer_prepay = None
_safe_edit = None

_DISPUTE_STATE_KEYS = (
    "dispute_qid",
    "dispute_num",
    "dispute_ptyp",
    "dispute_prev_status",
    "dispute_reason",
)


def _pending_key(uid: int) -> str:
    return f"dispute_pending_{uid}"


def _make_state(queue_id: int, number: str, ptype: str, prev_status: str = None, reason: str = "not_stood") -> dict:
    return {
        "dispute_qid": queue_id,
        "dispute_num": number,
        "dispute_ptyp": ptype or "sms",
        "dispute_prev_status": prev_status or "waiting",
        "dispute_reason": reason or "not_stood",
    }


def _store_pending_state(context: ContextTypes.DEFAULT_TYPE, uid: int, state: dict, *, current_user: bool = False):
    """Store pending proof state both in user_data and bot_data fallback."""
    if current_user:
        context.user_data.update(state)

    app_ud = context.application.user_data.get(uid)
    if app_ud is not None:
        app_ud.update(state)

    context.bot_data[_pending_key(uid)] = dict(state)


def _clear_pending_state(context: ContextTypes.DEFAULT_TYPE, uid: int):
    for key in _DISPUTE_STATE_KEYS:
        context.user_data.pop(key, None)

    app_ud = context.application.user_data.get(uid)
    if app_ud is not None:
        for key in _DISPUTE_STATE_KEYS:
            app_ud.pop(key, None)

    context.bot_data.pop(_pending_key(uid), None)


def _pop_pending_state(context: ContextTypes.DEFAULT_TYPE, uid: int) -> dict:
    app_ud = context.application.user_data.get(uid) or {}
    bot_ud = context.bot_data.get(_pending_key(uid)) or {}

    queue_id = (
        context.user_data.get("dispute_qid")
        or app_ud.get("dispute_qid")
        or bot_ud.get("dispute_qid")
    )
    if not queue_id:
        return {}

    state = {
        "dispute_qid": queue_id,
        "dispute_num": (
            context.user_data.get("dispute_num")
            or app_ud.get("dispute_num")
            or bot_ud.get("dispute_num")
            or "—"
        ),
        "dispute_ptyp": (
            context.user_data.get("dispute_ptyp")
            or app_ud.get("dispute_ptyp")
            or bot_ud.get("dispute_ptyp")
            or "sms"
        ),
        "dispute_prev_status": (
            context.user_data.get("dispute_prev_status")
            or app_ud.get("dispute_prev_status")
            or bot_ud.get("dispute_prev_status")
            or "waiting"
        ),
        "dispute_reason": (
            context.user_data.get("dispute_reason")
            or app_ud.get("dispute_reason")
            or bot_ud.get("dispute_reason")
            or "not_stood"
        ),
    }
    _clear_pending_state(context, uid)
    return state


def _restore_status_from_state(state: dict) -> str:
    prev_status = (state or {}).get("dispute_prev_status") or "waiting"
    allowed = {
        "waiting",
        "confirmed",
        "wait_sms_confirm",
        "wait_sms_code",
        "sms_sent",
        "wait_qr_confirm",
        "wait_qr_photo",
        "qr_sent",
        "cancelled",
        "fell",
    }
    return prev_status if prev_status in allowed else "waiting"


def _reason_text(reason: str) -> str:
    return "Номер слетел до холда" if reason == "fell_early" else "Номер не встал"


def init_disputes(bot_module):
    """
    Вызвать один раз при старте бота, передав модуль бота.
    Заполняет все зависимости.
    """
    global _db, _get_setting, _now_msk, _esc_md, _is_admin, _is_buyer
    global _ADMIN_IDS, _buyer_prepay_on, _buyer_prepay_price
    global _buyer_panel_markup, _refund_buyer_prepay, _safe_edit

    _db                 = bot_module.db
    _get_setting        = bot_module.get_setting
    _now_msk            = bot_module.now_msk
    _esc_md             = bot_module.esc_md
    _is_admin           = bot_module.is_admin
    _is_buyer           = bot_module.is_buyer
    _ADMIN_IDS          = bot_module.ADMIN_IDS
    _buyer_prepay_on    = bot_module._buyer_prepay_on
    _buyer_prepay_price = bot_module._buyer_prepay_price
    _buyer_panel_markup = bot_module._buyer_panel_markup
    _refund_buyer_prepay = bot_module._refund_buyer_prepay
    _safe_edit          = bot_module.safe_edit


# ─────────────────────────────────────────────────────────
# Покупатель нажал "Не встал" — просим скриншот
# ─────────────────────────────────────────────────────────
async def cb_buy_dispute_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    buyer_uid = q.from_user.id
    queue_id  = int(q.data.split("buy_dispute_start_", 1)[1])

    with _db() as conn:
        an = conn.execute(
            "SELECT number, phone_type, status FROM active_numbers WHERE queue_id=? AND op_id=?",
            (queue_id, buyer_uid)
        ).fetchone()
    if not an:
        await q.answer("❌ Не найдено.", show_alert=True); return
    number = an[0]
    ptype  = an[1] or "sms"
    status = an[2]
    with _db() as conn:
        open_dispute = conn.execute(
            "SELECT id FROM buyer_disputes "
            "WHERE queue_id=? AND buyer_uid=? AND status='open' "
            "ORDER BY id DESC LIMIT 1",
            (queue_id, buyer_uid)
        ).fetchone()

    if open_dispute:
        await q.answer("⚠️ Жалоба уже отправлена и ждёт решения администратора.", show_alert=True); return
    if status in ("not_stood", "cancelled", "stood", "paid"):
        await q.answer("⚠️ Номер уже закрыт.", show_alert=True); return

    _store_pending_state(
        context,
        buyer_uid,
        _make_state(queue_id, number, ptype, status),
        current_user=True,
    )

    with _db() as conn:
        conn.execute("UPDATE active_numbers SET status='disputed' WHERE queue_id=?", (queue_id,))

    await q.edit_message_text(
        f"🚫 *Номер не встал* — жалоба\n\n"
        f"Номер: `{number}`\n\n"
        f"📸 *Пришлите скриншот* подтверждающий что номер не встал.\n"
        f"Скриншот будет отправлен администратору.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Отменить жалобу", callback_data=f"buy_dispute_cancel_{queue_id}")
        ]])
    )


# ─────────────────────────────────────────────────────────
# Покупатель отменил жалобу
# ─────────────────────────────────────────────────────────
async def cb_buy_dispute_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    queue_id = int(q.data.split("buy_dispute_cancel_", 1)[1])
    uid = q.from_user.id
    state = _pop_pending_state(context, uid)
    restore_status = _restore_status_from_state(state)
    with _db() as conn:
        conn.execute(
            "UPDATE active_numbers SET status=? WHERE queue_id=? AND status='disputed'",
            (restore_status, queue_id)
        )
    await q.edit_message_text(
        "↩️ Жалоба отменена.",
        parse_mode="Markdown",
        reply_markup=_buyer_panel_markup(uid)
    )


# ─────────────────────────────────────────────────────────
# Принимаем скриншот от покупателя — вызывается из global_photo_handler бота
# ─────────────────────────────────────────────────────────
async def handle_dispute_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Вызывать из global_photo_handler когда покупатель должен прислать скрин.
    Берёт состояние из context.user_data (покупатель нажал сам)
    ИЛИ из context.application.user_data[uid] (дроп нажал op_not_stood).
    Возвращает True если обработали, False если нет.
    """
    uid = update.effective_user.id
    state = _pop_pending_state(context, uid)
    if not state:
        return False

    queue_id = int(state["dispute_qid"])
    number = state.get("dispute_num") or "—"
    ptype = state.get("dispute_ptyp") or "sms"
    reason_text = _reason_text(state.get("dispute_reason"))

    buyer = update.effective_user
    buyer_tag  = f"@{_esc_md(buyer.username)}" if buyer.username else f"ID `{uid}`"
    type_label = "💬 СМС" if ptype == "sms" else "📷 QR"

    with _db() as conn:
        an = conn.execute(
            "SELECT number, phone_type, op_id, status FROM active_numbers WHERE queue_id=?",
            (queue_id,)
        ).fetchone()
        existing_dispute = conn.execute(
            "SELECT id FROM buyer_disputes "
            "WHERE queue_id=? AND buyer_uid=? AND status='open' "
            "ORDER BY id DESC LIMIT 1",
            (queue_id, uid)
        ).fetchone()
        pp = conn.execute(
            "SELECT id, amount FROM buyer_prepay_pending "
            "WHERE buyer_uid=? AND queue_id=? AND status='paid' ORDER BY id DESC LIMIT 1",
            (uid, queue_id)
        ).fetchone()
        drop_row = conn.execute(
            "SELECT username, user_id FROM active_numbers WHERE queue_id=?", (queue_id,)
        ).fetchone()

    if existing_dispute:
        await update.message.reply_text(
            f"⚠️ *Жалоба #{existing_dispute[0]} уже принята и ждёт решения администратора.*",
            parse_mode="Markdown"
        )
        return True

    if not an:
        await update.message.reply_text(
            "❌ Номер уже не найден. Обратитесь к администратору.",
            parse_mode="Markdown"
        )
        return True

    number = an[0] or number
    ptype = an[1] or ptype
    type_label = "💬 СМС" if ptype == "sms" else "📷 QR"

    prepay_id  = pp[0] if pp else None
    prepay_amt = pp[1] if pp else None

    # Баланс-списание без prepay_pending записи
    if not prepay_amt and _buyer_prepay_on(uid):
        v = _buyer_prepay_price(uid)
        if v > 0:
            prepay_amt = v

    drop_tag = (
        f"@{_esc_md(drop_row[0])}" if (drop_row and drop_row[0])
        else (f"ID `{drop_row[1]}`" if drop_row else "—")
    )

    # BUG FIX: статус был "confirmed" + немедленный авто-возврат.
    # Теперь создаём диспут со статусом "open" и ждём решения администратора.
    # Это позволяет кнопкам adm_dispute_ok / adm_dispute_rej работать корректно.
    now_str = _now_msk().isoformat()
    with _db() as conn:
        conn.execute(
            "INSERT INTO buyer_disputes "
            "(queue_id, buyer_uid, number, ptype, prepay_id, status, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (queue_id, uid, number, ptype, prepay_id, "open", now_str)
        )
        dispute_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))
        conn.execute("UPDATE active_numbers SET status='disputed' WHERE queue_id=?", (queue_id,))

    # ── Капшн для канала диспутов ─────────────────────────
    prepay_str = f"\n💳 Сумма к возврату: `{prepay_amt:.2f}` USDT" if prepay_amt else ""
    caption = (
        f"🚫 *Диспут #{dispute_id}* — {reason_text}\n\n"
        f"Номер: `{number}`\n"
        f"Дроп: {drop_tag}\n"
        f"Покупатель: {buyer_tag}\n"
        f"Метод: {type_label}{prepay_str}\n\n"
        f"📸 Скриншот выше."
    )

    # Кнопки для принятия решения админом
    kb_d = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить — вернуть деньги",     callback_data=f"adm_dispute_ok_{dispute_id}")],
        [InlineKeyboardButton("❌ Отклонить — деньги не возвращать", callback_data=f"adm_dispute_rej_{dispute_id}")],
    ])

    photo = update.message.photo[-1].file_id
    group_msg_id = None
    dispute_group = _get_setting("dispute_channel_id")

    if dispute_group:
        try:
            raw = dispute_group.strip()
            chat_dest = int(raw) if raw.lstrip("-").isdigit() else raw

            msg = await context.bot.send_photo(
                chat_dest, photo,
                caption=caption, parse_mode="Markdown", reply_markup=kb_d
            )
            group_msg_id = msg.message_id
            with _db() as conn:
                conn.execute(
                    "UPDATE buyer_disputes SET channel_msg_id=? WHERE id=?",
                    (group_msg_id, dispute_id)
                )
        except Exception as e:
            log.error(f"Диспут #{dispute_id} не отправлен в канал: {e}")
            # Пробуем уведомить админов если канал недоступен
            for adm in _ADMIN_IDS:
                try:
                    msg = await context.bot.send_photo(
                        adm, photo,
                        caption=caption + f"\n\n⚠️ Не смог отправить в канал: `{e}`",
                        parse_mode="Markdown", reply_markup=kb_d
                    )
                    group_msg_id = True
                except:
                    pass
    else:
        # Канал не настроен — шлём прямо админам
        for adm in _ADMIN_IDS:
            try:
                msg = await context.bot.send_photo(
                    adm, photo,
                    caption=caption, parse_mode="Markdown", reply_markup=kb_d
                )
                group_msg_id = True
            except:
                pass

    # ── Сообщение покупателю — ждём решения админа ────────
    await update.message.reply_text(
        f"✅ *Жалоба #{dispute_id} принята.*\n\n"
        f"Номер: `{number}`\n"
        f"Причина: {reason_text}\n\n"
        f"{'⏳ Ожидайте решения администратора.' if group_msg_id else '⚠️ Группа диспутов не настроена — обратитесь к администратору напрямую.'}",
        parse_mode="Markdown"
    )
    return True


# ─────────────────────────────────────────────────────────
# Админ подтвердил диспут → возвращаем деньги
# ─────────────────────────────────────────────────────────
async def cb_adm_dispute_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not _is_admin(q.from_user.id):
        await q.answer("⛔ Нет доступа.", show_alert=True); return
    await q.answer()
    dispute_id = int(q.data.split("adm_dispute_ok_", 1)[1])

    with _db() as conn:
        d = conn.execute(
            "SELECT queue_id, buyer_uid, number, ptype, prepay_id, status FROM buyer_disputes WHERE id=?",
            (dispute_id,)
        ).fetchone()
    if not d:
        await q.answer("❌ Диспут не найден.", show_alert=True); return
    queue_id, buyer_uid, number, ptype, prepay_id, d_status = d
    if d_status != "open":
        await q.answer("⚠️ Диспут уже закрыт.", show_alert=True); return

    now_str = _now_msk().isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE buyer_disputes SET status='confirmed', resolved_at=? WHERE id=?",
            (now_str, dispute_id)
        )
        conn.execute("UPDATE active_numbers SET status='not_stood' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))
        if prepay_id:
            conn.execute(
                "UPDATE buyer_prepay_pending SET status='refunded' WHERE id=?", (prepay_id,)
            )

    await _refund_buyer_prepay(context, queue_id, number, "Номер не встал (диспут подтверждён)")

    try:
        await context.bot.send_message(
            buyer_uid,
            f"✅ *Диспут #{dispute_id} подтверждён*\n\n"
            f"Номер `{number}` — деньги возвращены на баланс.",
            parse_mode="Markdown",
            reply_markup=_buyer_panel_markup(buyer_uid)
        )
    except Exception as e:
        log.error(f"Не смог уведомить покупателя {buyer_uid}: {e}")

    edit_fn = q.edit_message_caption if (q.message and q.message.photo) else q.edit_message_text
    await _safe_edit(
        edit_fn,
        f"✅ *Диспут #{dispute_id} подтверждён*\n\n"
        f"Покупатель: ID {buyer_uid}\n"
        f"Номер: `{number}`\n"
        f"💰 Деньги возвращены покупателю на баланс.",
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────────────────────
# Админ отклонил диспут → деньги не возвращаем
# ─────────────────────────────────────────────────────────
async def cb_adm_dispute_rej(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not _is_admin(q.from_user.id):
        await q.answer("⛔ Нет доступа.", show_alert=True); return
    await q.answer()
    dispute_id = int(q.data.split("adm_dispute_rej_", 1)[1])

    with _db() as conn:
        d = conn.execute(
            "SELECT queue_id, buyer_uid, number, ptype, prepay_id, status FROM buyer_disputes WHERE id=?",
            (dispute_id,)
        ).fetchone()
    if not d:
        await q.answer("❌ Диспут не найден.", show_alert=True); return
    queue_id, buyer_uid, number, ptype, prepay_id, d_status = d
    if d_status != "open":
        await q.answer("⚠️ Диспут уже закрыт.", show_alert=True); return

    now_str = _now_msk().isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE buyer_disputes SET status='rejected', resolved_at=? WHERE id=?",
            (now_str, dispute_id)
        )
        conn.execute("UPDATE active_numbers SET status='cancelled' WHERE queue_id=?", (queue_id,))
        conn.execute("UPDATE queue SET deleted=1 WHERE id=?", (queue_id,))
        if prepay_id:
            conn.execute(
                "UPDATE buyer_prepay_pending SET status='consumed' WHERE id=?", (prepay_id,)
            )

    try:
        await context.bot.send_message(
            buyer_uid,
            f"❌ *Диспут #{dispute_id} отклонён*\n\n"
            f"Номер `{number}` — администратор не подтвердил заявку.",
            parse_mode="Markdown",
            reply_markup=_buyer_panel_markup(buyer_uid)
        )
    except Exception as e:
        log.error(f"Не смог уведомить покупателя {buyer_uid}: {e}")

    await _safe_edit(
        q.edit_message_caption if q.message and q.message.photo else q.edit_message_text,
        f"❌ *Диспут #{dispute_id} отклонён*\n\n"
        f"Номер: `{number}`\n"
        f"{'💳 Предоплата сгорела.' if prepay_id else 'Предоплаты не было.'}",
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────────────────────
# Регистрация хэндлеров
# ─────────────────────────────────────────────────────────
def register_dispute_handlers(app, bot_module):
    """Вызвать один раз при старте — регистрирует callback-хэндлеры диспутов."""
    init_disputes(bot_module)
    # Фото НЕ регистрируем здесь — оно уже обрабатывается в global_photo_handler бота
    # через вызов handle_dispute_photo(update, context)
    app.add_handler(CallbackQueryHandler(cb_buy_dispute_start,  pattern="^buy_dispute_start_"))
    app.add_handler(CallbackQueryHandler(cb_buy_dispute_cancel, pattern="^buy_dispute_cancel_"))
    app.add_handler(CallbackQueryHandler(cb_adm_dispute_ok,     pattern="^adm_dispute_ok_"))
    app.add_handler(CallbackQueryHandler(cb_adm_dispute_rej,    pattern="^adm_dispute_rej_"))
