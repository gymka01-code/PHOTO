"""
PhotoFlip — Telegram Mini App Backend  v5.0
FastAPI + Aiogram 3 + aiosqlite  |  VPS edition

CHANGES in v5:
 - VPS-ready: runs on 0.0.0.0:80, no tunnel required
 - WEBAPP_URL read from env var or hardcoded fallback
 - Portfolio privacy: sell_at / expiry_time never exposed to users
 - Soft conversion: channel-subscription check fires ONLY at withdrawal
 - Full RU / EN localization driven by player.lang
 - Referral system 2.0  →  /api/referrals endpoint
 - Admin panel: stats + reply to support via API
 - VIP priority flag on all withdrawal requests
 - 24-hour inactivity reminder worker
 - Fixed KeyError in get_quest_status (uses list(ch.keys())[0])
"""

import asyncio
import logging
import math
import os
import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    WebAppInfo,
)
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN    = os.getenv("BOT_TOKEN",  "8700481112:AAGwUZffQtN0r9KsEq_dZk3liQeLg_9L3Xw")
ADMIN_ID     = int(os.getenv("ADMIN_ID", "7502434760"))
PORT         = int(os.getenv("PORT", "80"))

WEBHOOK_PATH = "/webhook"

# ── Укажи здесь домен или IP своего VPS (с https:// если есть SSL,
#    или http://123.45.67.89 если пока без).
#    Можно переопределить через переменную окружения: export WEBAPP_URL=https://yourdomain.com
WEBAPP_URL   = os.getenv("WEBAPP_URL", "http://YOUR_VPS_IP_OR_DOMAIN")
WEBHOOK_URL  = f"{WEBAPP_URL}{WEBHOOK_PATH}"

DB_PATH     = "photoflip.db"
UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(exist_ok=True)

# ── Economy ──────────────────────────────────────────────────
RUB_TO_USD_RATE        = 92.0
COMMISSION_PCT         = 0.02
SINGLE_MIN_RUB         = 200
SINGLE_MAX_RUB         = 600
PACK_MIN_RUB           = 1_000
PACK_MAX_RUB           = 3_000
PACK_SIZE              = 5
MIN_REFERRALS_WITHDRAW = 3

# ── VIP tiers: (min_referrals, max_delay_seconds, slot_limit) ─
VIP_TIERS = [
    (0,  3600,  5),   # Lvl 0:  0-2  refs →  60 min,  5 slots
    (3,  2700, 10),   # Lvl 1:  3-4  refs →  45 min, 10 slots
    (5,  1800, 15),   # Lvl 2:  5-9  refs →  30 min, 15 slots
    (10,  900, 20),   # Lvl 3: 10-24 refs →  15 min, 20 slots
    (25,  300, 25),   # Lvl 4: 25-49 refs →   5 min, 25 slots
    (50,   60, 30),   # Lvl 5: 50+   refs →   1 min, 30 slots
]
MIN_DELAY_SECS = 30

PARTNER_CHANNELS = [
    {"-1003642113064": "@dsdfsdfawer", "name": "PhotoFlip Community",
     "url": "https://t.me/dsdfsdfawer"},
]

NPC_NAMES = [
    "Sheikh Al-Rashid", "Victoria Chen", "Marcus Webb", "Priya Nair",
    "Alejandro Torres", "Yuki Tanaka", "Isabella Rossi", "Omar Hassan",
    "Sophie Laurent", "Raj Patel",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  LOCALIZATION
# ═══════════════════════════════════════════════════════════════
_T = {
    "en": {
        "welcome": (
            "👋 Welcome to <b>PhotoFlip</b>!\n\n"
            "📸 Upload photos → Valuation → Auction → Earn USD\n\n"
            "💰 Balance: <b>${balance:.2f}</b>\n"
            "⭐ VIP Level: <b>{vip}</b> · Slots: <b>{slots}</b>\n\n"
            "🔗 Your referral link:\n<code>{ref_url}</code>\n\n"
            "Invite <b>3 friends</b> to unlock withdrawal.\n\n"
            "Tap below to open PhotoFlip:"
        ),
        "btn_open":        "📸 Open PhotoFlip",
        "btn_referrals":   "🤝 Referrals",
        "referrals_msg": (
            "🤝 <b>Your referrals: {count}</b>\n\n"
            "Share your link — each friend unlocks faster sales "
            "and higher VIP level.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "new_referral": (
            "👥 <b>New referral!</b> {name} joined via your link.\n"
            "Referrals: <b>{count}</b> · VIP Level: <b>{lvl}</b> · Slots: <b>{slots}</b>"
        ),
        "sold": (
            "✅ <b>Photo sold!</b>\n\n"
            "💴 Price: <b>{rub:,} ₽</b> → <b>${gross}</b>\n"
            "📉 Project fee (2%): <b>−${commission}</b>\n"
            "💰 Credited: <b>${net}</b>\n"
            "🤵 Buyer: <b>{buyer}</b>\n\n"
            "Balance: <b>${balance:.2f}</b>"
        ),
        "status_auction":  "In Auction",
        "support_reply":   "📨 <b>Support reply:</b>\n\n{text}",
        "remind": (
            "⏰ <b>PhotoFlip reminder</b>\n\n"
            "Your photos are live on auction! "
            "Invite friends to speed up sales and unlock withdrawal.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "withdraw_locked": "Invite <b>3 friends</b> to unlock withdrawal.",
        "vip_priority":    "⭐ VIP users (levels 1–5) get priority in the withdrawal queue.",
        "sub_required_ru": "Почти готово! Подпишитесь на каналы партнёров для верификации.",
        "sub_required_en": "Almost there! Subscribe to partner channels to verify your account.",
    },
    "ru": {
        "welcome": (
            "👋 Добро пожаловать в <b>PhotoFlip</b>!\n\n"
            "📸 Загрузи фото → Оценка → Аукцион → Заработай USD\n\n"
            "💰 Баланс: <b>${balance:.2f}</b>\n"
            "⭐ VIP Уровень: <b>{vip}</b> · Слотов: <b>{slots}</b>\n\n"
            "🔗 Ваша реферальная ссылка:\n<code>{ref_url}</code>\n\n"
            "Пригласите <b>3 друзей</b> для активации вывода.\n\n"
            "Нажмите ниже, чтобы открыть PhotoFlip:"
        ),
        "btn_open":        "📸 Открыть PhotoFlip",
        "btn_referrals":   "🤝 Рефералы",
        "referrals_msg": (
            "🤝 <b>Ваши рефералы: {count}</b>\n\n"
            "Делитесь ссылкой — каждый друг ускоряет продажи "
            "и повышает VIP-уровень.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "new_referral": (
            "👥 <b>Новый реферал!</b> {name} зарегистрировался по вашей ссылке.\n"
            "Рефералов: <b>{count}</b> · VIP Уровень: <b>{lvl}</b> · Слотов: <b>{slots}</b>"
        ),
        "sold": (
            "✅ <b>Ваше фото продано!</b>\n\n"
            "💴 Цена: <b>{rub:,} ₽</b> → <b>${gross}</b>\n"
            "📉 Комиссия (2%): <b>−${commission}</b>\n"
            "💰 Начислено: <b>${net}</b>\n"
            "🤵 Покупатель: <b>{buyer}</b>\n\n"
            "Баланс: <b>${balance:.2f}</b>"
        ),
        "status_auction":  "На аукционе",
        "support_reply":   "📨 <b>Ответ поддержки:</b>\n\n{text}",
        "remind": (
            "⏰ <b>Напоминание PhotoFlip</b>\n\n"
            "Ваши фото на аукционе! "
            "Приглашайте друзей, чтобы ускорить продажи и разблокировать вывод.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "withdraw_locked": "Пригласите <b>3 друзей</b> для активации вывода.",
        "vip_priority":    "⭐ Пользователи с VIP-статусом (уровень 1–5) получают приоритет в очереди на вывод средств.",
        "sub_required_ru": "Почти готово! Подпишитесь на каналы партнёров для верификации.",
        "sub_required_en": "Almost there! Subscribe to partner channels to verify your account.",
    },
}


def tr(lang: str, key: str, **kw) -> str:
    """Return a localized string, fall back to English."""
    bucket = _T.get(lang, _T["en"])
    tmpl   = bucket.get(key, _T["en"].get(key, key))
    return tmpl.format(**kw) if kw else tmpl


# ═══════════════════════════════════════════════════════════════
#  MATH HELPERS
# ═══════════════════════════════════════════════════════════════

def rub_to_usd(rub: float) -> float:
    return round(rub / RUB_TO_USD_RATE, 2)

def apply_commission(usd: float) -> float:
    return round(usd * (1 - COMMISSION_PCT), 2)

def vip_level(refs: int) -> int:
    lvl = 0
    for i, (thr, _, _) in enumerate(VIP_TIERS):
        if refs >= thr:
            lvl = i
    return lvl

def vip_max_delay(refs: int) -> int:
    return VIP_TIERS[vip_level(refs)][1]

def vip_slot_limit(refs: int) -> int:
    return VIP_TIERS[vip_level(refs)][2]

def usd_to_stars(usd: float) -> int:
    """100 Stars = 1.20 USD  →  Stars = floor(usd / 0.012)"""
    return math.floor(usd / 0.012)


# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # players
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id         INTEGER PRIMARY KEY,
                username        TEXT,
                balance         REAL    DEFAULT 0.0,
                total_earned    REAL    DEFAULT 0.0,
                photos_sold     INTEGER DEFAULT 0,
                referrals_count INTEGER DEFAULT 0,
                lang            TEXT    DEFAULT 'en',
                last_seen       TEXT    DEFAULT (datetime('now')),
                created_at      TEXT    DEFAULT (datetime('now'))
            )""")
        for col in [
            "referrals_count INTEGER DEFAULT 0",
            "lang TEXT DEFAULT 'en'",
            "last_seen TEXT DEFAULT (datetime('now'))",
        ]:
            try:
                await db.execute(f"ALTER TABLE players ADD COLUMN {col}")
            except Exception:
                pass

        # photos
        await db.execute("""
            CREATE TABLE IF NOT EXISTS photos (
                id          TEXT PRIMARY KEY,
                user_id     INTEGER,
                filename    TEXT,
                batch_id    TEXT,
                base_price  REAL,
                final_price REAL,
                sale_rub    REAL    DEFAULT 0,
                status      TEXT    DEFAULT 'pending',
                sell_at     TEXT,
                sold_at     TEXT,
                buyer       TEXT,
                created_at  TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY(user_id) REFERENCES players(user_id)
            )""")
        for col in ["batch_id TEXT", "sale_rub REAL DEFAULT 0"]:
            try:
                await db.execute(f"ALTER TABLE photos ADD COLUMN {col}")
            except Exception:
                pass

        # quests
        await db.execute("""
            CREATE TABLE IF NOT EXISTS quests (
                user_id    INTEGER,
                channel_id TEXT,
                completed  INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, channel_id),
                FOREIGN KEY(user_id) REFERENCES players(user_id)
            )""")

        # referrals
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id INTEGER,
                referred_id INTEGER PRIMARY KEY,
                created_at  TEXT DEFAULT (datetime('now'))
            )""")

        # support_messages
        await db.execute("""
            CREATE TABLE IF NOT EXISTS support_messages (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                text         TEXT,
                direction    TEXT,
                admin_msg_id INTEGER,
                created_at   TEXT DEFAULT (datetime('now'))
            )""")

        # withdrawal_requests  (tracks VIP priority)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                amount_usd  REAL,
                stars       INTEGER DEFAULT 0,
                method      TEXT,
                is_priority INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now'))
            )""")

        await db.commit()


# ── CRUD helpers ─────────────────────────────────────────────

async def get_or_create_player(user_id: int, username: str = "") -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        if row is None:
            await db.execute(
                "INSERT INTO players (user_id, username) VALUES (?,?)", (user_id, username)
            )
            await db.commit()
            async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as c:
                row = await c.fetchone()
        return dict(row)


async def get_player(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        return dict(row) if row else None


async def touch_last_seen(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE players SET last_seen=datetime('now') WHERE user_id=?", (user_id,)
        )
        await db.commit()


async def get_player_photos(user_id: int, lang: str = "en") -> list:
    """Returns photos WITHOUT sell_at — user must not know the auction end time."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM photos WHERE user_id=? ORDER BY created_at DESC LIMIT 50",
            (user_id,),
        ) as c:
            rows = await c.fetchall()
    result = []
    for r in rows:
        p = dict(r)
        p.pop("sell_at", None)          # ← HIDDEN: no expiry time for users
        if p.get("status") == "on_auction":
            p["status_label"] = tr(lang, "status_auction")
        result.append(p)
    return result


async def get_active_photo_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'", (user_id,)
        ) as c:
            row = await c.fetchone()
        return row[0] if row else 0


async def get_quest_status(user_id: int) -> list:
    """Fixed: uses list(ch.keys())[0] instead of ch['id']."""
    result = []
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for ch in PARTNER_CHANNELS:
            channel_id = list(ch.keys())[0]          # ← bug-fix
            async with db.execute(
                "SELECT completed FROM quests WHERE user_id=? AND channel_id=?",
                (user_id, channel_id),
            ) as c:
                row = await c.fetchone()
            result.append({
                **ch,
                "id":        channel_id,
                "completed": bool(row["completed"]) if row else False,
            })
    return result


async def get_referral_list(referrer_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT r.referred_id, r.created_at, p.username,
                      CASE WHEN p.photos_sold > 0 THEN 1 ELSE 0 END AS is_active
               FROM referrals r
               LEFT JOIN players p ON p.user_id = r.referred_id
               WHERE r.referrer_id = ?
               ORDER BY r.created_at DESC""",
            (referrer_id,),
        ) as c:
            rows = await c.fetchall()
    return [dict(r) for r in rows]


async def get_admin_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT COUNT(*) AS cnt FROM players") as c:
            users = (await c.fetchone())["cnt"]
        async with db.execute("SELECT COUNT(*) AS cnt FROM photos") as c:
            photos = (await c.fetchone())["cnt"]
        async with db.execute("SELECT COALESCE(SUM(balance),0) AS s FROM players") as c:
            pending = round((await c.fetchone())["s"], 2)
    return {"users": users, "photos": photos, "pending_withdraw_usd": pending}


async def channels_all_subscribed(user_id: int) -> bool:
    """True only if every partner channel is marked completed."""
    for ch in PARTNER_CHANNELS:
        channel_id = list(ch.keys())[0]
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT completed FROM quests WHERE user_id=? AND channel_id=?",
                (user_id, channel_id),
            ) as c:
                row = await c.fetchone()
        if not row or not row["completed"]:
            return False
    return True


async def referral_url(user_id: int) -> str:
    try:
        me = await bot.get_me()
        return f"https://t.me/{me.username}?start=ref_{user_id}"
    except Exception:
        return ""


# ═══════════════════════════════════════════════════════════════
#  BACKGROUND WORKERS
# ═══════════════════════════════════════════════════════════════

async def auction_worker():
    """Resolve expired auctions every 15 seconds."""
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                now = datetime.utcnow().isoformat()
                async with db.execute(
                    "SELECT * FROM photos WHERE status='on_auction' AND sell_at<=?", (now,)
                ) as c:
                    due = await c.fetchall()

                for photo in due:
                    photo   = dict(photo)
                    buyer   = random.choice(NPC_NAMES)
                    sale_rub = photo.get("sale_rub") or random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
                    gross   = rub_to_usd(float(sale_rub))
                    net     = apply_commission(gross)
                    commission = round(gross - net, 2)

                    await db.execute(
                        """UPDATE photos SET status='sold', sold_at=datetime('now'),
                           buyer=?, final_price=?, sale_rub=? WHERE id=?""",
                        (buyer, net, sale_rub, photo["id"]),
                    )
                    await db.execute(
                        """UPDATE players SET balance=balance+?, total_earned=total_earned+?,
                           photos_sold=photos_sold+1 WHERE user_id=?""",
                        (net, net, photo["user_id"]),
                    )
                    await db.commit()

                    try:
                        player = await get_player(photo["user_id"])
                        if not player:
                            continue
                        lang    = player.get("lang", "en")
                        new_bal = round((player["balance"] or 0) + net, 2)
                        await bot.send_message(
                            photo["user_id"],
                            tr(lang, "sold",
                               rub=int(sale_rub), gross=gross, commission=commission,
                               net=net, buyer=buyer, balance=new_bal),
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception as e:
                        logger.debug(f"Notify failed {photo['user_id']}: {e}")
        except Exception as e:
            logger.error(f"auction_worker: {e}")
        await asyncio.sleep(15)


async def reminder_worker():
    """Ping users who haven't visited in 24 h, once per hour."""
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT user_id, lang FROM players WHERE last_seen < ?", (cutoff,)
                ) as c:
                    rows = await c.fetchall()
            for row in rows:
                uid  = row["user_id"]
                lang = row["lang"] or "en"
                ref  = await referral_url(uid)
                try:
                    await bot.send_message(
                        uid,
                        tr(lang, "remind", ref_url=ref),
                        parse_mode=ParseMode.HTML,
                    )
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE players SET last_seen=datetime('now') WHERE user_id=?", (uid,)
                        )
                        await db.commit()
                except Exception as e:
                    logger.debug(f"Reminder failed {uid}: {e}")
        except Exception as e:
            logger.error(f"reminder_worker: {e}")


# ═══════════════════════════════════════════════════════════════
#  AIOGRAM BOT HANDLERS
# ═══════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message):
    user   = message.from_user
    player = await get_or_create_player(user.id, user.username or "")
    await touch_last_seen(user.id)
    lang   = player.get("lang", "en")

    # ── Referral deep-link handling ──
    args = message.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1][4:])
            if referrer_id != user.id:
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(
                        "SELECT referred_id FROM referrals WHERE referred_id=?", (user.id,)
                    ) as c:
                        existing = await c.fetchone()
                    if not existing:
                        await db.execute(
                            "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)",
                            (referrer_id, user.id),
                        )
                        await db.execute(
                            "UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?",
                            (referrer_id,),
                        )
                        await db.commit()
                        try:
                            rp = await get_player(referrer_id)
                            if rp:
                                rc  = rp.get("referrals_count", 0) + 1
                                rl  = vip_level(rc)
                                rs  = vip_slot_limit(rc)
                                rlang = rp.get("lang", "en")
                                await bot.send_message(
                                    referrer_id,
                                    tr(rlang, "new_referral",
                                       name=user.first_name, count=rc, lvl=rl, slots=rs),
                                    parse_mode=ParseMode.HTML,
                                )
                        except Exception:
                            pass
        except ValueError:
            pass

    ref  = await referral_url(user.id)
    lvl  = vip_level(player.get("referrals_count", 0))
    slots = vip_slot_limit(player.get("referrals_count", 0))

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tr(lang, "btn_open"),
                              web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(text=tr(lang, "btn_referrals"),
                              callback_data="show_referrals")],
    ])
    await message.answer(
        tr(lang, "welcome",
           balance=player["balance"] or 0, vip=lvl, slots=slots, ref_url=ref),
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@dp.callback_query(F.data == "show_referrals")
async def cb_referrals(cb: CallbackQuery):
    await cb.answer()
    player = await get_player(cb.from_user.id)
    lang   = (player or {}).get("lang", "en")
    ref    = await referral_url(cb.from_user.id)
    count  = (player or {}).get("referrals_count", 0)
    await cb.message.answer(
        tr(lang, "referrals_msg", count=count, ref_url=ref),
        parse_mode=ParseMode.HTML,
    )


@dp.message(F.reply_to_message)
async def admin_reply(message: Message):
    """Admin replies to a forwarded support message → delivered to user in-chat."""
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return
    replied_id = message.reply_to_message.message_id
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id FROM support_messages WHERE admin_msg_id=? AND direction='in'",
            (replied_id,),
        ) as c:
            row = await c.fetchone()
        if not row:
            return
        target   = row["user_id"]
        txt      = message.text or message.caption or ""
        await db.execute(
            "INSERT INTO support_messages (user_id, text, direction, admin_msg_id) VALUES (?,?,'out',?)",
            (target, txt, replied_id),
        )
        await db.commit()
    p    = await get_player(target)
    lang = (p or {}).get("lang", "en")
    try:
        await bot.send_message(target, tr(lang, "support_reply", text=txt),
                               parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"support reply delivery failed: {e}")


# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── 1. Init DB ─────────────────────────────────────────────
    await init_db()

    # ── 2. Register Telegram webhook ──────────────────────────
    print(f"\n{'='*55}")
    print(f"  🌐  App URL  : {WEBAPP_URL}")
    print(f"  🔗  Webhook  : {WEBHOOK_URL}")
    print(f"{'='*55}\n")
    try:
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
        logger.info("Webhook registered successfully.")
    except Exception as e:
        logger.warning(f"Webhook registration failed: {e}")

    # ── 3. Start background workers ───────────────────────────
    t1 = asyncio.create_task(auction_worker())
    t2 = asyncio.create_task(reminder_worker())

    yield  # ←── app is running

    t1.cancel()
    t2.cancel()
    await bot.delete_webhook()


app = FastAPI(title="PhotoFlip API v4", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")


# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return FileResponse("index.html")


# ── Player ────────────────────────────────────────────────────

@app.get("/api/player/{user_id}")
async def api_get_player(user_id: int, username: str = ""):
    player    = await get_or_create_player(user_id, username)
    lang      = player.get("lang", "en")
    photos    = await get_player_photos(user_id, lang)   # sell_at stripped here
    quests    = await get_quest_status(user_id)
    ref_count = player.get("referrals_count", 0)
    lvl       = vip_level(ref_count)
    slots     = vip_slot_limit(ref_count)
    active    = await get_active_photo_count(user_id)
    ref       = await referral_url(user_id)
    await touch_last_seen(user_id)

    return {
        "player":              player,
        "photos":              photos,
        "quests":              quests,
        "withdraw_unlocked":   ref_count >= MIN_REFERRALS_WITHDRAW,
        "vip_level":           lvl,
        "vip_tiers":           [{"min": t[0], "max_delay": t[1], "slots": t[2]} for t in VIP_TIERS],
        "referral_url":        ref,
        "rub_rate":            RUB_TO_USD_RATE,
        "active_slots":        active,
        "slot_limit":          slots,
        "min_referrals_withdraw": MIN_REFERRALS_WITHDRAW,
        # Localised UI hints (no mention of channel subscription here)
        "withdraw_condition":  tr(lang, "withdraw_locked"),
        "vip_priority_note":   tr(lang, "vip_priority"),
    }


@app.put("/api/player/{user_id}/lang")
async def api_set_lang(user_id: int, request: Request):
    data = await request.json()
    lang = data.get("lang", "en")
    if lang not in _T:
        lang = "en"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET lang=? WHERE user_id=?", (lang, user_id))
        await db.commit()
    return {"lang": lang}


# ── Referrals ────────────────────────────────────────────────

@app.get("/api/referrals/{user_id}")
async def api_referrals(user_id: int):
    player = await get_player(user_id)
    if not player:
        raise HTTPException(404, "Player not found")
    refs = await get_referral_list(user_id)
    ref  = await referral_url(user_id)
    return {
        "referrals":       refs,
        "referrals_count": player.get("referrals_count", 0),
        "referral_url":    ref,
    }


# ── Upload ────────────────────────────────────────────────────

@app.post("/api/upload")
async def api_upload(
    user_id:  int              = Form(...),
    username: str              = Form(""),
    files:    List[UploadFile] = File(...),
):
    player = await get_or_create_player(user_id, username)

    if (player["balance"] or 0) > 0:
        raise HTTPException(403, "Withdraw your balance before uploading new photos.")

    ref_count  = player.get("referrals_count", 0)
    slot_limit = vip_slot_limit(ref_count)
    active     = await get_active_photo_count(user_id)
    num_files  = len(files)

    if not (1 <= num_files <= 5):
        raise HTTPException(400, "Upload 1 to 5 photos at a time.")

    if active + num_files > slot_limit:
        avail = max(slot_limit - active, 0)
        raise HTTPException(
            403,
            f"Slot limit reached ({active}/{slot_limit} active). "
            f"You can upload {avail} more. Upgrade VIP for more slots.",
        )

    is_pack    = (num_files == PACK_SIZE)
    if is_pack:
        total      = random.randint(PACK_MIN_RUB, PACK_MAX_RUB)
        rub_each   = [total // num_files] * num_files
    else:
        rub_each   = [random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB) for _ in range(num_files)]

    max_delay  = vip_max_delay(ref_count)
    batch_id   = uuid.uuid4().hex
    results    = []

    async with aiosqlite.connect(DB_PATH) as db:
        for idx, file in enumerate(files):
            ext      = Path(file.filename).suffix.lower() if file.filename else ".jpg"
            filename = f"{uuid.uuid4().hex}{ext}"
            (UPLOADS_DIR / filename).write_bytes(await file.read())

            delay    = random.randint(MIN_DELAY_SECS, max(max_delay, MIN_DELAY_SECS + 1))
            sell_at  = (datetime.utcnow() + timedelta(seconds=delay)).isoformat()
            sale_rub = rub_each[idx]
            prev_usd = apply_commission(rub_to_usd(sale_rub))
            pid      = uuid.uuid4().hex

            await db.execute(
                """INSERT INTO photos
                   (id, user_id, filename, batch_id, base_price, final_price,
                    sale_rub, status, sell_at)
                   VALUES (?,?,?,?,?,?,?,'on_auction',?)""",
                (pid, user_id, filename, batch_id, prev_usd, prev_usd, sale_rub, sell_at),
            )
            results.append({
                "photo_id":   pid,
                "filename":   filename,
                "base_price": prev_usd,
                "preview_rub":sale_rub,
                # sell_at intentionally NOT returned — portfolio privacy
                "status":     "on_auction",
                "vip_level":  vip_level(ref_count),
            })
        await db.commit()

    return {
        "batch_id":    batch_id,
        "is_pack":     is_pack,
        "photos":      results,
        "total_rub":   sum(rub_each),
        "slot_limit":  slot_limit,
        "active_after":active + num_files,
    }


# ── Quest ─────────────────────────────────────────────────────

@app.post("/api/quest/complete")
async def api_quest_complete(request: Request):
    data       = await request.json()
    user_id    = data.get("user_id")
    channel_id = data.get("channel_id")
    if not user_id or not channel_id:
        raise HTTPException(400, "Missing user_id or channel_id")

    verified = False
    try:
        member   = await bot.get_chat_member(channel_id, user_id)
        verified = member.status in (
            ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR
        )
    except Exception as e:
        logger.warning(f"get_chat_member {user_id}/{channel_id}: {e}")
        verified = True   # fallback if bot not in channel

    if not verified:
        raise HTTPException(403, "User has not joined the channel yet.")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO quests (user_id, channel_id, completed) VALUES (?,?,1)",
            (user_id, channel_id),
        )
        await db.commit()

    player    = await get_player(user_id)
    ref_count = (player or {}).get("referrals_count", 0)
    return {
        "quests":           await get_quest_status(user_id),
        "withdraw_unlocked":ref_count >= MIN_REFERRALS_WITHDRAW,
    }


# ── Withdraw ─────────────────────────────────────────────────
#  Soft-conversion: channel subscription is checked ONLY here.
#  The main menu never mentions it — only "invite 3 friends".

def _build_sub_required_response(lang: str) -> JSONResponse:
    channels = [
        {"id": list(ch.keys())[0], "url": ch.get("url", ""), "name": ch.get("name", "")}
        for ch in PARTNER_CHANNELS
    ]
    return JSONResponse(
        status_code=402,
        content={
            "error":    "subscription_required",
            "channels": channels,
            "message":  tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en"),
        },
    )


@app.post("/api/withdraw")
async def api_withdraw(request: Request):
    data    = await request.json()
    user_id = data.get("user_id")
    if not user_id:
        raise HTTPException(400, "Missing user_id")

    player = await get_player(user_id)
    if not player:
        raise HTTPException(404, "Player not found")

    lang      = player.get("lang", "en")
    ref_count = player.get("referrals_count", 0)
    if ref_count < MIN_REFERRALS_WITHDRAW:
        raise HTTPException(403, tr(lang, "withdraw_locked"))

    # ← Soft conversion: subscription modal fires here
    if not await channels_all_subscribed(user_id):
        return _build_sub_required_response(lang)

    if (player["balance"] or 0) <= 0:
        raise HTTPException(400, "Nothing to withdraw.")

    amount      = round(player["balance"], 2)
    lvl         = vip_level(ref_count)
    is_priority = 1 if lvl >= 1 else 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET balance=0 WHERE user_id=?", (user_id,))
        await db.execute(
            "INSERT INTO withdrawal_requests (user_id, amount_usd, method, is_priority) "
            "VALUES (?,?,?,?)",
            (user_id, amount, "usd", is_priority),
        )
        await db.commit()

    if ADMIN_ID:
        try:
            prio_tag = "⭐ VIP PRIORITY\n" if is_priority else ""
            await bot.send_message(
                ADMIN_ID,
                f"💳 <b>USD Withdrawal</b>\n{prio_tag}"
                f"User: <code>{user_id}</code> (@{player.get('username','')})\n"
                f"Amount: <b>${amount:.2f}</b> · VIP <b>{lvl}</b> · Refs <b>{ref_count}</b>\n"
                f"<i>{tr('en','vip_priority')}</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    return {"success": True, "withdrawn_usd": amount, "new_balance": 0.0}


@app.post("/api/withdraw/stars")
async def api_withdraw_stars(request: Request):
    data    = await request.json()
    user_id = data.get("user_id")
    if not user_id:
        raise HTTPException(400, "Missing user_id")

    player = await get_player(user_id)
    if not player:
        raise HTTPException(404, "Player not found")

    lang      = player.get("lang", "en")
    ref_count = player.get("referrals_count", 0)
    if ref_count < MIN_REFERRALS_WITHDRAW:
        raise HTTPException(403, tr(lang, "withdraw_locked"))

    if not await channels_all_subscribed(user_id):
        return _build_sub_required_response(lang)

    if (player["balance"] or 0) <= 0:
        raise HTTPException(400, "Nothing to withdraw.")

    usd_amount   = round(player["balance"], 2)
    stars_amount = usd_to_stars(usd_amount)
    lvl          = vip_level(ref_count)
    is_priority  = 1 if lvl >= 1 else 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET balance=0 WHERE user_id=?", (user_id,))
        await db.execute(
            "INSERT INTO withdrawal_requests (user_id, amount_usd, stars, method, is_priority) "
            "VALUES (?,?,?,?,?)",
            (user_id, usd_amount, stars_amount, "stars", is_priority),
        )
        await db.commit()

    if ADMIN_ID:
        try:
            prio_tag = "⭐ VIP PRIORITY\n" if is_priority else ""
            await bot.send_message(
                ADMIN_ID,
                f"⭐ <b>Stars Withdrawal</b>\n{prio_tag}"
                f"User: <code>{user_id}</code> (@{player.get('username','')})\n"
                f"${usd_amount:.2f} → <b>{stars_amount:,} ⭐</b> · VIP <b>{lvl}</b>\n"
                f"<i>{tr('en','vip_priority')}</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    return {"success": True, "withdrawn_usd": usd_amount,
            "stars": stars_amount, "new_balance": 0.0}


# ── Support ───────────────────────────────────────────────────

@app.post("/api/support/send")
async def api_support_send(request: Request):
    data    = await request.json()
    user_id = data.get("user_id")
    text    = (data.get("text") or "").strip()
    if not user_id or not text:
        raise HTTPException(400, "Missing user_id or text")

    player = await get_player(user_id)
    if not player:
        raise HTTPException(404, "Player not found")

    admin_msg_id = None
    if ADMIN_ID:
        try:
            uname = player.get("username") or str(user_id)
            sent  = await bot.send_message(
                ADMIN_ID,
                f"🎧 <b>Support — PhotoFlip</b>\n"
                f"From: <code>{user_id}</code> (@{uname})\n\n{text}",
                parse_mode=ParseMode.HTML,
            )
            admin_msg_id = sent.message_id
        except Exception as e:
            logger.warning(f"support forward failed: {e}")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO support_messages (user_id, text, direction, admin_msg_id) "
            "VALUES (?,?,'in',?)",
            (user_id, text, admin_msg_id),
        )
        await db.commit()
    return {"success": True}


@app.get("/api/support/messages/{user_id}")
async def api_support_messages(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, text, direction, created_at FROM support_messages "
            "WHERE user_id=? ORDER BY created_at ASC LIMIT 100",
            (user_id,),
        ) as c:
            rows = await c.fetchall()
    return {"messages": [dict(r) for r in rows]}


# ── Admin ─────────────────────────────────────────────────────

def _require_admin(request: Request):
    """Simple token check: pass X-Admin-Token: <ADMIN_ID> header."""
    if request.headers.get("X-Admin-Token", "") != str(ADMIN_ID):
        raise HTTPException(403, "Forbidden")


@app.get("/api/admin/stats")
async def api_admin_stats(request: Request):
    _require_admin(request)
    return await get_admin_stats()


@app.get("/api/admin/withdrawals")
async def api_admin_withdrawals(request: Request):
    """VIP-priority requests appear first."""
    _require_admin(request)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT wr.*, p.username FROM withdrawal_requests wr
               LEFT JOIN players p ON p.user_id = wr.user_id
               ORDER BY wr.is_priority DESC, wr.created_at ASC"""
        ) as c:
            rows = await c.fetchall()
    return {"withdrawals": [dict(r) for r in rows]}


@app.post("/api/admin/support/reply")
async def api_admin_support_reply(request: Request):
    """Admin replies to a user via API (alternative to Telegram reply)."""
    _require_admin(request)
    data         = await request.json()
    user_id      = data.get("user_id")
    reply_text   = (data.get("text") or "").strip()
    admin_msg_id = data.get("admin_msg_id")   # optional reference

    if not user_id or not reply_text:
        raise HTTPException(400, "Missing user_id or text")

    player = await get_player(user_id)
    if not player:
        raise HTTPException(404, "Player not found")
    lang = player.get("lang", "en")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO support_messages (user_id, text, direction, admin_msg_id) "
            "VALUES (?,?,'out',?)",
            (user_id, reply_text, admin_msg_id),
        )
        await db.commit()

    try:
        await bot.send_message(
            user_id,
            tr(lang, "support_reply", text=reply_text),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(f"admin api reply delivery failed: {e}")

    return {"success": True}


# ── Webhook ───────────────────────────────────────────────────

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    from aiogram.types import Update
    update = Update(**await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    # Порт 80 — сайт открывается напрямую по IP без указания порта.
    # Если нужен SSL (HTTPS), поставь Nginx как reverse proxy на 443 → 80.
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
