import asyncio
import html
import logging
import os
import random
import urllib.parse
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
import math

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    WebAppInfo,
)
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

# ═══════════════════════════════════════════════════════════════
#  CONFIG  — no defaults for secrets; use env vars only
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN = os.environ["BOT_TOKEN"]                          # REQUIRED — no fallback
ADMIN_ID  = int(os.getenv("ADMIN_ID", "0"))                 # Super-admin user_id
PORT      = int(os.getenv("PORT", "8000"))

WEBHOOK_PATH = "/webhook"
WEBAPP_URL   = os.getenv("WEBAPP_URL", "https://photo-production-d5b8.up.railway.app")
WEBHOOK_URL  = f"{WEBAPP_URL}{WEBHOOK_PATH}"

# ── Persistent storage (Railway Volume compatible) ────────────
_VOLUME     = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", ".")
DB_PATH     = os.path.join(_VOLUME, "photoflip.db")
UPLOADS_DIR = Path(os.path.join(_VOLUME, "uploads"))
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
_BOT_USERNAME_CACHE = os.path.join(_VOLUME, ".bot_username")

# ── File upload limits ────────────────────────────────────────
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

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
    (0,  3600,  5),
    (3,  2700, 10),
    (5,  1800, 15),
    (10,  900, 20),
    (25,  300, 25),
    (50,   60, 30),
]
MIN_DELAY_SECS = 30

PARTNER_CHANNELS = [
    {"-1003642113064": "@dsdfsdfawer", "name": "PhotoFlip Community",
     "url": "https://t.me/dsdfsdfawer"},

]

# ── Required subscription channel ─────────────────────────────
REQUIRED_CHANNEL_ID   = "@dsdfsdfawer"
REQUIRED_CHANNEL_URL  = "https://t.me/dsdfsdfawer"
REQUIRED_CHANNEL_NAME = "PhotoFlip Community"

FAKE_USERS = [
    "u***r7", "a***2", "m***k9", "p***y4", "t***3", "j***8", "k***5", "s***1",
    "x***z2", "q***9", "r***m3", "b***6", "c***w5", "n***4", "f***h1", "d***7",
    "w***l8", "e***v3", "g***o6", "h***i2", "y***t5", "o***p1", "z***k4", "v***s9",
    "PhotoNinja_7", "SniperLens_3", "PixelHunter_2", "SnapMaster_5",
    "Дмитрий Волков", "Артем Степанов", "Сергей Карпов", "Никита Миронов",
    "John Doe", "Mark Anthony", "Steve Parker", "Lucas Fisher", "Henry White",
    "crypto_king_77", "usdt_master", "p2p_shark", "arbitrage_pro", "solana_whale"
]

FEED_ACTIONS = [
    ("en", "🖼 Bought a photo from {seller}"),
    ("en", "💸 Sold a photo for ${amount}"),
    ("en", "🔨 Listed photo on auction"),
    ("en", "🏆 Won an auction for ${amount}"),
    ("ru", "🖼 Купил фото у {seller}"),
    ("ru", "💸 Продал фото за ${amount}"),
    ("ru", "🔨 Выставил на аукцион"),
    ("ru", "🏆 Победил в торгах за ${amount}"),
]

MARKET_NEWS_EN = [
    "📈 Demand for landscape shots up 34% in the past 24 hours!",
    "🔥 Portrait photography breaking records — average price +18%.",
    "💡 Analysts: street photos gaining popularity among buyers.",
]

MARKET_NEWS_RU = [
    "📈 Спрос на пейзажные снимки вырос на 34% за последние сутки!",
    "🔥 Портретная фотография бьёт рекорды — средняя цена +18%.",
    "💡 Аналитики: уличные фото набирают популярность среди покупателей.",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REFERRAL_NOTIFY_TMPL = (
    "🔔 <b>New Referral / Новый реферал!</b>\n\n"
    "👤 User / Пользователь: {username} (ID: <code>{user_id}</code>)\n"
    "✅ Status: Added to your team / Добавлен в команду."
)

def make_share_url(ref_url: str) -> str:
    text = (
        "Твоя камера теперь печатает деньги. Серьезно. 🖼💰\n"
        "PhotoFlip — это как биржа, только вместо акций — твои фото. "
        "Флипай лоты, лови профит в баксах и выводи.\n"
        "Залетай по моей ссылке: 🔗\n"
        "Проверим, чей лот купят быстрее? 😉"
    )
    return (
        "https://t.me/share/url"
        "?url=" + urllib.parse.quote(ref_url, safe="") +
        "&text=" + urllib.parse.quote(text, safe="")
    )

class AdminReply(StatesGroup):
    waiting_reply = State()

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
        "btn_open":      "📸 Open PhotoFlip",
        "btn_referrals": "🤝 Referrals",
        "btn_share":     "📤 Share & Invite",
        "referrals_msg": (
            "🤝 <b>Your referrals: {count}</b>\n\n"
            "Invite <b>{need}</b> more friend(s) to unlock withdrawal.\n\n"
            "Share your link — each friend raises your VIP level.\n\n"
            "🔗 Your referral link (tap to copy):\n"
            "<code>{ref_url}</code>"
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
        "withdraw_locked":  "Invite <b>3 friends</b> to unlock withdrawal.",
        "vip_priority":     "⭐ VIP users (levels 1–5) get priority in the withdrawal queue.",
        "sub_required_en":  "Almost there! Subscribe to our channel to access PhotoFlip.",
        "sub_required_ru":  "Почти готово! Подпишитесь на наш канал для доступа к PhotoFlip.",
        "lang_changed":     "🌐 Language switched to <b>English</b>.",
        "broadcast_done":   "📣 Broadcast complete. Delivered: <b>{ok}</b> users.",
        "withdraw_processing": (
            "✅ Withdrawal requested!\n\n"
            "Your request is being processed.\n"
            "Payouts take 1–7 business days."
        ),
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
        "btn_open":      "📸 Открыть PhotoFlip",
        "btn_referrals": "🤝 Рефералы",
        "btn_share":     "📤 Поделиться ссылкой",
        "referrals_msg": (
            "🤝 <b>Ваших рефералов: {count}</b>\n\n"
            "Пригласите ещё <b>{need}</b> чел., чтобы разблокировать вывод.\n\n"
            "Делитесь ссылкой — каждый друг повышает VIP-уровень.\n\n"
            "🔗 Ваша реферальная ссылка:\n"
            "<code>{ref_url}</code>"
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
        "withdraw_locked":  "Пригласите <b>3 друзей</b> для активации вывода.",
        "vip_priority":     "⭐ VIP 1-5 получают приоритет.",
        "sub_required_en":  "Almost there! Subscribe to our channel to access PhotoFlip.",
        "sub_required_ru":  "Почти готово! Подпишитесь на наш канал для доступа к PhotoFlip.",
        "lang_changed":     "🌐 Язык переключён на <b>Русский</b>.",
        "broadcast_done":   "📣 Рассылка завершена. Доставлено: <b>{ok}</b> пользователей.",
        "withdraw_processing": (
            "✅ Заявка принята!\n\n"
            "Ваша заявка обрабатывается.\n"
            "Выплаты занимают 1–7 рабочих дней."
        ),
    },
}

def tr(lang: str, key: str, **kw) -> str:
    bucket = _T.get(lang, _T["en"])
    tmpl   = bucket.get(key, _T["en"].get(key, key))
    return tmpl.format(**kw) if kw else tmpl

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
    return math.floor(usd / 0.012)

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id         INTEGER PRIMARY KEY,
                username        TEXT,
                balance         REAL    DEFAULT 0.0,
                total_earned    REAL    DEFAULT 0.0,
                photos_sold     INTEGER DEFAULT 0,
                referrals_count INTEGER DEFAULT 0,
                referred_by     INTEGER DEFAULT NULL,
                lang            TEXT    DEFAULT 'en',
                last_seen       TEXT    DEFAULT (datetime('now')),
                created_at      TEXT    DEFAULT (datetime('now'))
            )
        """)
        for col_def in [
            "referrals_count INTEGER DEFAULT 0", "referred_by INTEGER",
            "lang TEXT DEFAULT 'en'", "last_seen TEXT DEFAULT (datetime('now'))",
        ]:
            try: await db.execute(f"ALTER TABLE players ADD COLUMN {col_def}")
            except Exception: pass

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
            )
        """)
        for col_def in ["batch_id TEXT", "sale_rub REAL DEFAULT 0"]:
            try: await db.execute(f"ALTER TABLE photos ADD COLUMN {col_def}")
            except Exception: pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS quests (
                user_id    INTEGER,
                channel_id TEXT,
                completed  INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, channel_id),
                FOREIGN KEY(user_id) REFERENCES players(user_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id INTEGER,
                referred_id INTEGER PRIMARY KEY,
                created_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS support_messages (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                text         TEXT,
                direction    TEXT,
                admin_msg_id INTEGER,
                created_at   TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admin_msg_map (
                admin_msg_id INTEGER,
                admin_id     INTEGER,
                user_id      INTEGER,
                created_at   TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (admin_msg_id, admin_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                amount_usd  REAL,
                stars       INTEGER DEFAULT 0,
                method      TEXT,
                is_priority INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                added_by   INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()

async def get_or_create_player(user_id: int, username: str = "", referred_by: int | None = None) -> tuple[dict, bool]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            await db.execute("INSERT INTO players (user_id, username, referred_by) VALUES (?,?,?)", (user_id, username, referred_by))
            await db.commit()
            async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
            return dict(row), True
        if username and username != (row["username"] or ""):
            await db.execute("UPDATE players SET username=? WHERE user_id=?", (username, user_id))
            await db.commit()
            return dict(row) | {"username": username}, False
        return dict(row), False

async def get_player(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

async def touch_last_seen(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET last_seen=datetime('now') WHERE user_id=?", (user_id,))
        await db.commit()

async def get_player_photos(user_id: int, lang: str = "en") -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM photos WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (user_id,)) as cur:
            rows = await cur.fetchall()
    result = []
    for r in rows:
        p = dict(r)
        if p.get("status") == "on_auction":
            p["status_label"] = tr(lang, "status_auction")
        result.append(p)
    return result

async def get_active_photo_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'", (user_id,)) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM players WHERE referred_by=?", (user_id,)) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

async def get_quest_status(user_id: int) -> list:
    result = []
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for ch in PARTNER_CHANNELS:
            channel_id = list(ch.keys())[0]
            async with db.execute("SELECT completed FROM quests WHERE user_id=? AND channel_id=?", (user_id, channel_id)) as cur:
                row = await cur.fetchone()
            result.append({**ch, "id": channel_id, "completed": bool(row["completed"]) if row else False})
    return result

async def get_referral_list(referrer_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT r.referred_id, r.created_at, p.username,
                   CASE WHEN p.photos_sold > 0 THEN 1 ELSE 0 END AS is_active
            FROM referrals r
            LEFT JOIN players p ON p.user_id = r.referred_id
            WHERE r.referrer_id = ?
            ORDER BY r.created_at DESC
            """,
            (referrer_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]

async def get_admin_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT COUNT(*) AS cnt FROM players") as cur:
            users = (await cur.fetchone())["cnt"]
        async with db.execute("SELECT COUNT(*) AS cnt FROM photos") as cur:
            photos = (await cur.fetchone())["cnt"]
        async with db.execute("SELECT COALESCE(SUM(balance), 0) AS s FROM players") as cur:
            pending = round((await cur.fetchone())["s"], 2)
    return {"users": users, "photos": photos, "pending_withdraw_usd": pending}

async def channels_all_subscribed(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for ch in PARTNER_CHANNELS:
            channel_id = list(ch.keys())[0]
            async with db.execute("SELECT completed FROM quests WHERE user_id=? AND channel_id=?", (user_id, channel_id)) as cur:
                row = await cur.fetchone()
            if not row or not row["completed"]:
                return False
    return True

async def referral_url(user_id: int) -> str:
    global _bot_username
    if not _bot_username:
        try:
            me = await bot.get_me()
            _bot_username = me.username
            _save_cached_bot_username(_bot_username)
        except Exception:
            pass
    if _bot_username:
        return f"https://t.me/{_bot_username}?start=ref_{user_id}"
    return ""

async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM players") as cur:
            rows = await cur.fetchall()
    return [r[0] for r in rows]

async def get_admin_ids() -> set[int]:
    ids: set[int] = set()
    if ADMIN_ID: ids.add(ADMIN_ID)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM admins") as cur:
            rows = await cur.fetchall()
    ids.update(r[0] for r in rows)
    return ids

async def is_admin(user_id: int) -> bool:
    if ADMIN_ID and user_id == ADMIN_ID: return True
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) as cur:
            return bool(await cur.fetchone())

async def add_admin(user_id: int, username: str, added_by: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO admins (user_id, username, added_by) VALUES (?,?,?)", (user_id, username, added_by))
        await db.commit()

async def remove_admin(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) as cur:
            exists = await cur.fetchone()
        if exists:
            await db.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
            await db.commit()
            return True
    return False

async def list_admins() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id, username, added_by, created_at FROM admins") as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]

async def forward_support_to_admins(user_id: int, username: str, text: str) -> int | None:
    admin_ids = await get_admin_ids()
    primary_msg_id: int | None = None
    uname = username or str(user_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"💬 Reply to @{uname} (id {user_id})", callback_data=f"adm_reply:{user_id}")
    ]])
    async with aiosqlite.connect(DB_PATH) as db:
        for aid in admin_ids:
            try:
                sent = await bot.send_message(
                    aid, f"🎧 <b>Support</b>\nFrom: <code>{user_id}</code> (@{uname})\n\n{text}",
                    parse_mode=ParseMode.HTML, reply_markup=kb
                )
                await db.execute("INSERT OR REPLACE INTO admin_msg_map (admin_msg_id, admin_id, user_id) VALUES (?,?,?)", (sent.message_id, aid, user_id))
                if aid == ADMIN_ID: primary_msg_id = sent.message_id
            except Exception: pass
        await db.commit()
    return primary_msg_id

async def auction_worker():
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                now = datetime.utcnow().isoformat()
                async with db.execute("SELECT * FROM photos WHERE status='on_auction' AND sell_at<=?", (now,)) as cur:
                    due = await cur.fetchall()

                for photo in due:
                    photo      = dict(photo)
                    buyer      = random.choice(FAKE_USERS)
                    sale_rub   = photo.get("sale_rub") or random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
                    gross      = rub_to_usd(float(sale_rub))
                    net        = apply_commission(gross)
                    commission = round(gross - net, 2)

                    await db.execute(
                        "UPDATE photos SET status='sold', sold_at=datetime('now'), buyer=?, final_price=?, sale_rub=? WHERE id=?",
                        (buyer, net, sale_rub, photo["id"])
                    )
                    await db.execute(
                        "UPDATE players SET balance=balance+?, total_earned=total_earned+?, photos_sold=photos_sold+1 WHERE user_id=?",
                        (net, net, photo["user_id"])
                    )
                    await db.commit()

                    try:
                        player  = await get_player(photo["user_id"])
                        if player:
                            await bot.send_message(
                                photo["user_id"],
                                tr(player.get("lang", "en"), "sold", rub=int(sale_rub), gross=gross, commission=commission, net=net, buyer=buyer, balance=round((player["balance"] or 0)+net, 2)),
                                parse_mode=ParseMode.HTML
                            )
                    except Exception: pass
        except Exception as e: logger.error(f"auction_worker error: {e}")
        await asyncio.sleep(15)

async def reminder_worker():
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT user_id, lang FROM players WHERE last_seen < ?", (cutoff,)) as cur:
                    rows = await cur.fetchall()

            for row in rows:
                uid, lang = row["user_id"], row["lang"] or "en"
                ref = await referral_url(uid)
                try:
                    await bot.send_message(uid, tr(lang, "remind", ref_url=ref), parse_mode=ParseMode.HTML)
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("UPDATE players SET last_seen=datetime('now') WHERE user_id=?", (uid,))
                        await db.commit()
                except Exception: pass
        except Exception: pass

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

def _load_cached_bot_username() -> str | None:
    u = os.getenv("BOT_USERNAME", "").strip()
    if u: return u
    try:
        with open(_BOT_USERNAME_CACHE) as f:
            if cached := f.read().strip(): return cached
    except Exception: pass
    return None

def _save_cached_bot_username(username: str):
    try:
        with open(_BOT_USERNAME_CACHE, "w") as f: f.write(username)
    except Exception: pass

_bot_username: str | None = _load_cached_bot_username()

async def is_subscribed_to_channel(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
        return member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
    except Exception: return False

async def check_subscription(user_id: int) -> bool:
    if await is_admin(user_id): return True
    return await is_subscribed_to_channel(user_id)

def _sub_gate_keyboard(args_str: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Subscribe to Channel", url=REQUIRED_CHANNEL_URL)],
        [InlineKeyboardButton(text="✅ I've Subscribed", callback_data=f"chksub:{args_str}")]
    ])

async def _send_sub_required(target: Message, args_str: str = ""):
    await target.answer(
        f"👋 Welcome to <b>PhotoFlip</b>!\n\n📢 To use the bot you must subscribe to our channel first:\n"
        f"<a href='{REQUIRED_CHANNEL_URL}'><b>{REQUIRED_CHANNEL_NAME}</b></a>\n\nAfter subscribing tap <b>✅ I've Subscribed</b>.",
        parse_mode=ParseMode.HTML, reply_markup=_sub_gate_keyboard(args_str)
    )

async def _bind_referral(new_user_id: int, referrer_id: int, first_name: str) -> bool:
    if referrer_id == new_user_id: return False
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT 1 FROM players WHERE user_id=?", (referrer_id,)) as cur:
            if not await cur.fetchone(): return False
        async with db.execute("SELECT 1 FROM referrals WHERE referred_id=?", (new_user_id,)) as cur:
            if await cur.fetchone(): return False
        async with db.execute("SELECT referred_by FROM players WHERE user_id=?", (new_user_id,)) as cur:
            prow = await cur.fetchone()
        if prow is None or prow["referred_by"] is not None: return False

        await db.execute("UPDATE players SET referred_by=? WHERE user_id=?", (referrer_id, new_user_id))
        await db.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)", (referrer_id, new_user_id))
        await db.execute("UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?", (referrer_id,))
        await db.commit()

    try:
        new_player = await get_player(new_user_id)
        display = html.escape("@" + new_player["username"] if new_player and new_player.get("username") else first_name or str(new_user_id))
        await bot.send_message(referrer_id, REFERRAL_NOTIFY_TMPL.format(username=display, user_id=new_user_id), parse_mode=ParseMode.HTML)
    except Exception: pass
    return True

async def _process_start(target: Message, user, args: str = ""):
    player, _ = await get_or_create_player(user.id, user.username or "")
    await touch_last_seen(user.id)
    player    = await get_player(user.id)
    lang      = player.get("lang", "en")
    ref       = await referral_url(user.id)
    ref_count = await get_referral_count(user.id)
    lvl       = vip_level(ref_count)
    slots     = vip_slot_limit(ref_count)

    share_url = make_share_url(ref) if ref else None
    rows = [
        [InlineKeyboardButton(text=tr(lang, "btn_open"), web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(text=tr(lang, "btn_referrals"), callback_data="show_referrals")],
    ]
    if share_url: rows.append([InlineKeyboardButton(text=tr(lang, "btn_share"), url=share_url)])
    
    news_suffix = f"\n\n{random.choice(MARKET_NEWS_RU if lang == 'ru' else MARKET_NEWS_EN)}"
    await target.answer(
        tr(lang, "welcome", balance=player["balance"] or 0, vip=lvl, slots=slots, ref_url=ref) + news_suffix,
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )

@dp.callback_query(F.data.startswith("adm_reply:"))
async def cb_admin_reply_start(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("❌ Not authorised.", show_alert=True)
        return
    await state.set_state(AdminReply.waiting_reply)
    await state.update_data(target_uid=int(cb.data.split(":")[1]))
    await cb.answer()
    await cb.message.reply(f"✍️ <b>Replying to user <code>{cb.data.split(':')[1]}</code></b>\n\nType your message below. Send /cancel to abort.", parse_mode=ParseMode.HTML)

@dp.message(AdminReply.waiting_reply, Command("cancel"))
async def admin_reply_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Reply cancelled.")

@dp.message(AdminReply.waiting_reply)
async def admin_reply_send(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    target_uid = data.get("target_uid")
    text = message.text or message.caption or ""
    if not text:
        await message.answer("⚠️ Empty message.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO support_messages (user_id, text, direction) VALUES (?,?,'out')", (target_uid, text))
        await db.commit()

    p = await get_player(target_uid)
    try:
        await bot.send_message(target_uid, tr((p or {}).get("lang", "en"), "support_reply", text=text), parse_mode=ParseMode.HTML)
        await message.answer(f"✅ Reply delivered to <code>{target_uid}</code>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ Delivery failed: {e}")
    await state.clear()

@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user, args_str = message.from_user, (command.args or "").strip()
    referrer_id = None
    raw_arg = args_str[4:] if args_str.startswith("ref_") else args_str
    if raw_arg:
        try:
            if int(raw_arg) != user.id: referrer_id = int(raw_arg)
        except ValueError: pass

    await get_or_create_player(user.id, user.username or "")
    if referrer_id is not None:
        await _bind_referral(user.id, referrer_id, user.first_name or str(user.id))

    if not await check_subscription(user.id):
        await _send_sub_required(message, args_str)
        return
    await _process_start(message, user, args_str)

@dp.callback_query(F.data.startswith("chksub:"))
async def cb_check_sub(cb: CallbackQuery):
    if not await is_subscribed_to_channel(cb.from_user.id):
        await cb.answer("❌ You haven't subscribed yet!", show_alert=True)
        return
    user_id = cb.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        for ch in PARTNER_CHANNELS:
            await db.execute("INSERT OR REPLACE INTO quests (user_id, channel_id, completed) VALUES (?,?,1)", (user_id, list(ch.keys())[0]))
        await db.commit()
    await cb.answer("✅ Subscription confirmed!")
    try: await cb.message.delete()
    except Exception: pass
    await _process_start(cb.message, cb.from_user, cb.data[7:])

@dp.message(Command("lang"))
async def cmd_lang(message: Message):
    if not await check_subscription(message.from_user.id): return await _send_sub_required(message, "")
    player, _ = await get_or_create_player(message.from_user.id, message.from_user.username or "")
    new_lang = "en" if player.get("lang", "en") == "ru" else "ru"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET lang=? WHERE user_id=?", (new_lang, message.from_user.id))
        await db.commit()
    await message.answer(tr(new_lang, "lang_changed"), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "show_referrals")
async def cb_referrals(cb: CallbackQuery):
    if not await check_subscription(cb.from_user.id):
        return await cb.answer("❌ Subscribe to @dsdfsdfawer first!", show_alert=True)
    await cb.answer()
    player = await get_player(cb.from_user.id)
    lang, ref, count = (player or {}).get("lang", "en"), await referral_url(cb.from_user.id), await get_referral_count(cb.from_user.id)
    share_url = make_share_url(ref) if ref else None
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr(lang, "btn_share"), url=share_url)]]) if share_url else None
    await cb.message.answer(tr(lang, "referrals_msg", count=count, ref_url=ref, need=max(0, MIN_REFERRALS_WITHDRAW - count)), parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.message(F.reply_to_message)
async def admin_reply_legacy(message: Message):
    if not await is_admin(message.from_user.id): return
    admin_id, replied_msg_id = message.from_user.id, message.reply_to_message.message_id
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id FROM admin_msg_map WHERE admin_msg_id=? AND admin_id=?", (replied_msg_id, admin_id)) as cur:
            row = await cur.fetchone()
        if not row:
            async with db.execute("SELECT user_id FROM support_messages WHERE admin_msg_id=? AND direction='in'", (replied_msg_id,)) as cur:
                row = await cur.fetchone()
        if not row: return await message.reply("⚠️ No support ticket linked.", parse_mode=ParseMode.HTML)
        
        target_uid, txt = row["user_id"], message.text or message.caption or ""
        await db.execute("INSERT INTO support_messages (user_id, text, direction, admin_msg_id) VALUES (?, ?, 'out', ?)", (target_uid, txt, replied_msg_id))
        await db.commit()

    try:
        await bot.send_message(target_uid, tr((await get_player(target_uid) or {}).get("lang", "en"), "support_reply", text=txt), parse_mode=ParseMode.HTML)
        await message.reply(f"✅ Reply delivered to <code>{target_uid}</code>", parse_mode=ParseMode.HTML)
    except Exception as e: await message.reply(f"❌ Delivery failed: {e}")

@dp.message(Command("addadmin"))
async def cmd_addadmin(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    args = (command.args or "").split()
    if not args: return await message.answer("Usage: /addadmin <user_id> [username]")
    try: await add_admin(int(args[0]), args[1] if len(args) > 1 else str(int(args[0])), message.from_user.id)
    except ValueError: return await message.answer("❌ Invalid user_id.")
    await message.answer(f"✅ Admin added.", parse_mode=ParseMode.HTML)

@dp.message(Command("removeadmin"))
async def cmd_removeadmin(message: Message, command: CommandObject):
    if message.from_user.id != ADMIN_ID: return
    try:
        if await remove_admin(int((command.args or "").strip())): await message.answer("✅ Admin removed.")
        else: await message.answer("⚠️ Not an admin.")
    except ValueError: await message.answer("Usage: /removeadmin <user_id>")

@dp.message(Command("admins"))
async def cmd_admins(message: Message):
    if not await is_admin(message.from_user.id): return
    admins = await list_admins()
    lines = [f"👑 <b>Super-admin:</b> <code>{ADMIN_ID}</code>"] + (["\n<b>DB admins:</b>"] + [f"• <code>{a['user_id']}</code> @{a['username']}" for a in admins] if admins else ["\nNo extra admins."])
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args: return await message.answer("Usage: /broadcast [message]")
    user_ids, delivered = await get_all_user_ids(), 0
    await message.answer(f"📣 Starting broadcast...")
    for uid in user_ids:
        try:
            await bot.send_message(uid, command.args, parse_mode=ParseMode.HTML)
            delivered += 1
        except Exception: pass
        await asyncio.sleep(0.05)
    await message.answer(tr((await get_player(message.from_user.id) or {}).get("lang", "en"), "broadcast_done", ok=delivered), parse_mode=ParseMode.HTML)

@dp.message(~F.text.startswith("/"))
async def generic_message_gate(message: Message):
    if not message.reply_to_message and not await check_subscription(message.from_user.id):
        await _send_sub_required(message, "")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    try: await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True, request_timeout=30, allowed_updates=["message", "callback_query", "chat_member"])
    except Exception: pass
    try:
        global _bot_username
        _bot_username = (await bot.get_me()).username
        _save_cached_bot_username(_bot_username)
    except Exception: pass
    t1, t2 = asyncio.create_task(auction_worker()), asyncio.create_task(reminder_worker())
    yield
    t1.cancel(); t2.cancel()
    try: await bot.delete_webhook()
    except Exception: pass

app = FastAPI(title="PhotoFlip API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")

@app.get("/")
async def root():
    return FileResponse("index.html") if Path("index.html").exists() else PlainTextResponse("Upload index.html")

@app.get("/api/feed")
async def api_feed():
    events = []
    for user in random.sample(FAKE_USERS, min(10, len(FAKE_USERS))):
        amount = round(random.uniform(2.0, 48.0), 2)
        lang_ev, tmpl = random.choice(FEED_ACTIONS)
        events.append({"user": user, "action": tmpl.format(seller=random.choice([n for n in FAKE_USERS if n != user]), amount=amount), "amount": amount, "lang": lang_ev, "timestamp": (datetime.utcnow() - timedelta(seconds=random.randint(0, 600))).isoformat()})
    events.sort(key=lambda e: e["timestamp"], reverse=True)
    return {"events": events}

@app.get("/api/player/{user_id}")
async def api_get_player(user_id: int, username: str = ""):
    player, _ = await get_or_create_player(user_id, username)
    lang = player.get("lang", "en")
    
    if not await is_admin(user_id):
        try:
            live_ok = await is_subscribed_to_channel(user_id)
            async with aiosqlite.connect(DB_PATH) as db:
                for ch in PARTNER_CHANNELS:
                    await db.execute("INSERT OR REPLACE INTO quests (user_id, channel_id, completed) VALUES (?,?,?)", (user_id, list(ch.keys())[0], 1 if live_ok else 0))
                await db.commit()
            subscribed = live_ok
        except Exception:
            subscribed = await channels_all_subscribed(user_id)
        
        if not subscribed:
            return JSONResponse(status_code=402, content={"error": "subscription_required", "channels": [{"id": list(ch.keys())[0], "url": ch.get("url", ""), "name": ch.get("name", "")} for ch in PARTNER_CHANNELS], "message": tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en"), "required_channel": REQUIRED_CHANNEL_URL})

    ref_count = await get_referral_count(user_id)
    player["referrals_count"] = ref_count
    await touch_last_seen(user_id)

    return {
        "player": player,
        "photos": await get_player_photos(user_id, lang),
        "quests": await get_quest_status(user_id),
        "withdraw_unlocked": ref_count >= MIN_REFERRALS_WITHDRAW,
        "vip_level": vip_level(ref_count),
        "vip_tiers": [{"min": t[0], "max_delay": t[1], "slots": t[2]} for t in VIP_TIERS],
        "referral_url": await referral_url(user_id),
        "rub_rate": RUB_TO_USD_RATE,
        "active_slots": await get_active_photo_count(user_id),
        "slot_limit": vip_slot_limit(ref_count),
        "min_referrals_withdraw": MIN_REFERRALS_WITHDRAW,
        "withdraw_condition": tr(lang, "withdraw_locked"),
        "vip_priority_note": tr(lang, "vip_priority"),
    }

@app.put("/api/player/{user_id}/lang")
async def api_set_lang(user_id: int, request: Request):
    lang = (await request.json()).get("lang", "en")
    if lang not in _T: lang = "en"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET lang=? WHERE user_id=?", (lang, user_id))
        await db.commit()
    return {"lang": lang}

@app.get("/api/referrals/{user_id}")
async def api_referrals(user_id: int):
    if not await get_player(user_id): raise HTTPException(404, "Player not found")
    return {"referrals": await get_referral_list(user_id), "referrals_count": await get_referral_count(user_id), "referral_url": await referral_url(user_id)}

@app.post("/api/upload")
async def api_upload(user_id: int = Form(...), username: str = Form(""), files: List[UploadFile] = File(...)):
    player, _ = await get_or_create_player(user_id, username)
    if (player["balance"] or 0) > 0: raise HTTPException(403, "Withdraw balance first.")
    
    # ── РАЗРЕШАЕМ ДО 30 ФАЙЛОВ ДЛЯ ВИПОВ ──
    if not (1 <= len(files) <= 30): raise HTTPException(400, "1 to 30 photos allowed.")

    files_data = []
    for f in files:
        raw = await f.read()
        if len(raw) > MAX_FILE_SIZE: raise HTTPException(400, "File > 10MB.")
        files_data.append((f.filename or "photo.jpg", raw))

    ref_count, active = await get_referral_count(user_id), await get_active_photo_count(user_id)
    slot_limit = vip_slot_limit(ref_count)
    if active + len(files_data) > slot_limit: raise HTTPException(403, "Slot limit reached.")

    # Логика "пакета" — если загружено 5 или более файлов за раз
    is_pack = len(files_data) >= PACK_SIZE
    rub_each = []
    if is_pack:
        for _ in range(len(files_data)):
            rub_each.append(random.randint(PACK_MIN_RUB, PACK_MAX_RUB) // PACK_SIZE)
    else:
        for _ in range(len(files_data)):
            rub_each.append(random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB))

    batch_id, max_delay, results = uuid.uuid4().hex, vip_max_delay(ref_count), []

    async with aiosqlite.connect(DB_PATH) as db:
        for idx, (orig_name, raw) in enumerate(files_data):
            filename = f"{uuid.uuid4().hex}{Path(orig_name).suffix.lower() or '.jpg'}"
            (UPLOADS_DIR / filename).write_bytes(raw)
            sell_at = (datetime.utcnow() + timedelta(seconds=random.randint(MIN_DELAY_SECS, max(max_delay, MIN_DELAY_SECS + 1)))).isoformat()
            sale_rub, prev_usd, pid = rub_each[idx], apply_commission(rub_to_usd(rub_each[idx])), uuid.uuid4().hex
            await db.execute("INSERT INTO photos (id, user_id, filename, batch_id, base_price, final_price, sale_rub, status, sell_at) VALUES (?,?,?,?,?,?,?,'on_auction',?)", (pid, user_id, filename, batch_id, prev_usd, prev_usd, sale_rub, sell_at))
            results.append({"photo_id": pid, "filename": filename, "base_price": prev_usd, "preview_rub": sale_rub, "status": "on_auction", "vip_level": vip_level(ref_count)})
        await db.commit()
    return {"batch_id": batch_id, "is_pack": is_pack, "photos": results, "total_rub": sum(rub_each), "slot_limit": slot_limit, "active_after": active + len(files_data)}

@app.post("/api/quest/complete")
async def api_quest_complete(request: Request):
    data = await request.json()
    user_id, channel_id = data.get("user_id"), data.get("channel_id")
    if not user_id or not channel_id: raise HTTPException(400, "Missing data")
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        if member.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR): raise HTTPException(403, "Not joined")
    except Exception: pass

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO quests (user_id, channel_id, completed) VALUES (?,?,1)", (user_id, channel_id))
        await db.commit()
    return {"quests": await get_quest_status(user_id), "withdraw_unlocked": await get_referral_count(user_id) >= MIN_REFERRALS_WITHDRAW}

def _build_sub_required_response(lang: str) -> JSONResponse:
    msg = tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en")
    return JSONResponse(status_code=402, content={"error": "subscription_required", "channels": [{"id": list(ch.keys())[0], "url": ch.get("url", ""), "name": ch.get("name", "")} for ch in PARTNER_CHANNELS], "message": msg})

@app.post("/api/withdraw")
async def api_withdraw(request: Request):
    user_id = (await request.json()).get("user_id")
    player = await get_player(user_id)
    if not player: raise HTTPException(404, "Player not found")
    lang, ref_count = player.get("lang", "en"), await get_referral_count(user_id)

    if ref_count < MIN_REFERRALS_WITHDRAW: raise HTTPException(403, tr(lang, "withdraw_locked"))
    if not await channels_all_subscribed(user_id): return _build_sub_required_response(lang)
    if (player["balance"] or 0) <= 0: raise HTTPException(400, "Nothing to withdraw.")

    amount, is_priority = round(player["balance"], 2), 1 if vip_level(ref_count) >= 1 else 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET balance=0 WHERE user_id=?", (user_id,))
        await db.execute("INSERT INTO withdrawal_requests (user_id, amount_usd, method, is_priority) VALUES (?,?,'usd',?)", (user_id, amount, is_priority))
        await db.commit()

    for aid in await get_admin_ids():
        try: await bot.send_message(aid, f"💳 <b>USD Withdrawal</b>\n{'⭐ VIP PRIORITY' if is_priority else ''}\nUser: <code>{user_id}</code>\nAmount: <b>${amount:.2f}</b>", parse_mode=ParseMode.HTML)
        except Exception: pass
    return {"success": True, "withdrawn_usd": amount, "new_balance": 0.0, "message": tr(lang, "withdraw_processing")}

@app.post("/api/withdraw/stars")
async def api_withdraw_stars(request: Request):
    user_id = (await request.json()).get("user_id")
    player = await get_player(user_id)
    if not player: raise HTTPException(404, "Player not found")
    lang, ref_count = player.get("lang", "en"), await get_referral_count(user_id)

    if ref_count < MIN_REFERRALS_WITHDRAW: raise HTTPException(403, tr(lang, "withdraw_locked"))
    if not await channels_all_subscribed(user_id): return _build_sub_required_response(lang)
    if (player["balance"] or 0) <= 0: raise HTTPException(400, "Nothing to withdraw.")

    usd_amount, stars_amount, is_priority = round(player["balance"], 2), usd_to_stars(round(player["balance"], 2)), 1 if vip_level(ref_count) >= 1 else 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET balance=0 WHERE user_id=?", (user_id,))
        await db.execute("INSERT INTO withdrawal_requests (user_id, amount_usd, stars, method, is_priority) VALUES (?,?,?,'stars',?)", (user_id, usd_amount, stars_amount, is_priority))
        await db.commit()

    for aid in await get_admin_ids():
        try: await bot.send_message(aid, f"⭐ <b>Stars Withdrawal</b>\n{'⭐ VIP PRIORITY' if is_priority else ''}\nUser: <code>{user_id}</code>\n${usd_amount:.2f} → <b>{stars_amount} ⭐</b>", parse_mode=ParseMode.HTML)
        except Exception: pass
    return {"success": True, "withdrawn_usd": usd_amount, "stars": stars_amount, "new_balance": 0.0, "message": tr(lang, "withdraw_processing")}

@app.post("/api/support/send")
async def api_support_send(request: Request):
    data = await request.json()
    user_id, text = data.get("user_id"), (data.get("text") or "").strip()
    if not user_id or not text: raise HTTPException(400, "Missing data")
    player = await get_player(user_id)
    if not player: raise HTTPException(404, "Player not found")
    admin_msg_id = await forward_support_to_admins(user_id, player.get("username") or str(user_id), text)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO support_messages (user_id, text, direction, admin_msg_id) VALUES (?,?,'in',?)", (user_id, text, admin_msg_id))
        await db.commit()
    return {"success": True}

@app.get("/api/support/messages/{user_id}")
async def api_support_messages(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, text, direction, created_at FROM support_messages WHERE user_id=? ORDER BY created_at ASC LIMIT 100", (user_id,)) as cur:
            rows = await cur.fetchall()
    return {"messages": [dict(r) for r in rows]}

def _require_admin_token(request: Request):
    if request.headers.get("X-Admin-Token", "") != str(ADMIN_ID): raise HTTPException(403, "Forbidden")

@app.get("/api/admin/stats")
async def api_admin_stats(request: Request):
    _require_admin_token(request)
    return await get_admin_stats()

@app.get("/api/admin/withdrawals")
async def api_admin_withdrawals(request: Request):
    _require_admin_token(request)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT wr.*, p.username FROM withdrawal_requests wr LEFT JOIN players p ON p.user_id = wr.user_id ORDER BY wr.is_priority DESC, wr.created_at ASC") as cur:
            rows = await cur.fetchall()
    return {"withdrawals": [dict(r) for r in rows]}

@app.post("/api/admin/support/reply")
async def api_admin_support_reply(request: Request):
    _require_admin_token(request)
    data = await request.json()
    player = await get_player(data.get("user_id"))
    if not player: raise HTTPException(404, "Player not found")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO support_messages (user_id, text, direction, admin_msg_id) VALUES (?,?,'out',?)", (data.get("user_id"), data.get("text", "").strip(), data.get("admin_msg_id")))
        await db.commit()
    try: await bot.send_message(data.get("user_id"), tr(player.get("lang", "en"), "support_reply", text=data.get("text", "").strip()), parse_mode=ParseMode.HTML)
    except Exception: pass
    return {"success": True}

@app.post("/api/referral/bind")
async def api_referral_bind(request: Request):
    data = await request.json()
    new_user_id, ref_param = data.get("user_id"), str(data.get("ref_param") or "").strip()
    if not new_user_id: raise HTTPException(400, "Missing user_id")
    referrer_id = int(ref_param[4:]) if ref_param.startswith("ref_") else int(ref_param) if ref_param.isdigit() else None
    if not referrer_id or referrer_id == new_user_id: return {"bound": False, "reason": "invalid_ref"}
    await get_or_create_player(new_user_id, str(data.get("username") or ""))
    return {"bound": await _bind_referral(new_user_id, referrer_id, str(data.get("first_name") or str(new_user_id)))}

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    from aiogram.types import Update
    await dp.feed_update(bot, Update(**await request.json()))
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)