"""
PhotoFlip — Telegram Mini App Backend  v8.0
FastAPI + Aiogram 3 + aiosqlite  |  Railway edition

CHANGES in v8:
 - PORT default fixed to 8000
 - MAX_FILE_SIZE 10MB hard limit in /api/upload (reads bytes, checks size)
 - set_webhook: request_timeout=30 + allowed_updates
 - REFERRAL ANTI-ABUSE: "clean user" = referred_by IS NULL + photos count = 0
 - /api/player WebApp entry checks mandatory subscription
 - All SQL queries complete and verified
 - All aiosqlite sessions closed properly
 - Admin Reply: looks up user_id by admin_msg_id in support_messages
 - admin_msg_id stored on forward, retrieved on reply
 - Full EN/RU dual language
 - USD only, no coins
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
from aiogram.filters import Command, CommandObject, CommandStart
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
#  CONFIG
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN = os.getenv("BOT_TOKEN", "8700481112:AAGwUZffQtN0r9KsEq_dZk3liQeLg_9L3Xw")
ADMIN_ID  = int(os.getenv("ADMIN_ID", ""))
PORT      = int(os.getenv("PORT", "8000"))

WEBHOOK_PATH = "/webhook"
WEBAPP_URL   = os.getenv("WEBAPP_URL", "https://photo-production-d5b8.up.railway.app")
WEBHOOK_URL  = f"{WEBAPP_URL}{WEBHOOK_PATH}"

DB_PATH     = "photoflip.db"
UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(exist_ok=True)

# ── File upload limits ────────────────────────────────────────
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB hard limit per file

# ── Economy ──────────────────────────────────────────────────
RUB_TO_USD_RATE        = 92.0
COMMISSION_PCT         = 0.02
SINGLE_MIN_RUB         = 200
SINGLE_MAX_RUB         = 600
PACK_MIN_RUB           = 1_000
PACK_MAX_RUB           = 3_000
PACK_SIZE              = 5
MIN_REFERRALS_WITHDRAW = 3
BONUS_MIN_USD          = 0.05
BONUS_MAX_USD          = 0.50
BONUS_COOLDOWN_HOURS   = 24

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

# ── 60+ NPC names for live feed ──────────────────────────────
FAKE_USERS = [
    "u***r7", "a***2", "m***k9", "p***y4", "t***3", "j***8", "k***5", "s***1",
    "x***z2", "q***9", "r***m3", "b***6", "c***w5", "n***4", "f***h1", "d***7",
    "w***l8", "e***v3", "g***o6", "h***i2", "y***t5", "o***p1", "z***k4", "v***s9",
    "i***b3", "l***n7", "R***a8", "M***e2", "A***i6", "T***o4",
    "PhotoNinja_7", "SniperLens_3", "PixelHunter_2", "SnapMaster_5",
    "ArtClipper_9", "LensPro_4", "FrameKing_1", "ShotWizard_6",
    "GoldenHour_8", "NightShooter_3", "UrbanLens_5", "NatureSnap_7",
    "MacroKing_2", "ArtFlip_6", "StreetPhoto_4", "GalleryMod_9",
    "AuctionAce_1", "BidMaster_3", "PhotoTrader_7", "ClickBoss_5",
    "VintageSnap_8", "ColorPop_2", "DarkroomPro_6", "HighRes_4",
    "LensFlare_9", "ShutterBug_1", "ExposureX_3", "FocalPoint_7",
    "RAWmaster_5", "DepthChaser_2",
    "SkilledTrader", "CryptoKing", "MasterPhoto", "Dimon777", "Alena_V",
    "PhotoPro99", "NightOwl42", "LuckyStar88", "TraderMax", "Sergey_K",
    "Anna_Photo", "DenisFlip", "KatyaBest", "VolodiaT", "RuslanPro",
    "Misha_88", "Tanya2024", "IgorAuction", "SvetaFlip", "AlexPhoto",
    "NatalyV", "PavelX", "OlegMaster", "VikaBoss", "ArtemTrade",
    "DashaPro", "KirilPhoto", "ZoyaFlip", "TimurK", "LenaAuction",
    "FedorPix", "GalyaX", "BorisTrade", "ZinaPhoto", "YuraBest",
    "MilaFlip", "KostikPro", "NikaAuction", "Andrey777", "photo_ninja",
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
    "🌍 International collectors actively buying nature shots.",
    "⚡ Hot trend: sunset photos — over 200 deals per hour.",
    "📊 Architectural photography market growing steadily for 2nd month.",
    "🎨 Abstract shots entered the top-10 best-selling categories.",
    "🏙 Nighttime cityscapes are this week's buyer favorite.",
    "🤖 AI can't replace live photography — prices up +22%.",
    "🌊 Ocean photos in huge demand from European collectors.",
]

MARKET_NEWS_RU = [
    "📈 Спрос на пейзажные снимки вырос на 34% за последние сутки!",
    "🔥 Портретная фотография бьёт рекорды — средняя цена +18%.",
    "💡 Аналитики: уличные фото набирают популярность среди покупателей.",
    "🌍 Международные коллекционеры активно скупают природные снимки.",
    "⚡ Горячий тренд: фото заката — более 200 сделок за час.",
    "📊 Рынок архитектурной фотографии стабильно растёт второй месяц.",
    "🎨 Абстрактные снимки вошли в топ-10 продаваемых категорий.",
    "🏙 Городские пейзажи ночью — хит недели среди покупателей.",
    "🤖 Нейросети не могут заменить живую фотографию — рост цен на +22%.",
    "🌊 Морские снимки пользуются огромным спросом у европейских коллекционеров.",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  LOCALIZATION  (default: English)
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
        "btn_open":       "📸 Open PhotoFlip",
        "btn_referrals":  "🤝 Referrals",
        "btn_bonus":      "💰 Get Daily Bonus",
        "referrals_msg": (
            "🤝 <b>Your referrals: {count}</b>\n\n"
            "Share your link — each friend unlocks faster sales "
            "and higher VIP level.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "new_referral": (
            "👥 <b>New referral!</b> {name} joined via your link.\n"
            "Referrals: <b>{count}</b> · VIP Level: <b>{lvl}</b> · Slots: <b>{slots}</b>\n"
            "💵 Bonus: <b>+$0.50</b> credited to your balance!"
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
        "sub_required_en": "Almost there! Subscribe to our channel to access PhotoFlip.",
        "sub_required_ru": "Почти готово! Подпишитесь на наш канал для доступа к PhotoFlip.",
        "lang_changed":    "🌐 Language switched to <b>English</b>.",
        "bonus_received":  "🎁 Daily bonus: <b>+${amount:.2f}</b>!\n\nBalance: <b>${balance:.2f}</b>",
        "bonus_cooldown":  "⏳ Come back in <b>{hours}h {mins}m</b> for your next bonus.",
        "broadcast_done":  "📣 Broadcast complete. Delivered: <b>{ok}</b> users.",
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
        "btn_open":       "📸 Открыть PhotoFlip",
        "btn_referrals":  "🤝 Рефералы",
        "btn_bonus":      "💰 Получить бонус",
        "referrals_msg": (
            "🤝 <b>Ваши рефералы: {count}</b>\n\n"
            "Делитесь ссылкой — каждый друг ускоряет продажи "
            "и повышает VIP-уровень.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "new_referral": (
            "👥 <b>Новый реферал!</b> {name} зарегистрировался по вашей ссылке.\n"
            "Рефералов: <b>{count}</b> · VIP Уровень: <b>{lvl}</b> · Слотов: <b>{slots}</b>\n"
            "💵 Бонус: <b>+$0.50</b> начислен на ваш баланс!"
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
        "sub_required_en": "Almost there! Subscribe to our channel to access PhotoFlip.",
        "sub_required_ru": "Почти готово! Подпишитесь на наш канал для доступа к PhotoFlip.",
        "lang_changed":    "🌐 Язык переключён на <b>Русский</b>.",
        "bonus_received":  "🎁 Ежедневный бонус: <b>+${amount:.2f}</b>!\n\nБаланс: <b>${balance:.2f}</b>",
        "bonus_cooldown":  "⏳ Следующий бонус через <b>{hours}ч {mins}м</b>.",
        "broadcast_done":  "📣 Рассылка завершена. Доставлено: <b>{ok}</b> пользователей.",
    },
}


def tr(lang: str, key: str, **kw) -> str:
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
    return math.floor(usd / 0.012)


# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # ── players ──────────────────────────────────────────────
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
                last_bonus      TEXT    DEFAULT NULL,
                created_at      TEXT    DEFAULT (datetime('now'))
            )
        """)

        # Safe migrations for older DB instances
        migration_cols = [
            "referrals_count INTEGER DEFAULT 0",
            "referred_by INTEGER",
            "lang TEXT DEFAULT 'en'",
            "last_seen TEXT DEFAULT (datetime('now'))",
            "last_bonus TEXT DEFAULT NULL",
        ]
        for col_def in migration_cols:
            try:
                await db.execute(f"ALTER TABLE players ADD COLUMN {col_def}")
            except Exception:
                pass  # column already exists

        # ── photos ───────────────────────────────────────────────
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
            try:
                await db.execute(f"ALTER TABLE photos ADD COLUMN {col_def}")
            except Exception:
                pass

        # ── quests ───────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS quests (
                user_id    INTEGER,
                channel_id TEXT,
                completed  INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, channel_id),
                FOREIGN KEY(user_id) REFERENCES players(user_id)
            )
        """)

        # ── referrals ────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id INTEGER,
                referred_id INTEGER PRIMARY KEY,
                created_at  TEXT DEFAULT (datetime('now'))
            )
        """)

        # ── support_messages ─────────────────────────────────────
        # admin_msg_id: ID of the message sent to admin's chat (used for reply routing)
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

        # ── withdrawal_requests ──────────────────────────────────
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

        await db.commit()


# ── CRUD helpers ─────────────────────────────────────────────

async def get_or_create_player(
    user_id: int,
    username: str = "",
    referred_by: int | None = None,
) -> tuple[dict, bool]:
    """Returns (player_dict, is_new).
    If referred_by is given and user is brand new, it is stored in the players row.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM players WHERE user_id=?", (user_id,)
        ) as cur:
            row = await cur.fetchone()

        if row is None:
            await db.execute(
                "INSERT INTO players (user_id, username, referred_by) VALUES (?,?,?)",
                (user_id, username, referred_by),
            )
            await db.commit()
            async with db.execute(
                "SELECT * FROM players WHERE user_id=?", (user_id,)
            ) as cur:
                row = await cur.fetchone()
            return dict(row), True

        return dict(row), False


async def get_player(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM players WHERE user_id=?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None


async def touch_last_seen(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE players SET last_seen=datetime('now') WHERE user_id=?", (user_id,)
        )
        await db.commit()


async def get_player_photos(user_id: int, lang: str = "en") -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM photos WHERE user_id=? ORDER BY created_at DESC LIMIT 50",
            (user_id,),
        ) as cur:
            rows = await cur.fetchall()
    result = []
    for r in rows:
        p = dict(r)
        # sell_at intentionally kept — frontend countdown timer needs it
        if p.get("status") == "on_auction":
            p["status_label"] = tr(lang, "status_auction")
        result.append(p)
    return result


async def get_active_photo_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'",
            (user_id,),
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0


async def get_total_photo_count(user_id: int) -> int:
    """Total photos ever uploaded — used in referral anti-abuse check."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM photos WHERE user_id=?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0


async def get_quest_status(user_id: int) -> list:
    result = []
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for ch in PARTNER_CHANNELS:
            channel_id = list(ch.keys())[0]
            async with db.execute(
                "SELECT completed FROM quests WHERE user_id=? AND channel_id=?",
                (user_id, channel_id),
            ) as cur:
                row = await cur.fetchone()
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
        async with db.execute(
            "SELECT COALESCE(SUM(balance), 0) AS s FROM players"
        ) as cur:
            pending = round((await cur.fetchone())["s"], 2)
    return {"users": users, "photos": photos, "pending_withdraw_usd": pending}


async def channels_all_subscribed(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for ch in PARTNER_CHANNELS:
            channel_id = list(ch.keys())[0]
            async with db.execute(
                "SELECT completed FROM quests WHERE user_id=? AND channel_id=?",
                (user_id, channel_id),
            ) as cur:
                row = await cur.fetchone()
            if not row or not row["completed"]:
                return False
    return True


async def referral_url(user_id: int) -> str:
    global _bot_username
    try:
        if not _bot_username:
            me = await bot.get_me()
            _bot_username = me.username
        return f"https://t.me/{_bot_username}?start=ref_{user_id}"
    except Exception:
        return ""


async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM players") as cur:
            rows = await cur.fetchall()
    return [r[0] for r in rows]


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
                ) as cur:
                    due = await cur.fetchall()

                for photo in due:
                    photo      = dict(photo)
                    buyer      = random.choice(FAKE_USERS)
                    sale_rub   = photo.get("sale_rub") or random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
                    gross      = rub_to_usd(float(sale_rub))
                    net        = apply_commission(gross)
                    commission = round(gross - net, 2)

                    await db.execute(
                        """
                        UPDATE photos
                        SET status='sold', sold_at=datetime('now'), buyer=?, final_price=?, sale_rub=?
                        WHERE id=?
                        """,
                        (buyer, net, sale_rub, photo["id"]),
                    )
                    await db.execute(
                        """
                        UPDATE players
                        SET balance=balance+?, total_earned=total_earned+?, photos_sold=photos_sold+1
                        WHERE user_id=?
                        """,
                        (net, net, photo["user_id"]),
                    )
                    await db.commit()

                    try:
                        player  = await get_player(photo["user_id"])
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
            logger.error(f"auction_worker error: {e}")
        await asyncio.sleep(15)


async def reminder_worker():
    """Ping inactive users once per hour."""
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT user_id, lang FROM players WHERE last_seen < ?", (cutoff,)
                ) as cur:
                    rows = await cur.fetchall()

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
                            "UPDATE players SET last_seen=datetime('now') WHERE user_id=?",
                            (uid,)
                        )
                        await db.commit()
                except Exception as e:
                    logger.debug(f"Reminder failed {uid}: {e}")
        except Exception as e:
            logger.error(f"reminder_worker error: {e}")


# ═══════════════════════════════════════════════════════════════
#  AIOGRAM BOT HANDLERS
# ═══════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

# Cached bot username — populated at startup, avoids get_me() on every referral_url() call
_bot_username: str | None = None


# ── Subscription gate helper ──────────────────────────────────

async def is_subscribed_to_channel(user_id: int) -> bool:
    """Return True if user is a member of the required channel."""
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
        return member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        )
    except Exception as e:
        logger.debug(f"Subscription check failed for {user_id}: {e}")
        return False


def _sub_gate_keyboard(args_str: str) -> InlineKeyboardMarkup:
    """Keyboard with Subscribe + Check buttons."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📢 Subscribe to Channel",
            url=REQUIRED_CHANNEL_URL,
        )],
        [InlineKeyboardButton(
            text="✅ I've Subscribed",
            callback_data=f"chksub:{args_str}",
        )],
    ])


async def _send_sub_required(target: Message, args_str: str = ""):
    await target.answer(
        "👋 Welcome to <b>PhotoFlip</b>!\n\n"
        "📢 To use the bot you must subscribe to our channel first:\n"
        f"<a href='{REQUIRED_CHANNEL_URL}'><b>{REQUIRED_CHANNEL_NAME}</b></a>\n\n"
        "After subscribing tap <b>✅ I've Subscribed</b> to continue.",
        parse_mode=ParseMode.HTML,
        reply_markup=_sub_gate_keyboard(args_str),
    )


# ── Referral anti-abuse logic ─────────────────────────────────

async def _process_referral(user_id: int, user_first_name: str, args: str):
    """
    Referral binding with anti-abuse protection.

    A referral ($0.50 bonus) is credited ONLY when:
      1. args starts with 'ref_<referrer_id>'
      2. referrer_id != user_id (no self-referral)
      3. referrer exists in DB
      4. user is NOT already in the referrals table
      5. user's referred_by IS NULL  (no existing referrer)
      6. user has ZERO total photos in the photos table
         (has not yet participated in the market)
    """
    if not args or not args.startswith("ref_"):
        return
    try:
        referrer_id = int(args[4:])
    except ValueError:
        logger.debug(f"Could not parse referrer from args='{args}'")
        return

    if referrer_id == user_id:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # 1. Check referrer exists
        async with db.execute(
            "SELECT 1 FROM players WHERE user_id=?", (referrer_id,)
        ) as cur:
            if not await cur.fetchone():
                logger.debug(f"Referrer {referrer_id} not found — skipping")
                return

        # 2. Already counted?
        async with db.execute(
            "SELECT 1 FROM referrals WHERE referred_id=?", (user_id,)
        ) as cur:
            if await cur.fetchone():
                logger.debug(f"Referral already counted for {user_id} — skipping")
                return

        # 3. Fetch current user state
        async with db.execute(
            "SELECT referred_by FROM players WHERE user_id=?", (user_id,)
        ) as cur:
            row = await cur.fetchone()

        if row is None:
            # User not in DB yet — will be handled by get_or_create_player
            return

        current_referred_by = row["referred_by"]

        # 4. User already has a referrer
        if current_referred_by is not None:
            logger.debug(f"User {user_id} already has referred_by={current_referred_by}")
            return

        # 5. Anti-abuse: check if user already participated in the market
        async with db.execute(
            "SELECT COUNT(*) AS cnt FROM photos WHERE user_id=?", (user_id,)
        ) as cur:
            cnt_row = await cur.fetchone()
        photo_count = cnt_row["cnt"] if cnt_row else 0

        if photo_count > 0:
            logger.debug(
                f"User {user_id} already has {photo_count} photo(s) — referral blocked (anti-abuse)"
            )
            return

        # All checks passed — bind the referral
        REFERRAL_BONUS_USD = 0.50

        await db.execute(
            "UPDATE players SET referred_by=? WHERE user_id=?",
            (referrer_id, user_id),
        )
        await db.execute(
            "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)",
            (referrer_id, user_id),
        )
        await db.execute(
            """
            UPDATE players
            SET referrals_count=referrals_count+1,
                balance=balance+?,
                total_earned=total_earned+?
            WHERE user_id=?
            """,
            (REFERRAL_BONUS_USD, REFERRAL_BONUS_USD, referrer_id),
        )
        await db.commit()
        logger.info(f"Referral BOUND + $0.50 credited: user {user_id} → referrer {referrer_id}")

    # Notify the referrer
    try:
        rp = await get_player(referrer_id)
        if rp:
            rc    = rp.get("referrals_count", 0) + 1
            rl    = vip_level(rc)
            rs    = vip_slot_limit(rc)
            rlang = rp.get("lang", "en")
            await bot.send_message(
                referrer_id,
                tr(rlang, "new_referral",
                   name=user_first_name, count=rc, lvl=rl, slots=rs),
                parse_mode=ParseMode.HTML,
            )
    except Exception as e:
        logger.debug(f"Referral notify failed: {e}")


async def _process_start(target: Message, user, args: str = ""):
    """Send the main welcome menu. Called AFTER subscription is confirmed."""

    # For new users — pass referred_by at creation time
    referred_by_on_create: int | None = None
    if args and args.startswith("ref_"):
        try:
            referred_by_on_create = int(args[4:])
            if referred_by_on_create == user.id:
                referred_by_on_create = None
        except ValueError:
            pass

    player, is_new = await get_or_create_player(
        user.id, user.username or "",
        referred_by=referred_by_on_create,
    )
    await touch_last_seen(user.id)

    # For new users who were created with a referred_by — seed the referrals table
    if is_new and referred_by_on_create is not None:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM referrals WHERE referred_id=?", (user.id,)
            ) as cur:
                already = await cur.fetchone()
            if not already:
                async with db.execute(
                    "SELECT 1 FROM players WHERE user_id=?", (referred_by_on_create,)
                ) as cur:
                    referrer_exists = await cur.fetchone()
                if referrer_exists:
                    REFERRAL_BONUS_USD = 0.50
                    await db.execute(
                        "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)",
                        (referred_by_on_create, user.id),
                    )
                    await db.execute(
                        """
                        UPDATE players
                        SET referrals_count=referrals_count+1,
                            balance=balance+?,
                            total_earned=total_earned+?
                        WHERE user_id=?
                        """,
                        (REFERRAL_BONUS_USD, REFERRAL_BONUS_USD, referred_by_on_create),
                    )
                    await db.commit()
                    logger.info(
                        f"Referral CREATED (new user): {user.id} → {referred_by_on_create}"
                    )
                    try:
                        rp = await get_player(referred_by_on_create)
                        if rp:
                            rc    = rp.get("referrals_count", 0) + 1
                            rl    = vip_level(rc)
                            rs    = vip_slot_limit(rc)
                            rlang = rp.get("lang", "en")
                            await bot.send_message(
                                referred_by_on_create,
                                tr(rlang, "new_referral",
                                   name=user.first_name, count=rc, lvl=rl, slots=rs),
                                parse_mode=ParseMode.HTML,
                            )
                    except Exception as e:
                        logger.debug(f"New-user referral notify failed: {e}")
    else:
        # Existing user — run full anti-abuse check
        await _process_referral(user.id, user.first_name or str(user.id), args)

    player = await get_player(user.id)
    lang   = player.get("lang", "en")
    ref    = await referral_url(user.id)
    lvl    = vip_level(player.get("referrals_count", 0))
    slots  = vip_slot_limit(player.get("referrals_count", 0))

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=tr(lang, "btn_open"),
            web_app=WebAppInfo(url=WEBAPP_URL),
        )],
        [
            InlineKeyboardButton(text=tr(lang, "btn_referrals"), callback_data="show_referrals"),
            InlineKeyboardButton(text=tr(lang, "btn_bonus"),     callback_data="daily_bonus"),
        ],
    ])

    news_suffix = ""
    if not is_new:
        news_list   = MARKET_NEWS_RU if lang == "ru" else MARKET_NEWS_EN
        news_suffix = f"\n\n{random.choice(news_list)}"

    await target.answer(
        tr(lang, "welcome",
           balance=player["balance"] or 0, vip=lvl, slots=slots, ref_url=ref) + news_suffix,
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


# ── /start ───────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user     = message.from_user
    args_str = command.args or ""
    logger.info(f"/start from {user.id}, args='{args_str}'")

    subscribed = await is_subscribed_to_channel(user.id)
    if not subscribed:
        await _send_sub_required(message, args_str)
        return

    await _process_start(message, user, args_str)


@dp.callback_query(F.data.startswith("chksub:"))
async def cb_check_sub(cb: CallbackQuery):
    args       = cb.data[7:]  # remove "chksub:"
    subscribed = await is_subscribed_to_channel(cb.from_user.id)
    if not subscribed:
        await cb.answer(
            "❌ You haven't subscribed yet! Join the channel first.", show_alert=True
        )
        return
    await cb.answer("✅ Subscription confirmed!")
    try:
        await cb.message.delete()
    except Exception:
        pass
    await _process_start(cb.message, cb.from_user, args)


# ── Block non-command messages for unsubscribed users ────────

@dp.message(~F.reply_to_message & ~F.text.startswith("/"))
async def generic_message_gate(message: Message):
    subscribed = await is_subscribed_to_channel(message.from_user.id)
    if not subscribed:
        await _send_sub_required(message, "")


# ── /lang ────────────────────────────────────────────────────

@dp.message(Command("lang"))
async def cmd_lang(message: Message):
    user   = message.from_user
    player = await get_player(user.id)
    if not player:
        player, _ = await get_or_create_player(user.id, user.username or "")

    if not await is_subscribed_to_channel(user.id):
        await _send_sub_required(message, "")
        return

    current_lang = player.get("lang", "en")
    new_lang     = "en" if current_lang == "ru" else "ru"

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE players SET lang=? WHERE user_id=?", (new_lang, user.id)
        )
        await db.commit()

    logger.info(f"User {user.id} switched language: {current_lang} → {new_lang}")
    await message.answer(tr(new_lang, "lang_changed"), parse_mode=ParseMode.HTML)


# ── /bonus ───────────────────────────────────────────────────

@dp.message(Command("bonus"))
async def cmd_bonus(message: Message):
    if not await is_subscribed_to_channel(message.from_user.id):
        await _send_sub_required(message, "")
        return
    await _give_daily_bonus(
        message.from_user.id, message.from_user.username or "", message
    )


@dp.callback_query(F.data == "daily_bonus")
async def cb_daily_bonus(cb: CallbackQuery):
    await cb.answer()
    await _give_daily_bonus(cb.from_user.id, cb.from_user.username or "", cb.message)


async def _give_daily_bonus(user_id: int, username: str, target: Message):
    player, _ = await get_or_create_player(user_id, username)
    lang      = player.get("lang", "en")
    last_b    = player.get("last_bonus")

    if last_b:
        last_dt = datetime.fromisoformat(last_b)
        diff    = datetime.utcnow() - last_dt
        if diff < timedelta(hours=BONUS_COOLDOWN_HOURS):
            remaining = timedelta(hours=BONUS_COOLDOWN_HOURS) - diff
            hrs  = int(remaining.total_seconds() // 3600)
            mins = int((remaining.total_seconds() % 3600) // 60)
            await target.answer(
                tr(lang, "bonus_cooldown", hours=hrs, mins=mins),
                parse_mode=ParseMode.HTML,
            )
            return

    amount  = round(random.uniform(BONUS_MIN_USD, BONUS_MAX_USD), 2)
    new_bal = round((player.get("balance") or 0) + amount, 2)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE players SET balance=?, last_bonus=datetime('now') WHERE user_id=?",
            (new_bal, user_id),
        )
        await db.commit()

    await target.answer(
        tr(lang, "bonus_received", amount=amount, balance=new_bal),
        parse_mode=ParseMode.HTML,
    )


# ── Callback: referrals ──────────────────────────────────────

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


# ── Admin: Reply to support message ──────────────────────────

@dp.message(F.reply_to_message)
async def admin_reply(message: Message):
    """
    When the admin does a Reply on a forwarded support message:
      1. Extract reply_to_message.message_id  (= admin_msg_id stored in DB)
      2. Look up user_id in support_messages WHERE admin_msg_id = that ID
      3. Send the reply text to that user
    """
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return

    replied_msg_id = message.reply_to_message.message_id

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id FROM support_messages WHERE admin_msg_id=? AND direction='in'",
            (replied_msg_id,),
        ) as cur:
            row = await cur.fetchone()

        if not row:
            logger.info(
                f"Admin replied to msg_id={replied_msg_id} — no matching ticket found"
            )
            await message.reply("⚠️ No support ticket linked to this message.")
            return

        target_uid = row["user_id"]
        txt        = message.text or message.caption or ""

        await db.execute(
            """
            INSERT INTO support_messages (user_id, text, direction, admin_msg_id)
            VALUES (?, ?, 'out', ?)
            """,
            (target_uid, txt, replied_msg_id),
        )
        await db.commit()

    p    = await get_player(target_uid)
    lang = (p or {}).get("lang", "en")
    try:
        await bot.send_message(
            target_uid,
            tr(lang, "support_reply", text=txt),
            parse_mode=ParseMode.HTML,
        )
        logger.info(f"Admin replied to user {target_uid}: {txt[:80]}")
        await message.reply(
            f"✅ Reply delivered to user <code>{target_uid}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(f"Admin reply delivery failed: {e}")
        await message.reply(f"❌ Delivery failed: {e}")


# ── Admin: Broadcast ─────────────────────────────────────────

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject):
    """Admin only: /broadcast [text]"""
    if message.from_user.id != ADMIN_ID:
        return

    text = command.args
    if not text:
        await message.answer("Usage: /broadcast [message text]")
        return

    user_ids  = await get_all_user_ids()
    delivered = 0

    await message.answer(f"📣 Starting broadcast to {len(user_ids)} users…")

    for uid in user_ids:
        try:
            await bot.send_message(uid, text, parse_mode=ParseMode.HTML)
            delivered += 1
        except Exception:
            pass  # User blocked the bot — skip
        await asyncio.sleep(0.05)  # Respect Telegram rate limits

    player = await get_player(ADMIN_ID)
    lang   = (player or {}).get("lang", "en")
    await message.answer(
        tr(lang, "broadcast_done", ok=delivered),
        parse_mode=ParseMode.HTML,
    )


# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    print(f"\n{'='*55}")
    print(f"  🌐  App URL  : {WEBAPP_URL}")
    print(f"  🔗  Webhook  : {WEBHOOK_URL}")
    print(f"  🚪  Port     : {PORT}")
    print(f"{'='*55}\n")

    try:
        await bot.set_webhook(
            WEBHOOK_URL,
            drop_pending_updates=True,
            request_timeout=30,
            allowed_updates=["message", "callback_query", "chat_member"],
        )
        logger.info("Webhook registered successfully.")
    except Exception as e:
        logger.warning(f"Webhook registration failed: {e}")

    # Pre-warm bot username cache so first referral_url() call is instant
    try:
        global _bot_username
        me = await bot.get_me()
        _bot_username = me.username
        logger.info(f"Bot username cached: @{_bot_username}")
    except Exception as e:
        logger.warning(f"Could not cache bot username: {e}")

    t1 = asyncio.create_task(auction_worker())
    t2 = asyncio.create_task(reminder_worker())

    yield

    t1.cancel()
    t2.cancel()
    try:
        await bot.delete_webhook()
    except Exception:
        pass


app = FastAPI(title="PhotoFlip API v8", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")


# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    if Path("index.html").exists():
        return FileResponse("index.html")
    return PlainTextResponse("Upload index.html to the project root.")


# ── Live Feed ─────────────────────────────────────────────────

@app.get("/api/feed")
async def api_feed():
    """Generate 10 live feed events. random.sample prevents repeated names in one batch."""
    batch_size = min(10, len(FAKE_USERS))
    sampled    = random.sample(FAKE_USERS, batch_size)
    events     = []

    for user in sampled:
        seller_pool   = [n for n in FAKE_USERS if n != user]
        seller        = random.choice(seller_pool)
        amount        = round(random.uniform(2.0, 48.0), 2)
        lang_ev, tmpl = random.choice(FEED_ACTIONS)
        action        = tmpl.format(seller=seller, amount=amount)
        events.append({
            "user":      user,
            "action":    action,
            "amount":    amount,
            "lang":      lang_ev,
            "timestamp": (
                datetime.utcnow() - timedelta(seconds=random.randint(0, 600))
            ).isoformat(),
        })

    events.sort(key=lambda e: e["timestamp"], reverse=True)
    return {"events": events}


# ── Player ────────────────────────────────────────────────────

@app.get("/api/player/{user_id}")
async def api_get_player(user_id: int, username: str = ""):
    """
    WebApp entry point.
    Subscription is checked via the DB (quests table) — NOT via a live Telegram API call.
    Live Telegram calls happen only in /api/quest/complete (where user actively verifies).
    This avoids gameData corruption on every 30-second poll due to transient Telegram API errors.
    Admin always bypasses the subscription gate.
    """
    is_admin = (user_id == ADMIN_ID)

    # Create/fetch player first so we always have lang for error messages
    player, _ = await get_or_create_player(user_id, username)
    lang      = player.get("lang", "en")

    # Subscription gate — DB check only, no live Telegram call here
    if not is_admin:
        subscribed = await channels_all_subscribed(user_id)
        if not subscribed:
            channels = [
                {"id": list(ch.keys())[0], "url": ch.get("url", ""), "name": ch.get("name", "")}
                for ch in PARTNER_CHANNELS
            ]
            msg = tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en")
            return JSONResponse(
                status_code=402,
                content={
                    "error":            "subscription_required",
                    "channels":         channels,
                    "message":          msg,
                    "required_channel": REQUIRED_CHANNEL_URL,
                },
            )

    photos    = await get_player_photos(user_id, lang)
    quests    = await get_quest_status(user_id)
    ref_count = player.get("referrals_count", 0)
    lvl       = vip_level(ref_count)
    slots     = vip_slot_limit(ref_count)
    active    = await get_active_photo_count(user_id)
    ref       = await referral_url(user_id)
    await touch_last_seen(user_id)

    return {
        "player":                 player,
        "photos":                 photos,
        "quests":                 quests,
        "withdraw_unlocked":      ref_count >= MIN_REFERRALS_WITHDRAW,
        "vip_level":              lvl,
        "vip_tiers":              [
            {"min": t[0], "max_delay": t[1], "slots": t[2]} for t in VIP_TIERS
        ],
        "referral_url":           ref,
        "rub_rate":               RUB_TO_USD_RATE,
        "active_slots":           active,
        "slot_limit":             slots,
        "min_referrals_withdraw": MIN_REFERRALS_WITHDRAW,
        "withdraw_condition":     tr(lang, "withdraw_locked"),
        "vip_priority_note":      tr(lang, "vip_priority"),
    }


@app.put("/api/player/{user_id}/lang")
async def api_set_lang(user_id: int, request: Request):
    """Update player language from the Mini App language toggle."""
    data = await request.json()
    lang = data.get("lang", "en")
    if lang not in _T:
        lang = "en"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE players SET lang=? WHERE user_id=?", (lang, user_id)
        )
        await db.commit()
    return {"lang": lang}


# ── Referrals ─────────────────────────────────────────────────

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
    player, _ = await get_or_create_player(user_id, username)

    if (player["balance"] or 0) > 0:
        raise HTTPException(403, "Withdraw your balance before uploading new photos.")

    if not (1 <= len(files) <= 5):
        raise HTTPException(400, "Upload 1 to 5 photos at a time.")

    # ── Read file bytes + hard 10 MB size check ───────────────
    files_data: list[tuple[str, bytes]] = []
    for file in files:
        raw = await file.read()
        if len(raw) > MAX_FILE_SIZE:
            raise HTTPException(
                400,
                f"File '{file.filename}' exceeds the 10 MB limit "
                f"({len(raw) // (1024*1024):.1f} MB). Please compress it and retry.",
            )
        files_data.append((file.filename or "photo.jpg", raw))

    ref_count  = player.get("referrals_count", 0)
    slot_limit = vip_slot_limit(ref_count)
    active     = await get_active_photo_count(user_id)
    num_files  = len(files_data)

    if active + num_files > slot_limit:
        avail = max(slot_limit - active, 0)
        raise HTTPException(
            403,
            f"Slot limit reached ({active}/{slot_limit} active). "
            f"You can upload {avail} more. Upgrade VIP for more slots.",
        )

    is_pack  = (num_files == PACK_SIZE)
    if is_pack:
        total    = random.randint(PACK_MIN_RUB, PACK_MAX_RUB)
        rub_each = [total // num_files] * num_files
    else:
        rub_each = [
            random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB) for _ in range(num_files)
        ]

    max_delay = vip_max_delay(ref_count)
    batch_id  = uuid.uuid4().hex
    results   = []

    async with aiosqlite.connect(DB_PATH) as db:
        for idx, (orig_name, raw) in enumerate(files_data):
            ext      = Path(orig_name).suffix.lower() or ".jpg"
            filename = f"{uuid.uuid4().hex}{ext}"
            (UPLOADS_DIR / filename).write_bytes(raw)

            delay    = random.randint(MIN_DELAY_SECS, max(max_delay, MIN_DELAY_SECS + 1))
            sell_at  = (datetime.utcnow() + timedelta(seconds=delay)).isoformat()
            sale_rub = rub_each[idx]
            prev_usd = apply_commission(rub_to_usd(sale_rub))
            pid      = uuid.uuid4().hex

            await db.execute(
                """
                INSERT INTO photos
                    (id, user_id, filename, batch_id, base_price, final_price,
                     sale_rub, status, sell_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'on_auction', ?)
                """,
                (pid, user_id, filename, batch_id, prev_usd, prev_usd, sale_rub, sell_at),
            )
            results.append({
                "photo_id":    pid,
                "filename":    filename,
                "base_price":  prev_usd,
                "preview_rub": sale_rub,
                "status":      "on_auction",
                "vip_level":   vip_level(ref_count),
            })
        await db.commit()

    return {
        "batch_id":     batch_id,
        "is_pack":      is_pack,
        "photos":       results,
        "total_rub":    sum(rub_each),
        "slot_limit":   slot_limit,
        "active_after": active + num_files,
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
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        )
    except Exception as e:
        logger.warning(f"get_chat_member {user_id}/{channel_id}: {e}")
        verified = True  # Lenient fallback if bot can't check

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
        "quests":            await get_quest_status(user_id),
        "withdraw_unlocked": ref_count >= MIN_REFERRALS_WITHDRAW,
    }


# ── Withdraw ─────────────────────────────────────────────────

def _build_sub_required_response(lang: str) -> JSONResponse:
    channels = [
        {"id": list(ch.keys())[0], "url": ch.get("url", ""), "name": ch.get("name", "")}
        for ch in PARTNER_CHANNELS
    ]
    msg = tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en")
    return JSONResponse(
        status_code=402,
        content={
            "error":    "subscription_required",
            "channels": channels,
            "message":  msg,
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

    if not await channels_all_subscribed(user_id):
        return _build_sub_required_response(lang)

    if (player["balance"] or 0) <= 0:
        raise HTTPException(400, "Nothing to withdraw.")

    amount      = round(player["balance"], 2)
    lvl         = vip_level(ref_count)
    is_priority = 1 if lvl >= 1 else 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE players SET balance=0 WHERE user_id=?", (user_id,)
        )
        await db.execute(
            """
            INSERT INTO withdrawal_requests (user_id, amount_usd, method, is_priority)
            VALUES (?, ?, 'usd', ?)
            """,
            (user_id, amount, is_priority),
        )
        await db.commit()

    if ADMIN_ID:
        try:
            prio_tag = "⭐ VIP PRIORITY\n" if is_priority else ""
            await bot.send_message(
                ADMIN_ID,
                f"💳 <b>USD Withdrawal</b>\n{prio_tag}"
                f"User: <code>{user_id}</code> (@{player.get('username', '')})\n"
                f"Amount: <b>${amount:.2f}</b> · VIP <b>{lvl}</b> · Refs <b>{ref_count}</b>\n"
                f"<i>{tr('en', 'vip_priority')}</i>",
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
        await db.execute(
            "UPDATE players SET balance=0 WHERE user_id=?", (user_id,)
        )
        await db.execute(
            """
            INSERT INTO withdrawal_requests (user_id, amount_usd, stars, method, is_priority)
            VALUES (?, ?, ?, 'stars', ?)
            """,
            (user_id, usd_amount, stars_amount, is_priority),
        )
        await db.commit()

    if ADMIN_ID:
        try:
            prio_tag = "⭐ VIP PRIORITY\n" if is_priority else ""
            await bot.send_message(
                ADMIN_ID,
                f"⭐ <b>Stars Withdrawal</b>\n{prio_tag}"
                f"User: <code>{user_id}</code> (@{player.get('username', '')})\n"
                f"${usd_amount:.2f} → <b>{stars_amount:,} ⭐</b> · VIP <b>{lvl}</b>\n"
                f"<i>{tr('en', 'vip_priority')}</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    return {
        "success":       True,
        "withdrawn_usd": usd_amount,
        "stars":         stars_amount,
        "new_balance":   0.0,
    }


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

    # Forward message to admin and capture the sent message_id → admin_msg_id
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
            logger.info(f"Support ticket forwarded to admin, admin_msg_id={admin_msg_id}")
        except Exception as e:
            # Log full error so we can diagnose (blocked bot, wrong token, etc.)
            logger.error(f"Support forward to admin FAILED (user={user_id}): {type(e).__name__}: {e}")
            # Still save the message to DB so the user knows it was received,
            # but admin_msg_id will be NULL — admin reply won't work until re-sent.

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO support_messages (user_id, text, direction, admin_msg_id)
            VALUES (?, ?, 'in', ?)
            """,
            (user_id, text, admin_msg_id),
        )
        await db.commit()

    if admin_msg_id is None:
        logger.warning(
            f"Support message from user={user_id} saved to DB but NOT delivered to admin. "
            f"Check that ADMIN_ID={ADMIN_ID} has started the bot and hasn't blocked it."
        )

    return {"success": True}


@app.get("/api/support/messages/{user_id}")
async def api_support_messages(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, text, direction, created_at
            FROM support_messages
            WHERE user_id=?
            ORDER BY created_at ASC
            LIMIT 100
            """,
            (user_id,),
        ) as cur:
            rows = await cur.fetchall()
    return {"messages": [dict(r) for r in rows]}


# ── Admin ─────────────────────────────────────────────────────

def _require_admin(request: Request):
    if request.headers.get("X-Admin-Token", "") != str(ADMIN_ID):
        raise HTTPException(403, "Forbidden")


@app.get("/api/admin/stats")
async def api_admin_stats(request: Request):
    _require_admin(request)
    return await get_admin_stats()


@app.get("/api/admin/withdrawals")
async def api_admin_withdrawals(request: Request):
    _require_admin(request)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT wr.*, p.username
            FROM withdrawal_requests wr
            LEFT JOIN players p ON p.user_id = wr.user_id
            ORDER BY wr.is_priority DESC, wr.created_at ASC
            """
        ) as cur:
            rows = await cur.fetchall()
    return {"withdrawals": [dict(r) for r in rows]}


@app.post("/api/admin/support/reply")
async def api_admin_support_reply(request: Request):
    _require_admin(request)
    data         = await request.json()
    user_id      = data.get("user_id")
    reply_text   = (data.get("text") or "").strip()
    admin_msg_id = data.get("admin_msg_id")

    if not user_id or not reply_text:
        raise HTTPException(400, "Missing user_id or text")

    player = await get_player(user_id)
    if not player:
        raise HTTPException(404, "Player not found")

    lang = player.get("lang", "en")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO support_messages (user_id, text, direction, admin_msg_id)
            VALUES (?, ?, 'out', ?)
            """,
            (user_id, reply_text, admin_msg_id),
        )
        await db.commit()

    try:
        await bot.send_message(
            user_id,
            tr(lang, "support_reply", text=reply_text),
            parse_mode=ParseMode.HTML,
        )
        logger.info(f"Admin (API) replied to {user_id}")
    except Exception as e:
        logger.warning(f"Admin API reply delivery failed: {e}")

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
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
