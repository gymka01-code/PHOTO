"""
PhotoFlip — Telegram Mini App Backend  v6.0
FastAPI + Aiogram 3 + aiosqlite  |  Railway edition

CHANGES in v6:
 - FIXED: Referral system — bonus only for NEW users, proper deep-link extraction
 - FIXED: /lang command — toggles ru↔en and replies in new language
 - NEW:   /bonus command — daily reward with 24h cooldown
 - NEW:   /broadcast command — admin mass message with delivery report
 - NEW:   Live feed /api/feed — 50+ nicknames, random events
 - NEW:   Market news on /start for returning users
 - NEW:   💰 Bonus button in main menu
 - FIXED: Admin reply prints to logs + print(f"Admin replied to {user_id}")
 - FIXED: Subscription check ONLY at final withdrawal step
 - FIXED: HOST 0.0.0.0, PORT from env
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
BOT_TOKEN    = os.getenv("BOT_TOKEN",  "8700481112:AAGwUZffQtN0r9KsEq_dZk3liQeLg_9L3Xw")
ADMIN_ID     = int(os.getenv("ADMIN_ID", "7502434760"))
PORT         = int(os.getenv("PORT", "80"))

WEBHOOK_PATH = "/webhook"
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

# ── 50+ NPC names for live feed ──────────────────────────────
NPC_NAMES = [
    "SkilledTrader", "CryptoKing", "MasterPhoto", "Dimon777", "Alena_V",
    "User_X", "PhotoPro99", "NightOwl42", "LuckyStar88", "TraderMax",
    "Sheikh Al-Rashid", "Victoria Chen", "Marcus Webb", "Priya Nair",
    "Alejandro Torres", "Yuki Tanaka", "Isabella Rossi", "Omar Hassan",
    "Sophie Laurent", "Raj Patel", "Sergey_K", "Anna_Photo", "DenisFlip",
    "KatyaBest", "VolodiaT", "RuslanPro", "Misha_88", "Tanya2024",
    "IgorAuction", "SvetaFlip", "AlexPhoto", "NatalyV", "PavelX",
    "OlegMaster", "VikaBoss", "ArtemTrade", "DashaPro", "KirilPhoto",
    "ZoyaFlip", "TimurK", "LenaAuction", "FedorPix", "GalyaX",
    "BorisTrade", "ZinaPhoto", "YuraBest", "MilaFlip", "KostikPro",
    "NikaAuction", "Andrey777", "photo_ninja", "snap_master", "lens_pro",
    "click_boss", "pixel_hunter", "frame_king", "shot_wizard", "art_flipper",
]

FEED_ACTIONS = [
    ("ru", "🖼 Купил фото у {seller}"),
    ("ru", "💸 Продал фото за ${amount}"),
    ("ru", "🔨 Выставил на аукцион"),
    ("ru", "🏆 Победил в торгах за ${amount}"),
    ("en", "🖼 Bought a photo from {seller}"),
    ("en", "💸 Sold a photo for ${amount}"),
    ("en", "🔨 Listed photo on auction"),
    ("en", "🏆 Won an auction for ${amount}"),
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
        "btn_open":         "📸 Open PhotoFlip",
        "btn_referrals":    "🤝 Referrals",
        "btn_bonus":        "💰 Get Daily Bonus",
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
        "status_auction":   "In Auction",
        "support_reply":    "📨 <b>Support reply:</b>\n\n{text}",
        "remind": (
            "⏰ <b>PhotoFlip reminder</b>\n\n"
            "Your photos are live on auction! "
            "Invite friends to speed up sales and unlock withdrawal.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "withdraw_locked":  "Invite <b>3 friends</b> to unlock withdrawal.",
        "vip_priority":     "⭐ VIP users (levels 1–5) get priority in the withdrawal queue.",
        "sub_required_ru":  "Почти готово! Подпишитесь на каналы партнёров для верификации.",
        "sub_required_en":  "Almost there! Subscribe to partner channels to verify your account.",
        "lang_changed":     "🌐 Language switched to <b>English</b>.",
        "bonus_received":   "🎁 Daily bonus: <b>+${amount:.2f}</b>!\n\nBalance: <b>${balance:.2f}</b>",
        "bonus_cooldown":   "⏳ Come back in <b>{hours}h {mins}m</b> for your next bonus.",
        "broadcast_done":   "📣 Broadcast complete. Delivered: <b>{ok}</b> users.",
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
        "btn_open":         "📸 Открыть PhotoFlip",
        "btn_referrals":    "🤝 Рефералы",
        "btn_bonus":        "💰 Получить бонус",
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
        "status_auction":   "На аукционе",
        "support_reply":    "📨 <b>Ответ поддержки:</b>\n\n{text}",
        "remind": (
            "⏰ <b>Напоминание PhotoFlip</b>\n\n"
            "Ваши фото на аукционе! "
            "Приглашайте друзей, чтобы ускорить продажи и разблокировать вывод.\n\n"
            "🔗 <code>{ref_url}</code>"
        ),
        "withdraw_locked":  "Пригласите <b>3 друзей</b> для активации вывода.",
        "vip_priority":     "⭐ Пользователи с VIP-статусом (уровень 1–5) получают приоритет в очереди на вывод средств.",
        "sub_required_ru":  "Почти готово! Подпишитесь на каналы партнёров для верификации.",
        "sub_required_en":  "Almost there! Subscribe to partner channels to verify your account.",
        "lang_changed":     "🌐 Язык переключён на <b>Русский</b>.",
        "bonus_received":   "🎁 Ежедневный бонус: <b>+${amount:.2f}</b>!\n\nБаланс: <b>${balance:.2f}</b>",
        "bonus_cooldown":   "⏳ Следующий бонус через <b>{hours}ч {mins}м</b>.",
        "broadcast_done":   "📣 Рассылка завершена. Доставлено: <b>{ok}</b> пользователей.",
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
        # players
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id         INTEGER PRIMARY KEY,
                username        TEXT,
                balance         REAL    DEFAULT 0.0,
                total_earned    REAL    DEFAULT 0.0,
                photos_sold     INTEGER DEFAULT 0,
                referrals_count INTEGER DEFAULT 0,
                lang            TEXT    DEFAULT 'ru',
                last_seen       TEXT    DEFAULT (datetime('now')),
                last_bonus      TEXT    DEFAULT NULL,
                created_at      TEXT    DEFAULT (datetime('now'))
            )""")
        # Safe migrations — add columns that may be missing in old DB
        for col in [
            "referrals_count INTEGER DEFAULT 0",
            "lang TEXT DEFAULT 'ru'",
            "last_seen TEXT DEFAULT (datetime('now'))",
            "last_bonus TEXT DEFAULT NULL",
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

        # withdrawal_requests
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

async def player_exists(user_id: int) -> bool:
    """Check if player is already in DB (used BEFORE create for referral logic)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM players WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
    return row is not None


async def get_or_create_player(user_id: int, username: str = "") -> tuple[dict, bool]:
    """Returns (player_dict, is_new). is_new=True if user was just created."""
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
            return dict(row), True   # ← new user
        return dict(row), False      # ← existing user


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


async def get_player_photos(user_id: int, lang: str = "ru") -> list:
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
        p.pop("sell_at", None)
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
    result = []
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for ch in PARTNER_CHANNELS:
            channel_id = list(ch.keys())[0]
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


async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM players") as c:
            rows = await c.fetchall()
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
                ) as c:
                    due = await c.fetchall()

                for photo in due:
                    photo      = dict(photo)
                    buyer      = random.choice(NPC_NAMES)
                    sale_rub   = photo.get("sale_rub") or random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
                    gross      = rub_to_usd(float(sale_rub))
                    net        = apply_commission(gross)
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
                        player  = await get_player(photo["user_id"])
                        if not player:
                            continue
                        lang    = player.get("lang", "ru")
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
    """Ping inactive users once per hour."""
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
                lang = row["lang"] or "ru"
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


# ── /start ───────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user = message.from_user

    # ── ОТЛАДКА: видно в логах Railway ──────────────────────
    print(f"DEBUG: Start cmd with args: {message.text}")

    # ── Определяем: новый пользователь или нет ──────────────
    player, is_new = await get_or_create_player(user.id, user.username or "")
    await touch_last_seen(user.id)
    lang = player.get("lang", "ru")

    # ── Реферальная система: засчитывается ТОЛЬКО новым ─────
    args = command.args  # aiogram 3 корректно извлекает параметр из deep-link
    if args and args.startswith("ref_"):
        try:
            referrer_id = int(args[4:])  # "ref_12345" → 12345
            print(f"DEBUG: Referral detected: referrer={referrer_id}, new_user={is_new}, uid={user.id}")
            # Бонус ТОЛЬКО если пользователь новый И не пытается сам себя пригласить
            if is_new and referrer_id != user.id:
                async with aiosqlite.connect(DB_PATH) as db:
                    # INSERT OR IGNORE предотвращает дубли
                    await db.execute(
                        "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)",
                        (referrer_id, user.id),
                    )
                    await db.execute(
                        "UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?",
                        (referrer_id,),
                    )
                    await db.commit()
                # Уведомляем пригласившего
                try:
                    rp = await get_player(referrer_id)
                    if rp:
                        rc    = rp.get("referrals_count", 0) + 1
                        rl    = vip_level(rc)
                        rs    = vip_slot_limit(rc)
                        rlang = rp.get("lang", "ru")
                        await bot.send_message(
                            referrer_id,
                            tr(rlang, "new_referral",
                               name=user.first_name, count=rc, lvl=rl, slots=rs),
                            parse_mode=ParseMode.HTML,
                        )
                except Exception as e:
                    logger.debug(f"Referral notify failed: {e}")
            elif not is_new:
                print(f"DEBUG: Referral skipped — user {user.id} already registered")
        except ValueError:
            print(f"DEBUG: Could not parse referrer ID from args: {args}")

    # Обновляем данные игрока после возможных изменений реферала
    player = await get_player(user.id)
    ref    = await referral_url(user.id)
    lvl    = vip_level(player.get("referrals_count", 0))
    slots  = vip_slot_limit(player.get("referrals_count", 0))

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tr(lang, "btn_open"),
                              web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(text=tr(lang, "btn_referrals"),
                              callback_data="show_referrals"),
         InlineKeyboardButton(text=tr(lang, "btn_bonus"),
                              callback_data="daily_bonus")],
    ])

    # Для существующих пользователей — случайная "новость рынка"
    news_suffix = ""
    if not is_new:
        news_list = MARKET_NEWS_RU if lang == "ru" else MARKET_NEWS_EN
        news_suffix = f"\n\n{random.choice(news_list)}"

    await message.answer(
        tr(lang, "welcome",
           balance=player["balance"] or 0, vip=lvl, slots=slots, ref_url=ref) + news_suffix,
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


# ── /lang — переключение языка ───────────────────────────────

@dp.message(Command("lang"))
async def cmd_lang(message: Message):
    user   = message.from_user
    player = await get_player(user.id)
    if not player:
        player, _ = await get_or_create_player(user.id, user.username or "")

    current_lang = player.get("lang", "ru")
    new_lang     = "en" if current_lang == "ru" else "ru"

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET lang=? WHERE user_id=?", (new_lang, user.id))
        await db.commit()

    print(f"DEBUG: User {user.id} switched language: {current_lang} → {new_lang}")
    await message.answer(tr(new_lang, "lang_changed"), parse_mode=ParseMode.HTML)


# ── /bonus — ежедневная награда ──────────────────────────────

@dp.message(Command("bonus"))
async def cmd_bonus(message: Message):
    await _give_daily_bonus(message.from_user.id, message.from_user.username or "", message)


@dp.callback_query(F.data == "daily_bonus")
async def cb_daily_bonus(cb: CallbackQuery):
    await cb.answer()
    await _give_daily_bonus(cb.from_user.id, cb.from_user.username or "", cb.message)


async def _give_daily_bonus(user_id: int, username: str, target: Message):
    player, _ = await get_or_create_player(user_id, username)
    lang      = player.get("lang", "ru")
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


# ── Callback: рефералы ───────────────────────────────────────

@dp.callback_query(F.data == "show_referrals")
async def cb_referrals(cb: CallbackQuery):
    await cb.answer()
    player = await get_player(cb.from_user.id)
    lang   = (player or {}).get("lang", "ru")
    ref    = await referral_url(cb.from_user.id)
    count  = (player or {}).get("referrals_count", 0)
    await cb.message.answer(
        tr(lang, "referrals_msg", count=count, ref_url=ref),
        parse_mode=ParseMode.HTML,
    )


# ── Admin: Reply на сообщение поддержки ──────────────────────

@dp.message(F.reply_to_message)
async def admin_reply(message: Message):
    """Когда админ отвечает на пересланное сообщение — ответ летит пользователю."""
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
        target  = row["user_id"]
        txt     = message.text or message.caption or ""
        await db.execute(
            "INSERT INTO support_messages (user_id, text, direction, admin_msg_id) VALUES (?,?,'out',?)",
            (target, txt, replied_id),
        )
        await db.commit()

    p    = await get_player(target)
    lang = (p or {}).get("lang", "ru")
    try:
        await bot.send_message(target, tr(lang, "support_reply", text=txt),
                               parse_mode=ParseMode.HTML)
        print(f"Admin replied to {target}")  # ← лог для Railway
    except Exception as e:
        logger.warning(f"support reply delivery failed: {e}")


# ── Admin: Broadcast ─────────────────────────────────────────

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject):
    """Только для администратора: /broadcast [текст]"""
    if message.from_user.id != ADMIN_ID:
        return

    text = command.args
    if not text:
        await message.answer("Использование: /broadcast [текст рассылки]")
        return

    user_ids  = await get_all_user_ids()
    delivered = 0

    await message.answer(f"📣 Начинаю рассылку для {len(user_ids)} пользователей...")

    for uid in user_ids:
        try:
            await bot.send_message(uid, text, parse_mode=ParseMode.HTML)
            delivered += 1
        except Exception:
            pass  # пользователь заблокировал бота — пропускаем
        await asyncio.sleep(0.05)  # не превышаем лимит Telegram API

    player = await get_player(ADMIN_ID)
    lang   = (player or {}).get("lang", "ru")
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
    print(f"{'='*55}\n")
    try:
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
        logger.info("Webhook registered successfully.")
    except Exception as e:
        logger.warning(f"Webhook registration failed: {e}")

    t1 = asyncio.create_task(auction_worker())
    t2 = asyncio.create_task(reminder_worker())

    yield

    t1.cancel()
    t2.cancel()
    await bot.delete_webhook()


app = FastAPI(title="PhotoFlip API v6", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")


# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    if Path("index.html").exists():
        return FileResponse("index.html")
    return PlainTextResponse("Загрузите интерфейс на GitHub")


# ── Live Feed ─────────────────────────────────────────────────

@app.get("/api/feed")
async def api_feed():
    """
    Генератор живой ленты: 10 случайных событий.
    Ники, суммы и типы действий выбираются случайно при каждом запросе.
    """
    events = []
    for _ in range(10):
        user    = random.choice(NPC_NAMES)
        seller  = random.choice([n for n in NPC_NAMES if n != user])
        amount  = round(random.uniform(2.0, 48.0), 2)
        lang_ev, action_tmpl = random.choice(FEED_ACTIONS)
        action  = action_tmpl.format(seller=seller, amount=amount)
        events.append({
            "user":      user,
            "action":    action,
            "amount":    amount,
            "lang":      lang_ev,
            "timestamp": (datetime.utcnow() - timedelta(seconds=random.randint(0, 600))).isoformat(),
        })
    # Сортируем по времени (новейшие первые)
    events.sort(key=lambda e: e["timestamp"], reverse=True)
    return {"events": events}


# ── Player ────────────────────────────────────────────────────

@app.get("/api/player/{user_id}")
async def api_get_player(user_id: int, username: str = ""):
    player, _  = await get_or_create_player(user_id, username)
    lang       = player.get("lang", "ru")
    photos     = await get_player_photos(user_id, lang)
    quests     = await get_quest_status(user_id)
    ref_count  = player.get("referrals_count", 0)
    lvl        = vip_level(ref_count)
    slots      = vip_slot_limit(ref_count)
    active     = await get_active_photo_count(user_id)
    ref        = await referral_url(user_id)
    await touch_last_seen(user_id)

    return {
        "player":               player,
        "photos":               photos,
        "quests":               quests,
        "withdraw_unlocked":    ref_count >= MIN_REFERRALS_WITHDRAW,
        "vip_level":            lvl,
        "vip_tiers":            [{"min": t[0], "max_delay": t[1], "slots": t[2]} for t in VIP_TIERS],
        "referral_url":         ref,
        "rub_rate":             RUB_TO_USD_RATE,
        "active_slots":         active,
        "slot_limit":           slots,
        "min_referrals_withdraw": MIN_REFERRALS_WITHDRAW,
        "withdraw_condition":   tr(lang, "withdraw_locked"),
        "vip_priority_note":    tr(lang, "vip_priority"),
    }


@app.put("/api/player/{user_id}/lang")
async def api_set_lang(user_id: int, request: Request):
    data = await request.json()
    lang = data.get("lang", "ru")
    if lang not in _T:
        lang = "ru"
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
    player, _ = await get_or_create_player(user_id, username)

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

    is_pack  = (num_files == PACK_SIZE)
    if is_pack:
        total    = random.randint(PACK_MIN_RUB, PACK_MAX_RUB)
        rub_each = [total // num_files] * num_files
    else:
        rub_each = [random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB) for _ in range(num_files)]

    max_delay = vip_max_delay(ref_count)
    batch_id  = uuid.uuid4().hex
    results   = []

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
            ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR
        )
    except Exception as e:
        logger.warning(f"get_chat_member {user_id}/{channel_id}: {e}")
        verified = True

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
# Проверка подписки — ТОЛЬКО здесь, на финальном шаге вывода.

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

    lang      = player.get("lang", "ru")
    ref_count = player.get("referrals_count", 0)
    if ref_count < MIN_REFERRALS_WITHDRAW:
        raise HTTPException(403, tr(lang, "withdraw_locked"))

    # ← Мягкая конверсия: подписка проверяется только здесь
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

    lang      = player.get("lang", "ru")
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
            """SELECT wr.*, p.username FROM withdrawal_requests wr
               LEFT JOIN players p ON p.user_id = wr.user_id
               ORDER BY wr.is_priority DESC, wr.created_at ASC"""
        ) as c:
            rows = await c.fetchall()
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
    lang = player.get("lang", "ru")

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
        print(f"Admin replied to {user_id}")
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
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
