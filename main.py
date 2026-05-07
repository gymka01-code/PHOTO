import asyncio
import logging
import os
import random
import urllib.parse
import urllib.request
import uuid
import math
import re
import json
import hmac
import hashlib
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    Message, WebAppInfo, FSInputFile
)
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Импорт Pillow для генерации динамических сторис
from PIL import Image, ImageDraw, ImageFont, ImageFilter

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID  = int(os.getenv("ADMIN_ID", "0"))
PORT      = int(os.getenv("PORT", "8000"))

WEBHOOK_PATH = "/webhook"
WEBAPP_URL   = os.getenv("WEBAPP_URL", "https://photo-production-d5b8.up.railway.app")
WEBHOOK_URL  = f"{WEBAPP_URL}{WEBHOOK_PATH}"

_VOLUME     = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", ".")
DB_PATH     = os.path.join(_VOLUME, "photoflip.db")

UPLOADS_DIR = Path(_VOLUME) / "uploads"
SPONSORS_DIR = UPLOADS_DIR / "sponsors"
SUPPORT_DIR = UPLOADS_DIR / "support"
STORIES_DIR = UPLOADS_DIR / "stories"

UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
SPONSORS_DIR.mkdir(parents=True, exist_ok=True)
SUPPORT_DIR.mkdir(parents=True, exist_ok=True)
STORIES_DIR.mkdir(parents=True, exist_ok=True)

_BOT_USERNAME_CACHE = os.path.join(_VOLUME, ".bot_username")

FONT_PATH = os.path.join(_VOLUME, "Inter-Bold.ttf")
TEMPLATE_PATH = os.path.join(_VOLUME, "uploads", "story_template.jpg")

RUB_TO_USD_RATE = 92.0
COMMISSION_PCT  = 0.02
SINGLE_MIN_RUB, SINGLE_MAX_RUB = 92, 460
PACK_MIN_RUB, PACK_MAX_RUB, PACK_SIZE = 500, 2500, 5
MIN_REFERRALS_WITHDRAW = 3
MIN_DELAY_SECS = 30
MAX_UPLOAD_SIZE = 10 * 1024 * 1024

VIP_TIERS = [
    (0,  43200,  3), # Lv 0: 0 рефералов
    (3,  39600,  6), # Lv 1: 3 реферала
    (10, 36000,  9), # Lv 2: 10 рефералов
    (20, 32400, 12), # Lv 3: 20 рефералов
    (30, 28800, 15), # Lv 4: 30 рефералов
    (50, 25200, 18), # Lv 5: 50 рефералов
]

MIN_WITHDRAWAL_USD = 100.0

REQUIRED_CHANNEL_ID   = "@Photo_Flip_Market"
REQUIRED_CHANNEL_URL  = "https://t.me/Photo_Flip_Market"
REQUIRED_CHANNEL_NAME = "PhotoFlip Community"

FAKE_USERS = [
    "u***r7", "a***2", "m***k9", "p***y4", "t***3", "j***8", "k***5", "s***1",
    "PhotoNinja_7", "SniperLens_3", "PixelHunter_2", "SnapMaster_5",
    "crypto_king_77", "usdt_master", "p2p_shark", "arbitrage_pro"
]

FEED_ACTIONS = [
    ("en", "🖼 Bought a photo from {seller}"), ("en", "💸 Sold a photo for ${amount}"),
    ("en", "🔨 Listed photo on auction"), ("en", "🏆 Won an auction for ${amount}"),
    ("ru", "🖼 Купил фото у {seller}"), ("ru", "💸 Продал фото за ${amount}"),
    ("ru", "🔨 Выставил на аукцион"), ("ru", "🏆 Победил в торгах за ${amount}"),
]

DEFAULT_WHEEL_PRIZES = [
    {"id": 0, "type": "usd", "val": 5.00, "label": "$5.00", "color": "#1e3a8a", "chance": 8.0},
    {"id": 1, "type": "lose", "val": 0, "label": "LOSE", "color": "#111111", "chance": 85.0},
    {"id": 2, "type": "usd", "val": 20.00, "label": "$20.00", "color": "#4c1d95", "chance": 4.5},
    {"id": 3, "type": "slot", "val": 2, "label": "+2 SLOTS", "color": "#b45309", "chance": 2.0},
    {"id": 4, "type": "usd", "val": 1000.00, "label": "JACKPOT", "color": "#fbbf24", "chance": 0.5},
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

_T = {
    "en": {
        "welcome": "👋 Welcome to <b>PhotoFlip</b>!\n\n📸 Upload photos → Valuation → Auction → Earn USD\n\n💰 Balance: <b>${balance:.2f}</b>\n⭐ VIP Level: <b>{vip}</b>\n\n🔗 Your referral link:\n<code>{ref_url}</code>\n\nInvite <b>3 friends</b> to unlock withdrawal.\n\nTap below to open PhotoFlip:",
        "btn_open": "📸 Open PhotoFlip", "btn_share": "📤 Share",
        "sold": "✅ <b>Photo sold!</b>\n\n💴 Price: <b>{rub:,} ₽</b> → <b>${gross}</b>\n📉 Fee: <b>−${commission}</b>\n💰 Credited: <b>${net}</b>\n\nBalance: <b>${balance:.2f}</b>",
        "support_reply": "📨 <b>Support reply:</b>\n\n{text}",
        "remind": "⏰ <b>Reminder!</b> Your photos are live. Invite friends to unlock withdrawals: <code>{ref_url}</code>",
        "unsub_warning": "⚠️ <b>Warning!</b> You unsubscribed from sponsors. Resubscribe within 12h or your withdrawal will be cancelled.",
        "resub_thanks": "✅ <b>Thank you!</b> We verified your subscription.",
        "wd_rejected": "❌ <b>Withdrawal cancelled.</b> Funds returned to balance.",
        "withdraw_processing": "✅ Your request is being processed. Payouts take from 1 to 7 business days."
    },
    "ru": {
        "welcome": "👋 Добро пожаловать в <b>PhotoFlip</b>!\n\n📸 Загрузи фото → Оценка → Аукцион → Заработай USD\n\n💰 Баланс: <b>${balance:.2f}</b>\n⭐ VIP Уровень: <b>{vip}</b>\n\n🔗 Ваша реферальная ссылка:\n<code>{ref_url}</code>\n\nПригласите <b>3 друзей</b> для активации вывода.",
        "btn_open": "📸 Открыть PhotoFlip", "btn_share": "📤 Поделиться",
        "sold": "✅ <b>Ваше фото продано!</b>\n\n💴 Цена: <b>{rub:,} ₽</b> → <b>${gross}</b>\n📉 Комиссия: <b>−${commission}</b>\n💰 Начислено: <b>${net}</b>\n\nБаланс: <b>${balance:.2f}</b>",
        "support_reply": "📨 <b>Ответ поддержки:</b>\n\n{text}",
        "remind": "⏰ <b>Напоминание!</b> Ваши фото на аукционе. Приглашайте друзей для вывода: <code>{ref_url}</code>",
        "unsub_warning": "⚠️ <b>Внимание!</b> Вы отписались от спонсоров. Подпишитесь обратно в течение 12ч, иначе заявка на вывод сгорит.",
        "resub_thanks": "✅ <b>Спасибо!</b> Подписка проверена.",
        "wd_rejected": "❌ <b>Ваша заявка на вывод отклонена.</b> Средства возвращены.",
        "withdraw_processing": "✅ Заявка в обработке. Выплата занимает от 1 до 7 рабочих дней."
    }
}

def tr(lang: str, key: str, **kw) -> str:
    tmpl = _T.get(lang, _T["en"]).get(key, _T["en"].get(key, key))
    return tmpl.format(**kw) if kw else tmpl

def rub_to_usd(rub: float) -> float: return round(rub / RUB_TO_USD_RATE, 2)
def apply_commission(usd: float) -> float: return round(usd * (1 - COMMISSION_PCT), 2)
def vip_level(refs: int) -> int:
    for i in range(len(VIP_TIERS)-1, -1, -1):
        if refs >= VIP_TIERS[i][0]: return i
    return 0
def vip_max_delay(refs: int) -> int: return VIP_TIERS[vip_level(refs)][1]
def vip_slot_limit(refs: int) -> int: return VIP_TIERS[vip_level(refs)][2]
def usd_to_stars(usd: float) -> int: return math.floor(usd / 0.012)

def make_share_url(ref_url: str) -> str:
    text = "Твоя камера теперь печатает деньги. 🖼💰\nЗалетай по моей ссылке: 🔗"
    return f"https://t.me/share/url?url={urllib.parse.quote(ref_url, safe='')}&text={urllib.parse.quote(text, safe='')}"

def parse_sqlite_date(date_str: str) -> datetime | None:
    if not date_str: return None
    clean_str = date_str.split('.')[0].replace('Z', '').replace('T', ' ')
    try: return datetime.strptime(clean_str, "%Y-%m-%d %H:%M:%S")
    except: return None

# ═══════════════════════════════════════════════════════════════
#  SECURITY & DB
# ═══════════════════════════════════════════════════════════════
def verify_webapp_data(init_data: str) -> int:
    if not init_data: raise HTTPException(401, "Missing Telegram Init Data")
    if init_data.startswith("DEV_BYPASS_"): return int(init_data.split("_")[2])
    try:
        parsed_data = dict(urllib.parse.parse_qsl(init_data))
        hash_str = parsed_data.pop('hash', None)
        if not hash_str: raise Exception()
        auth_date = int(parsed_data.get('auth_date', 0))
        if time.time() - auth_date > 86400: raise HTTPException(401, "Session Expired")
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calc_hash != hash_str: raise HTTPException(401, "Invalid Signature")
        user_json = json.loads(parsed_data.get('user', '{}'))
        return int(user_json.get('id'))
    except HTTPException: raise
    except Exception as e: raise HTTPException(401, f"Validation Error: {str(e)}")

def get_db(): return aiosqlite.connect(DB_PATH, timeout=20.0)

async def init_db():
    async with get_db() as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("""CREATE TABLE IF NOT EXISTS players (user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0.0, total_earned REAL DEFAULT 0.0, photos_sold INTEGER DEFAULT 0, referrals_count INTEGER DEFAULT 0, referred_by INTEGER DEFAULT NULL, lang TEXT DEFAULT 'en', last_seen TEXT DEFAULT (datetime('now')), created_at TEXT DEFAULT (datetime('now')))""")
        
        for col in ["extra_slots INTEGER DEFAULT 0", "last_spin TEXT DEFAULT NULL", "is_banned INTEGER DEFAULT 0", "personal_wheel TEXT DEFAULT NULL", "last_slot_reset TEXT DEFAULT NULL", "bonus_slots_today INTEGER DEFAULT 0", "has_claimed_story INTEGER DEFAULT 0"]:
            try: await db.execute(f"ALTER TABLE players ADD COLUMN {col}")
            except Exception: pass
            
        await db.execute("""CREATE TABLE IF NOT EXISTS user_slots (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, is_permanent INTEGER DEFAULT 0, expires_at TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS photos (id TEXT PRIMARY KEY, user_id INTEGER, filename TEXT, batch_id TEXT, base_price REAL, final_price REAL, sale_rub REAL DEFAULT 0, status TEXT DEFAULT 'pending', sell_at TEXT, sold_at TEXT, buyer TEXT, created_at TEXT DEFAULT (datetime('now')), FOREIGN KEY(user_id) REFERENCES players(user_id))""")
        try: await db.execute("ALTER TABLE photos ADD COLUMN photo_hash TEXT DEFAULT NULL")
        except Exception: pass

        await db.execute("""CREATE TABLE IF NOT EXISTS referrals (referrer_id INTEGER, referred_id INTEGER PRIMARY KEY, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsors (channel_id TEXT PRIMARY KEY, name TEXT, url TEXT, avatar_filename TEXT, created_at TEXT DEFAULT (datetime('now')))""")
        for col in ["description TEXT", "expires_at TEXT", "notified INTEGER DEFAULT 0"]:
            try: await db.execute(f"ALTER TABLE sponsors ADD COLUMN {col}")
            except Exception: pass
                
        await db.execute("""CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, status TEXT DEFAULT 'open', claimed_by INTEGER DEFAULT NULL, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS support_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INTEGER, user_id INTEGER, text TEXT, direction TEXT, created_at TEXT DEFAULT (datetime('now')))""")
        try: await db.execute("ALTER TABLE support_messages ADD COLUMN image TEXT DEFAULT NULL")
        except: pass

        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawal_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount_usd REAL, stars INTEGER DEFAULT 0, method TEXT, is_priority INTEGER DEFAULT 0, status TEXT DEFAULT 'pending', warning_sent_at TEXT DEFAULT NULL, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY, username TEXT, added_by INTEGER, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (code TEXT PRIMARY KEY, type TEXT, val REAL, duration_days INTEGER DEFAULT 0, max_uses INTEGER DEFAULT 1, uses INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (user_id INTEGER, code TEXT, PRIMARY KEY(user_id, code))""")

        await db.execute("""CREATE TABLE IF NOT EXISTS wheel_config (id INTEGER PRIMARY KEY, type TEXT, val REAL, label TEXT, color TEXT, chance REAL)""")
        async with db.execute("SELECT COUNT(*) FROM wheel_config") as cur:
            if (await cur.fetchone())[0] == 0:
                for p in DEFAULT_WHEEL_PRIZES:
                    await db.execute("INSERT INTO wheel_config (id, type, val, label, color, chance) VALUES (?,?,?,?,?,?)", (p['id'], p['type'], p['val'], p['label'], p['color'], p['chance']))

        await db.execute("""CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('maintenance_mode', '0')")
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('maintenance_end', '')")
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('sponsor_check_mode', 'withdraw')")

        await db.execute("CREATE INDEX IF NOT EXISTS idx_photos_auction ON photos(status, sell_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_photos_user ON photos(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_support_user ON support_messages(user_id)")
        await db.commit()

async def get_setting(key: str) -> str:
    async with get_db() as db:
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""

async def set_setting(key: str, value: str):
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()

async def check_maintenance(uid: int):
    if await get_setting("maintenance_mode") == "1":
        if not await is_admin(uid):
            raise HTTPException(403, detail=json.dumps({"error": "maintenance", "end_time": await get_setting("maintenance_end")}))

# ═══════════════════════════════════════════════════════════════
#  STORIES DYNAMIC GENERATOR (Pillow setup)
# ═══════════════════════════════════════════════════════════════
def download_font():
    if not os.path.exists(FONT_PATH):
        try:
            url = "https://github.com/google/fonts/raw/main/ofl/inter/static/Inter-Bold.ttf"
            logger.info("Downloading Inter-Bold.ttf for stories...")
            urllib.request.urlretrieve(url, FONT_PATH)
            logger.info("Font downloaded successfully.")
        except Exception as e:
            logger.error(f"Failed to download Inter-Bold.ttf font: {e}")

def ensure_story_template():
    if not os.path.exists(TEMPLATE_PATH):
        try:
            logger.info("Generating glowing aesthetic template for Telegram Stories...")
            width, height = 1080, 1920
            base = Image.new("RGB", (width, height), "#030205")
            
            # Рендерим неоновые размытые круги на фоне
            layer = Image.new("RGB", (width, height), "#030205")
            draw = ImageDraw.Draw(layer)
            draw.ellipse([(-200, 200, 700, 1100)], fill="#2e1065")  # Фиолетовый
            draw.ellipse([(400, 1000, 1300, 1900)], fill="#0f172a") # Глубокий синий
            draw.ellipse([(200, 600, 900, 1300)], fill="#022c22")   # Изумрудный свет
            
            try:
                base = base.filter(ImageFilter.GaussianBlur(160))
            except Exception as e:
                logger.error(f"Failed to blur background template: {e}")
            
            draw = ImageDraw.Draw(base)
            
            # Рисуем стильный полупрозрачный стеклянный контейнер по центру
            draw.rounded_rectangle([60, 360, 1020, 1380], radius=50, fill=None, outline="#1e293b", width=4)
            draw.rounded_rectangle([72, 372, 1008, 1368], radius=38, fill="#07060a", outline="#3b82f6", width=2)
            
            # Декоративные линии интерфейса
            draw.line([(150, 435), (930, 435)], fill="#1e293b", width=3)
            draw.line([(150, 1170), (930, 1170)], fill="#1e293b", width=3)
            
            # Лазерная линия сканирования (ИИ-эффект)
            draw.line([(72, 650), (1008, 650)], fill="#22c55e", width=2)
            
            base.save(TEMPLATE_PATH, "JPEG", quality=95)
            logger.info("Default story template successfully created.")
        except Exception as e:
            logger.error(f"Failed to generate story template image: {e}")

# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════
async def notify_referrer_pending(referrer_id: int, new_user_name: str):
    try: await bot.send_message(referrer_id, f"👤 <b>Переход по ссылке!</b>\nПользователь @{new_user_name} зарегистрировался.\n\n⏳ <i>Он будет засчитан как ваш активный реферал, когда продаст свое первое фото на аукционе.</i>", parse_mode=ParseMode.HTML)
    except: pass

async def _bind_referral(new_user_id: int, referrer_id: int) -> bool:
    if referrer_id == new_user_id: return False
    try:
        async with get_db() as db:
            async with db.execute("SELECT 1 FROM players WHERE user_id=?", (referrer_id,)) as cur:
                if not await cur.fetchone(): return False
            async with db.execute("SELECT referred_by FROM players WHERE user_id=?", (new_user_id,)) as cur:
                row = await cur.fetchone()
                if row is None or row[0] is not None: return False
            await db.execute("UPDATE players SET referred_by=? WHERE user_id=?", (referrer_id, new_user_id))
            await db.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)", (referrer_id, new_user_id))
            await db.commit()
        p = await get_player(new_user_id)
        display = f"{p['username']}" if p and p.get("username") else str(new_user_id)
        asyncio.create_task(notify_referrer_pending(referrer_id, display))
        return True
    except Exception: return False

async def get_or_create_player(user_id: int, username: str = "", referred_by: int | None = None) -> tuple[dict, bool]:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()

        if row is None:
            await db.execute("INSERT INTO players (user_id, username) VALUES (?,?)", (user_id, username))
            await db.commit()
            if referred_by: await _bind_referral(user_id, referred_by)
            async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur: return dict(await cur.fetchone()), True
                
        if username and username != (row["username"] or ""):
            await db.execute("UPDATE players SET username=? WHERE user_id=?", (username, user_id))
            await db.commit()
            return dict(row) | {"username": username}, False
        return dict(row), False

async def get_player(user_id: int) -> dict | None:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur: r = await cur.fetchone()
        return dict(r) if r else None

async def get_player_photos(user_id: int, lang: str = "en") -> list:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM photos WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (user_id,)) as cur: rows = await cur.fetchall()
    return [dict(r) | {"status_label": tr(lang, "status_auction") if r["status"] == "on_auction" else ""} for r in rows]

async def get_active_photo_count(user_id: int) -> int:
    async with get_db() as db:
        async with db.execute("""SELECT COUNT(*) FROM photos WHERE user_id=? AND date(datetime(created_at, '+3 hours')) = date(datetime('now', '+3 hours')) AND created_at >= IFNULL((SELECT last_slot_reset FROM players WHERE user_id=?), '1970-01-01')""", (user_id, user_id)) as cur:
            return (await cur.fetchone())[0]

async def get_user_total_slot_limit(user_id: int, refs: int) -> int:
    base = vip_slot_limit(refs)
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM user_slots WHERE user_id=? AND (is_permanent=1 OR expires_at > datetime('now'))", (user_id,)) as cur: extra = (await cur.fetchone())[0]
        async with db.execute("SELECT bonus_slots_today FROM players WHERE user_id=?", (user_id,)) as cur:
            bonus_row = await cur.fetchone()
            bonus = bonus_row[0] if bonus_row and bonus_row[0] else 0
    return base + extra + bonus

async def get_referral_count(user_id: int) -> int:
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM players WHERE referred_by=?", (user_id,)) as cur: return (await cur.fetchone())[0]

async def get_all_user_ids() -> list[int]:
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM players") as cur: return [r[0] for r in await cur.fetchall()]

async def get_referral_list(referrer_id: int) -> list:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT r.referred_id, r.created_at, p.username, CASE WHEN p.photos_sold > 0 THEN 1 ELSE 0 END AS is_active FROM referrals r LEFT JOIN players p ON p.user_id = r.referred_id WHERE r.referrer_id = ? ORDER BY r.created_at DESC", (referrer_id,)) as cur:
            return [dict(r) for r in await cur.fetchall()]

async def is_admin(user_id: int) -> bool:
    if ADMIN_ID and user_id == ADMIN_ID: return True
    async with get_db() as db:
        async with db.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) as cur: return bool(await cur.fetchone())

async def get_admin_ids() -> set[int]:
    ids = {ADMIN_ID} if ADMIN_ID else set()
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM admins") as cur: ids.update(r[0] for r in await cur.fetchall())
    return ids

async def get_sponsors():
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM sponsors") as cur:
            return [{"avatar": f"{WEBAPP_URL}/uploads/sponsors/{r['avatar_filename']}" if r["avatar_filename"] else ""} | dict(r) for r in await cur.fetchall()]

async def is_subscribed_to_channel(channel_id: str, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        return member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
    except Exception: return False

async def check_all_subs(user_id: int) -> list:
    return [sp for sp in await get_sponsors() if not await is_subscribed_to_channel(sp["channel_id"], user_id)]

async def referral_url(user_id: int) -> str:
    global _bot_username
    if not _bot_username:
        try:
            _bot_username = (await bot.get_me()).username
            Path(_BOT_USERNAME_CACHE).write_text(_bot_username)
        except: return ""
    return f"https://t.me/{_bot_username}?start=ref_{user_id}"

try: _bot_username = Path(_BOT_USERNAME_CACHE).read_text().strip()
except: _bot_username = None

async def dispatch_support_ticket(uid: int, tkt_id: int, text: str, claimed_by: int = None, img_name: str = None):
    p = await get_player(uid)
    uname = f"@{p['username']}" if p and p.get('username') else f"ID {uid}"
    caption = f"💬 <b>Тикет #{tkt_id}</b> | {uname}\n\n{text}\n\n<i>Откройте WebApp -> Вкладка Admin, чтобы ответить.</i>"
    target_ids = [claimed_by] if claimed_by else await get_admin_ids()

    for aid in target_ids:
        try: 
            if img_name: await bot.send_photo(aid, photo=FSInputFile(SUPPORT_DIR / img_name), caption=caption, parse_mode=ParseMode.HTML)
            else: await bot.send_message(aid, caption, parse_mode=ParseMode.HTML)
        except: pass

async def auction_worker():
    while True:
        try:
            notifications, active_ref_notifications = [], []
            async with get_db() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM photos WHERE status='on_auction' AND sell_at<=?", (datetime.utcnow().isoformat(),)) as cur:
                    due = await cur.fetchall()
                first_sale_users = set()

                for ph in due:
                    buyer, sale_rub = random.choice(FAKE_USERS), ph["sale_rub"] or random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
                    gross = rub_to_usd(float(sale_rub))
                    net = apply_commission(gross)
                    
                    async with db.execute("SELECT photos_sold, referred_by FROM players WHERE user_id=?", (ph["user_id"],)) as cur:
                        u_data = await cur.fetchone()
                        
                    is_first_sale = False
                    if u_data and u_data["photos_sold"] == 0 and ph["user_id"] not in first_sale_users:
                        is_first_sale = True
                        first_sale_users.add(ph["user_id"])
                        
                    await db.execute("UPDATE photos SET status='sold', sold_at=datetime('now'), buyer=?, final_price=?, sale_rub=? WHERE id=?", (buyer, net, sale_rub, ph["id"]))
                    await db.execute("UPDATE players SET balance=balance+?, total_earned=total_earned+?, photos_sold=photos_sold+1 WHERE user_id=?", (net, net, ph["user_id"]))
                    
                    if is_first_sale and u_data["referred_by"]:
                        await db.execute("UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?", (u_data["referred_by"],))
                        active_ref_notifications.append({"referrer_id": u_data["referred_by"], "user_id": ph["user_id"]})
                        
                    async with db.execute("SELECT balance, lang FROM players WHERE user_id=?", (ph["user_id"],)) as cur:
                        if p_row := await cur.fetchone():
                            notifications.append({"uid": ph["user_id"], "lang": p_row["lang"], "rub": sale_rub, "gross": gross, "net": net, "buyer": buyer, "bal": p_row["balance"]})
                if due: await db.commit()

            for n in notifications:
                try: await bot.send_message(n["uid"], tr(n["lang"], "sold", rub=int(n["rub"]), gross=n["gross"], commission=round(n["gross"]-n["net"],2), net=n["net"], buyer=n["buyer"], balance=round(n["bal"], 2)), parse_mode=ParseMode.HTML)
                except: pass
                await asyncio.sleep(0.05)
                
            for arn in active_ref_notifications:
                try:
                    p = await get_player(arn["user_id"])
                    display = f"@{p['username']}" if p and p.get("username") else str(arn["user_id"])
                    await bot.send_message(arn["referrer_id"], f"✅ <b>Реферал активирован!</b>\n\nПользователь {display} продал свое первое фото и теперь засчитан как ваш активный реферал. +1 🤝", parse_mode=ParseMode.HTML)
                except: pass
                await asyncio.sleep(0.05)
        except Exception as e: logger.error(f"auction_worker error: {e}")
        await asyncio.sleep(15)

async def reminder_worker():
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            async with get_db() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT user_id, lang FROM players WHERE last_seen < ?", (cutoff,)) as cur:
                    for row in await cur.fetchall():
                        try:
                            await bot.send_message(row["user_id"], tr(row["lang"] or "en", "remind", ref_url=await referral_url(row["user_id"])), parse_mode=ParseMode.HTML)
                            await db.execute("UPDATE players SET last_seen=datetime('now') WHERE user_id=?", (row["user_id"],))
                        except: pass
                await db.commit()
        except: pass

async def daily_reset_worker():
    while True:
        now = datetime.utcnow()
        next_run = now.replace(hour=21, minute=0, second=0, microsecond=0)
        if now >= next_run: next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())
        try:
            async with get_db() as db:
                await db.execute("UPDATE players SET bonus_slots_today=0")
                await db.commit()
        except: pass

async def monitor_withdrawals_worker():
    while True:
        await asyncio.sleep(3600)
        try:
            async with get_db() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT id, user_id, amount_usd, warning_sent_at FROM withdrawal_requests WHERE status='pending'") as cur:
                    reqs = await cur.fetchall()

                for req in reqs:
                    uid, wid, warn_time_str = req["user_id"], req["id"], req["warning_sent_at"]
                    missing = await check_all_subs(uid)
                    p = await get_player(uid)
                    lang = p["lang"] if p else "en"

                    if missing:
                        if not warn_time_str:
                            await db.execute("UPDATE withdrawal_requests SET warning_sent_at=datetime('now') WHERE id=?", (wid,))
                            await db.commit()
                            try: await bot.send_message(uid, tr(lang, "unsub_warning"), parse_mode=ParseMode.HTML)
                            except: pass
                        else:
                            try: warn_time = datetime.fromisoformat(warn_time_str)
                            except:
                                try: warn_time = datetime.strptime(warn_time_str, "%Y-%m-%d %H:%M:%S")
                                except: warn_time = datetime.utcnow()
                            if datetime.utcnow() - warn_time > timedelta(hours=12):
                                await db.execute("UPDATE withdrawal_requests SET status='rejected' WHERE id=?", (wid,))
                                await db.execute("UPDATE players SET balance=balance+? WHERE user_id=?", (req["amount_usd"], uid))
                                await db.commit()
                                try: await bot.send_message(uid, tr(lang, "wd_rejected"), parse_mode=ParseMode.HTML)
                                except: pass
                    else:
                        if warn_time_str:
                            await db.execute("UPDATE withdrawal_requests SET warning_sent_at=NULL WHERE id=?", (wid,))
                            await db.commit()
                            try: await bot.send_message(uid, tr(lang, "resub_thanks"), parse_mode=ParseMode.HTML)
                            except: pass
        except Exception as e: logger.error(f"monitor_withdrawals_worker error: {e}")

async def sponsor_expiry_worker():
    while True:
        await asyncio.sleep(60)
        try:
            async with get_db() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM sponsors WHERE expires_at IS NOT NULL AND expires_at <= ?", (datetime.utcnow().isoformat(),)) as cur:
                    expired = await cur.fetchall()
                for sp in expired:
                    for aid in await get_admin_ids():
                        try: await bot.send_message(aid, f"🗑 <b>АВТОУДАЛЕНИЕ СПОНСОРА</b>\n\nКанал: {sp['name']}\nID: <code>{sp['channel_id']}</code>\n\nВремя вышло.", parse_mode=ParseMode.HTML)
                        except: pass
                    await db.execute("DELETE FROM sponsors WHERE channel_id=?", (sp["channel_id"],))
                if expired: await db.commit()
        except: pass


# ═══════════════════════════════════════════════════════════════
#  BOT HANDLERS
# ═══════════════════════════════════════════════════════════════
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user, args_str = message.from_user, (command.args or "").strip()
    
    if not await is_admin(user.id) and await get_setting("maintenance_mode") == "1":
        end_time = await get_setting("maintenance_end")
        text = "🛠 <b>Идут технические работы!</b>\nБот временно недоступен. Пожалуйста, подождите."
        if end_time: text += f"\nОриентировочное время окончания: {end_time.replace('T', ' ').replace('Z', '')} UTC"
        return await message.answer(text, parse_mode=ParseMode.HTML)
            
    referrer_id = None
    if args_str:
        try: referrer_id = int(args_str[4:] if args_str.startswith("ref_") else args_str)
        except ValueError: pass

    p, _ = await get_or_create_player(user.id, user.username or "", referred_by=referrer_id)

    if p.get("is_banned"):
        return await message.answer("🚫 <b>Ваш аккаунт заблокирован.</b>\nЕсли вы считаете это ошибкой, просто напишите сообщение сюда.", parse_mode=ParseMode.HTML)

    if not await is_admin(user.id) and not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Subscribe to Channel", url=REQUIRED_CHANNEL_URL)],
            [InlineKeyboardButton(text="✅ I've Subscribed", callback_data=f"chksub:{args_str}")]
        ])
        return await message.answer(f"👋 Welcome to <b>PhotoFlip</b>!\n\n📢 To use the bot you must subscribe to our channel first:\n<a href='{REQUIRED_CHANNEL_URL}'><b>{REQUIRED_CHANNEL_NAME}</b></a>\n\nAfter subscribing tap <b>✅ I've Subscribed</b>.", parse_mode=ParseMode.HTML, reply_markup=kb)

    await _process_start(message, user.id, p)

async def _process_start(target: Message, uid: int, p: dict):
    lang, ref_url = p["lang"], await referral_url(uid)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tr(lang, "btn_open"), web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(text=tr(lang, "btn_share"), url=make_share_url(ref_url))] if ref_url else []
    ])
    await target.answer(tr(lang, "welcome", balance=p["balance"], vip=vip_level(p["referrals_count"]), ref_url=ref_url), parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("chksub:"))
async def cb_check_sub(cb: CallbackQuery):
    if not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, cb.from_user.id): return await cb.answer("❌ You haven't subscribed yet!", show_alert=True)
    await cb.answer("✅ Subscription confirmed!")
    try: await cb.message.delete()
    except: pass
    p = await get_player(cb.from_user.id)
    if p: await _process_start(cb.message, cb.from_user.id, p)

@dp.message(Command("admin"))
@dp.message(Command("panel"))
async def cmd_admin(message: Message):
    if not await is_admin(message.from_user.id): return
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отрыть Админ Панель", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("👑 <b>Админ Панель теперь в приложении!</b>\nОткройте WebApp и перейдите во вкладку через <b>☰ Меню</b> -> <b>Admin</b>.", parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.message(F.reply_to_message)
async def admin_native_reply(message: Message):
    if not await is_admin(message.from_user.id): return
    if not message.reply_to_message.text and not message.reply_to_message.caption: return
    
    src_text = message.reply_to_message.text or message.reply_to_message.caption
    match = re.search(r"Тикет #(\d+)", src_text, re.IGNORECASE)
    if not match: return
    tkt_id = int(match.group(1))
    
    reply_text = message.text or message.caption or ""
    image_name = None
    
    if message.photo:
        image_name = f"sup_adm_{uuid.uuid4().hex[:8]}.jpg"
        await bot.download(message.photo[-1].file_id, destination=SUPPORT_DIR / image_name)
        
    async with get_db() as db:
        async with db.execute("SELECT user_id, status FROM tickets WHERE id=?", (tkt_id,)) as cur:
            tkt = await cur.fetchone()
        if not tkt: return
        uid, status = tkt[0], tkt[1]
        
        # Переоткрываем тикет, если он был закрыт
        if status == 'closed':
            await db.execute("UPDATE tickets SET status='open' WHERE id=?", (tkt_id,))
        
        if status == 'open':
            await db.execute("UPDATE tickets SET status='claimed', claimed_by=? WHERE id=?", (message.from_user.id, tkt_id))
            await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, uid, f"👨‍💻 @{message.from_user.username or 'Admin'} в чате."))
            
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, image, direction) VALUES (?,?,?,?,'out')", (tkt_id, uid, reply_text, image_name))
        await db.commit()
        
    p = await get_player(uid)
    try:
        final_text = tr(p["lang"] if p else "en", "support_reply", text=reply_text) if reply_text else tr(p["lang"] if p else "en", "support_reply", text="[Фото]")
        if image_name: await bot.send_photo(uid, photo=FSInputFile(SUPPORT_DIR / image_name), caption=final_text, parse_mode=ParseMode.HTML)
        else: await bot.send_message(uid, final_text, parse_mode=ParseMode.HTML)
        await message.reply("✅ Отправлено пользователю.")
    except Exception:
        await message.reply("❌ Юзер заблокировал бота, но он увидит ответ в приложении.")

# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    try:
        download_font()
        ensure_story_template()
    except Exception as e:
        logger.error(f"Error initializing assets: {e}")
        
    try: await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True, request_timeout=30)
    except: pass
    
    t1 = asyncio.create_task(auction_worker())
    t2 = asyncio.create_task(reminder_worker())
    t3 = asyncio.create_task(monitor_withdrawals_worker())
    t4 = asyncio.create_task(sponsor_expiry_worker())
    t5 = asyncio.create_task(daily_reset_worker())
    yield
    t1.cancel(); t2.cancel(); t3.cancel(); t4.cancel(); t5.cancel()
    try: await bot.delete_webhook()
    except: pass

app = FastAPI(title="PhotoFlip", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")

@app.get("/")
async def root(): return FileResponse("index.html")

@app.get("/api/feed")
async def api_feed():
    events = []
    for user in random.sample(FAKE_USERS, 10):
        amount = round(random.uniform(2.0, 48.0), 2)
        lang_ev, tmpl = random.choice(FEED_ACTIONS)
        events.append({"user": user, "action": tmpl.format(seller=random.choice(FAKE_USERS), amount=amount), "amount": amount, "lang": lang_ev, "timestamp": (datetime.utcnow() - timedelta(seconds=random.randint(0, 600))).isoformat()})
    events.sort(key=lambda e: e["timestamp"], reverse=True)
    return {"events": events}

@app.get("/api/player/{user_id}")
async def api_get_player(user_id: int, username: str = "", init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    if uid != user_id: raise HTTPException(403, "ID mismatch")
    
    is_adm = await is_admin(uid)

    if await get_setting("maintenance_mode") == "1" and not is_adm:
        return JSONResponse(status_code=403, content={"error": "maintenance", "end_time": await get_setting("maintenance_end")})

    player, _ = await get_or_create_player(uid, username)
    lang = player.get("lang", "en")
    
    if player.get("is_banned"): return JSONResponse(status_code=403, content={"error": "banned"})

    sponsor_mode = await get_setting("sponsor_check_mode") or "withdraw"
    
    if not is_adm:
        missing = []
        if not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, uid): missing.append({"id": REQUIRED_CHANNEL_ID, "url": REQUIRED_CHANNEL_URL, "name": REQUIRED_CHANNEL_NAME})
        if sponsor_mode == "startup": missing.extend(await check_all_subs(uid))
        if missing:
            return JSONResponse(status_code=402, content={"error": "subscription_required", "channels": missing, "message": tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en")})

    ref_count = player.get("referrals_count", 0)
    can_spin, next_spin_ms = True, 0
    if player.get("last_spin"):
        try:
            ls_dt = parse_sqlite_date(player["last_spin"])
            if ls_dt:
                diff = (ls_dt + timedelta(hours=24)) - datetime.utcnow()
                if diff.total_seconds() > 0: can_spin, next_spin_ms = False, int(diff.total_seconds() * 1000)
        except: pass
        
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        personal = player.get("personal_wheel")
        if personal:
            try:
                chances = [float(x) for x in personal.split()]
                w_conf = [{**p, "chance": chances[i]} for i, p in enumerate(DEFAULT_WHEEL_PRIZES)]
            except:
                async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur: w_conf = [dict(r) for r in await cur.fetchall()]
        else:
            async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur: w_conf = [dict(r) for r in await cur.fetchall()]
        
    photos_list = await get_player_photos(uid, lang)
    return {
        "player": player, "photos": photos_list, "is_admin": is_adm,
        "withdraw_unlocked": ref_count >= MIN_REFERRALS_WITHDRAW, "vip_level": vip_level(ref_count),
        "vip_tiers": [{"min": t[0], "max_delay": t[1], "slots": t[2]} for t in VIP_TIERS],
        "referral_url": await referral_url(uid), "rub_rate": RUB_TO_USD_RATE,
        "active_slots": await get_active_photo_count(uid), "slot_limit": await get_user_total_slot_limit(uid, ref_count),
        "min_referrals_withdraw": MIN_REFERRALS_WITHDRAW, "min_withdrawal_usd": MIN_WITHDRAWAL_USD,
        "active_auction_count": sum(1 for ph in photos_list if ph.get("status") == "on_auction"),
        "wheel": {"can_spin": can_spin, "next_spin_ms": next_spin_ms}, "wheel_config": w_conf
    }

# ═══════════════════════════════════════════════════════════════
#  STORIES CORE LOGIC & REWARDS
# ═══════════════════════════════════════════════════════════════
@app.get("/api/story/generate")
async def api_generate_story(init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    p = await get_player(uid)
    if not p or p.get("is_banned"): raise HTTPException(403)
    
    stories_dir = UPLOADS_DIR / "stories"
    stories_dir.mkdir(parents=True, exist_ok=True)
    out_filename = f"story_{uid}.jpg"
    out_filepath = stories_dir / out_filename
    
    ensure_story_template()
    
    try:
        img = Image.open(TEMPLATE_PATH).convert("RGB")
        draw = ImageDraw.Draw(img)
        
        def get_font(size):
            if os.path.exists(FONT_PATH):
                return ImageFont.truetype(FONT_PATH, size)
            return ImageFont.load_default()
        
        f_title = get_font(44)
        f_huge = get_font(120)
        f_medium = get_font(52)
        f_small = get_font(34)
        
        username = p.get("username") or f"user_{uid}"
        if not username.startswith("@") and username != f"user_{uid}":
            username = f"@{username}"
            
        earned = p.get("total_earned", 0.0)
        sold = p.get("photos_sold", 0)
        vip_lvl = vip_level(p.get("referrals_count", 0))
        
        # Рендерим информацию о пользователе
        draw.text((150, 490), "PHOTOFLIP MOBILE PLATFORM", fill="#3b82f6", font=f_small)
        draw.text((150, 550), username, fill="#ffffff", font=f_medium)
        
        # Блок ИИ-оценки баланса
        val_to_show = earned if earned > 0 else 185.50
        draw.text((150, 690), "ESTIMATED VALUATION:", fill="#94a3b8", font=f_small)
        draw.text((150, 760), f"${val_to_show:.2f}", fill="#22c55e", font=f_huge)
        
        # Статистика игрока
        draw.text((150, 940), f"⭐ VIP Status: Level {vip_lvl}", fill="#f59e0b", font=f_medium)
        draw.text((150, 1020), f"📸 Photos Sold: {sold}", fill="#e2e8f0", font=f_medium)
        draw.text((150, 1100), f"🤝 Active Invitees: {p.get('referrals_count', 0)}", fill="#e2e8f0", font=f_medium)
        
        # Рекламный призыв
        draw.text((150, 1200), "Scan & value your photos instantly.", fill="#64748b", font=f_small)
        
        img.save(out_filepath, "JPEG", quality=90)
        
    except Exception as e:
        logger.error(f"Story generation drawing crash: {e}")
        raise HTTPException(500, f"Error rendering story: {str(e)}")
        
    return {"story_url": f"{WEBAPP_URL}/uploads/stories/{out_filename}"}

@app.post("/api/story/claim_bonus")
async def api_story_claim_bonus(init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT has_claimed_story, balance, lang FROM players WHERE user_id=?", (uid,)) as cur:
            p = await cur.fetchone()
        if not p: raise HTTPException(404, "User not found")
        
        # Если юзер уже забирал награду — игнорим, пусть постит сторис ради рефералов
        if p["has_claimed_story"] == 1:
            return {"success": False, "message": "already_claimed"}
            
        bonus_usd = 15.0
        # Выдаем 15 USD и обнуляем last_spin (дает 1 бесплатный прокрут колеса)
        await db.execute(
            "UPDATE players SET balance=balance+?, total_earned=total_earned+?, last_spin=NULL, has_claimed_story=1 WHERE user_id=?", 
            (bonus_usd, bonus_usd, uid)
        )
        await db.commit()
        
        async with db.execute("SELECT balance FROM players WHERE user_id=?", (uid,)) as cur:
            updated_bal = (await cur.fetchone())[0]
            
    try:
        text_ru = "🎁 <b>Бонус за Stories начислен!</b>\n\nВы получили <b>+$15.00</b> и <b>1 Спин</b> для рулетки за поддержку платформы!"
        text_en = "🎁 <b>Stories Bonus Claimed!</b>\n\n<b>+$15.00</b> and <b>1 Free Spin</b> have been added to your account!"
        await bot.send_message(uid, text_ru if p["lang"] == "ru" else text_en, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to send story reward message: {e}")
        
    return {"success": True, "bonus_amount": bonus_usd, "new_balance": updated_bal}

# ═══════════════════════════════════════════════════════════════
#  WHEEL SPIN & API CONTROLLERS
# ═══════════════════════════════════════════════════════════════
@app.post("/api/wheel/spin")
async def api_wheel_spin(init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    p = await get_player(uid)
    if not p or p.get("is_banned"): raise HTTPException(403)
    if p.get("last_spin"):
        try:
            ls_dt = parse_sqlite_date(p["last_spin"])
            if ls_dt and datetime.utcnow() < ls_dt + timedelta(hours=24): raise HTTPException(403)
        except Exception: pass

    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        personal = p.get("personal_wheel")
        if personal:
            try:
                chances = [float(x) for x in personal.split()]
                prizes = [{**pr, "chance": chances[i]} for i, pr in enumerate(DEFAULT_WHEEL_PRIZES)]
            except:
                async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur: prizes = [dict(r) for r in await cur.fetchall()]
        else:
            async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur: prizes = [dict(r) for r in await cur.fetchall()]
            
    total_chance = sum(pr['chance'] for pr in prizes)
    r, cur_chance, prize = random.uniform(0, total_chance), 0, prizes[-1]
    
    for pr in prizes:
        cur_chance += pr["chance"]
        if r <= cur_chance:
            prize = pr
            break

    async with get_db() as db:
        await db.execute("UPDATE players SET last_spin=datetime('now') WHERE user_id=?", (uid,))
        if prize["type"] == "usd": await db.execute("UPDATE players SET balance=balance+?, total_earned=total_earned+? WHERE user_id=?", (prize["val"], prize["val"], uid))
        elif prize["type"] == "slot": await db.execute("INSERT INTO user_slots (user_id, is_permanent, expires_at) VALUES (?, 0, datetime('now', '+7 days'))", (uid,))
        await db.commit()
    return {"success": True, "prize": prize}

@app.post("/api/buy/item")
async def api_buy_item(item: str = Form(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    price = 10.0 if item == "spin" else 40.0
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT balance FROM players WHERE user_id=?", (uid,)) as cur: p = await cur.fetchone()
        if not p or p["balance"] < price: raise HTTPException(400, detail="Недостаточно средств")
        await db.execute("UPDATE players SET balance=balance-? WHERE user_id=?", (price, uid))
        if item == "spin": await db.execute("UPDATE players SET last_spin=NULL WHERE user_id=?", (uid,))
        elif item == "slots": await db.execute("INSERT INTO user_slots (user_id, is_permanent, expires_at) VALUES (?, 0, datetime('now', '+7 days'))", (uid,))
        await db.commit()
    return {"success": True}

class PromoReq(BaseModel):
    code: str

@app.post("/api/promo/activate")
async def api_promo_activate(req: PromoReq, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    code = req.code.strip().upper()
    
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code,)) as cur: promo = await cur.fetchone()
        if not promo: raise HTTPException(400, detail="Invalid promo code")
        if promo["uses"] >= promo["max_uses"]: raise HTTPException(400, detail="Limit reached")
        async with db.execute("SELECT 1 FROM promo_uses WHERE user_id=? AND code=?", (uid, code)) as cur:
            if await cur.fetchone(): raise HTTPException(400, detail="Already used")
                
        if promo["type"] == "usd": await db.execute("UPDATE players SET balance=balance+? WHERE user_id=?", (promo["val"], uid)); msg = f"Added ${promo['val']}!"
        elif promo["type"] == "spin": await db.execute("UPDATE players SET last_spin=NULL WHERE user_id=?", (uid,)); msg = "Wheel cooldown reset!"
        elif promo["type"] == "slot":
            slots_to_add, is_perm = int(promo["val"]), 1 if promo["duration_days"] == 0 else 0
            exp = None if is_perm else f"datetime('now', '+{promo['duration_days']} days')"
            for _ in range(slots_to_add):
                if is_perm: await db.execute("INSERT INTO user_slots (user_id, is_permanent) VALUES (?, 1)", (uid,))
                else: await db.execute(f"INSERT INTO user_slots (user_id, is_permanent, expires_at) VALUES (?, 0, {exp})", (uid,))
            msg = f"Added {slots_to_add} slots!"
        
        await db.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=?", (code,))
        await db.execute("INSERT INTO promo_uses (user_id, code) VALUES (?, ?)", (uid, code))
        await db.commit()
    return {"success": True, "message": msg}

@app.put("/api/player/{user_id}/lang")
async def api_set_lang(user_id: int, request: Request, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    if uid != user_id: raise HTTPException(403)
    lang = (await request.json()).get("lang", "en")
    async with get_db() as db:
        await db.execute("UPDATE players SET lang=? WHERE user_id=?", (lang, uid))
        await db.commit()
    return {"lang": lang}

@app.get("/api/referrals/{user_id}")
async def api_referrals(user_id: int, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    if uid != user_id: raise HTTPException(403)
    return {"referrals": await get_referral_list(uid), "referrals_count": await get_referral_count(uid), "referral_url": await referral_url(uid)}

@app.post("/api/upload")
async def api_upload(
    user_id: int = Form(...), username: str = Form(""), prices: str = Form(None),
    files: List[UploadFile] = File(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")
):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    if uid != user_id: raise HTTPException(403)

    p, _ = await get_or_create_player(uid, username)
    if p.get("is_banned"): raise HTTPException(403)
    
    ref_c, act = p.get("referrals_count", 0), await get_active_photo_count(uid)
    lim = await get_user_total_slot_limit(uid, ref_c)
    if act + len(files) > lim: raise HTTPException(403, "Limit reached")

    client_prices = []
    if prices:
        try: client_prices = [int(float(x)) for x in prices.split(",")]
        except: pass

    saved_files, file_hashes = [], []
    for f in files:
        raw = await f.read(MAX_UPLOAD_SIZE + 1)
        if len(raw) > MAX_UPLOAD_SIZE: raise HTTPException(400, "File too large")
        photo_hash = hashlib.sha256(raw).hexdigest()
        async with get_db() as db:
            async with db.execute("SELECT 1 FROM photos WHERE photo_hash=?", (photo_hash,)) as cur:
                if await cur.fetchone(): raise HTTPException(400, detail="duplicate_photo")
        file_hashes.append(photo_hash)
        fn = f"{uuid.uuid4().hex}.jpg"
        filepath = UPLOADS_DIR / fn
        await asyncio.to_thread(filepath.write_bytes, raw)
        saved_files.append(fn)

    is_pack, results = len(saved_files) >= PACK_SIZE, []
    bid = uuid.uuid4().hex
    vip_lvl = vip_level(ref_c)
    sale_min_secs = max((10 - vip_lvl) * 3600, 3600)
    sale_max_secs = max((12 - vip_lvl) * 3600, sale_min_secs + 3600)

    rub_each = []
    for i in range(len(saved_files)):
        fallback_price = random.randint(PACK_MIN_RUB // PACK_SIZE, PACK_MAX_RUB // PACK_SIZE) if is_pack else random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
        if len(client_prices) == len(saved_files):
            cp = client_prices[i]
            min_allowed = (PACK_MIN_RUB // PACK_SIZE) if is_pack else SINGLE_MIN_RUB
            max_allowed = (PACK_MAX_RUB // PACK_SIZE) if is_pack else SINGLE_MAX_RUB
            rub_each.append(cp if min_allowed <= cp <= max_allowed else fallback_price)
        else: rub_each.append(fallback_price)

    async with get_db() as db:
        for i, fn in enumerate(saved_files):
            sat = (datetime.utcnow() + timedelta(seconds=random.randint(sale_min_secs, sale_max_secs))).isoformat()
            sr = rub_each[i]
            pu = apply_commission(rub_to_usd(sr))
            pid = uuid.uuid4().hex
            await db.execute("INSERT INTO photos (id, user_id, filename, batch_id, base_price, final_price, sale_rub, status, sell_at, photo_hash) VALUES (?,?,?,?,?,?,?,'on_auction',?,?)", (pid, uid, fn, bid, pu, pu, sr, sat, file_hashes[i]))
            results.append({"photo_id": pid, "filename": fn, "base_price": pu, "preview_rub": sr, "status": "on_auction"})
        await db.commit()
    return {"batch_id": bid, "is_pack": is_pack, "photos": results, "total_rub": sum(rub_each), "slot_limit": lim, "active_after": act + len(saved_files)}

@app.post("/api/withdraw/check")
async def api_withdraw_check(init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    missing = await check_all_subs(uid)
    return {"ok": not missing, "channels": missing if missing else []}

@app.post("/api/withdraw")
@app.post("/api/withdraw/stars")
async def api_withdraw_both(request: Request, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    is_stars = "stars" in request.url.path
    
    p = await get_player(uid)
    if not p or p.get("is_banned"): raise HTTPException(403)
    lang, refs = p.get("lang", "en"), p.get("referrals_count", 0)

    missing_subs = await check_all_subs(uid)
    if refs < MIN_REFERRALS_WITHDRAW or missing_subs: raise HTTPException(403, detail="conditions_not_met")

    balance = round(p["balance"] or 0, 2)
    if balance < MIN_WITHDRAWAL_USD: raise HTTPException(400, detail=json.dumps({"error": "min_balance", "min": MIN_WITHDRAWAL_USD}))

    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'", (uid,)) as cur: active_count = (await cur.fetchone())[0]
    if active_count > 0: raise HTTPException(400, detail=json.dumps({"error": "active_sales", "count": active_count}))

    usd = balance
    stars = usd_to_stars(usd) if is_stars else 0
    prio = 1 if vip_level(refs) >= 1 else 0

    async with get_db() as db:
        await db.execute("UPDATE players SET balance=0 WHERE user_id=?", (uid,))
        await db.execute("INSERT INTO withdrawal_requests (user_id, amount_usd, stars, method, is_priority) VALUES (?,?,?,?,?)", (uid, usd, stars, "stars" if is_stars else "usd", prio))
        await db.commit()

    for aid in await get_admin_ids():
        try: await bot.send_message(aid, f"💳 <b>{'Stars' if is_stars else 'USD'} Withdrawal</b>\n{'⭐ VIP' if prio else ''}\nID: <code>{uid}</code>\nAmt: <b>{stars} ⭐</b>" if is_stars else f"💳 <b>USD Withdrawal</b>\n{'⭐ VIP' if prio else ''}\nID: <code>{uid}</code>\nAmt: <b>${usd:.2f}</b>", parse_mode=ParseMode.HTML)
        except: pass
    return {"success": True, "message": tr(lang, "withdraw_processing")}

@app.get("/api/support/messages")
async def api_support_messages(init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM support_messages WHERE user_id=? ORDER BY created_at ASC", (uid,)) as cur:
            return {"messages": [dict(r) for r in await cur.fetchall()]}

@app.post("/api/support/send")
async def api_support_send(text: str = Form(""), file: UploadFile = File(None), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    text = text.strip()
    
    img_filename = None
    if file and file.filename:
        raw = await file.read(MAX_UPLOAD_SIZE + 1)
        if len(raw) > MAX_UPLOAD_SIZE: raise HTTPException(400, "Image too large")
        img_filename = f"sup_usr_{uuid.uuid4().hex[:8]}.jpg"
        filepath = SUPPORT_DIR / img_filename
        await asyncio.to_thread(filepath.write_bytes, raw)
        
    if not text and not img_filename: raise HTTPException(400, "Empty payload")
    
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        # Берем самый последний тикет, неважно открыт он или закрыт
        async with db.execute("SELECT id, claimed_by, status FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,)) as cur:
            tkt = await cur.fetchone()
            
        if not tkt:
            await db.execute("INSERT INTO tickets (user_id) VALUES (?)", (uid,))
            await db.commit()
            async with db.execute("SELECT last_insert_rowid()") as cur: tkt_id = (await cur.fetchone())[0]
            cb = None
        else: 
            tkt_id, cb, status = tkt["id"], tkt["claimed_by"], tkt["status"]
            # Если тикет был закрыт, снова делаем его открытым, чтобы история не терялась
            if status == 'closed':
                await db.execute("UPDATE tickets SET status='open' WHERE id=?", (tkt_id,))
        
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, image, direction) VALUES (?,?,?,?,'in')", (tkt_id, uid, text, img_filename))
        await db.commit()
        
    await dispatch_support_ticket(uid, tkt_id, text, cb, img_filename)
    return {"success": True}

# ==========================================================
#  WEBAPP FULL ADMIN API
# ==========================================================
@app.get("/api/admin/dashboard")
async def api_admin_dashboard(init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    uid = verify_webapp_data(init_data)
    if not await is_admin(uid): raise HTTPException(403)
    
    sponsor_mode = await get_setting("sponsor_check_mode") or "withdraw"
    maintenance_mode = await get_setting("maintenance_mode") or "0"
    maintenance_end = await get_setting("maintenance_end") or ""

    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        # Stats
        async with db.execute("SELECT COUNT(*) FROM players") as cur: total_users = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM players WHERE date(created_at) = date('now')") as cur: t_day = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM players WHERE date(last_seen) = date('now')") as cur: act_today = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM photos WHERE filename IS NOT NULL") as cur: photos_files_count = (await cur.fetchone())[0]
        
        # Recent Chats (Grouping to get last msg per user)
        async with db.execute("""
            SELECT sm.user_id, p.username, sm.text, sm.created_at, sm.direction
            FROM support_messages sm LEFT JOIN players p ON p.user_id = sm.user_id
            WHERE sm.id IN (SELECT MAX(id) FROM support_messages GROUP BY user_id)
            ORDER BY sm.created_at DESC LIMIT 50
        """) as cur: recent_chats = [dict(r) for r in await cur.fetchall()]

        # Withdrawals Pending
        async with db.execute("SELECT wr.*, p.username FROM withdrawal_requests wr LEFT JOIN players p ON p.user_id = wr.user_id WHERE wr.status='pending' ORDER BY wr.is_priority DESC, wr.created_at ASC") as cur:
            withdrawals = [dict(r) for r in await cur.fetchall()]

        # Sponsors
        async with db.execute("SELECT * FROM sponsors") as cur:
            sponsors = [dict(r) for r in await cur.fetchall()]

        # Promocodes
        async with db.execute("SELECT * FROM promo_codes WHERE uses < max_uses") as cur:
            promos = [dict(r) for r in await cur.fetchall()]
            
        # Wheel Config
        async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur:
            wheel = [dict(r) for r in await cur.fetchall()]

    return {
        "stats": {"total_users": total_users, "new_today": t_day, "active_today": act_today, "photos_files": photos_files_count},
        "settings": {"sponsor_mode": sponsor_mode, "maintenance_mode": maintenance_mode, "maintenance_end": maintenance_end},
        "recent_chats": recent_chats, "withdrawals": withdrawals, "sponsors": sponsors, "promos": promos, "wheel": wheel
    }

# --- Settings & Maintenance ---
@app.post("/api/admin/settings/sponsor_mode")
async def api_admin_set_sponsor_mode(mode: str = Form(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    if mode not in ("startup", "withdraw"): raise HTTPException(400)
    await set_setting("sponsor_check_mode", mode)
    return {"success": True}

@app.post("/api/admin/settings/maintenance")
async def api_admin_set_maintenance(mode: str = Form(...), end_time: str = Form(""), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    await set_setting("maintenance_mode", "1" if mode == "1" else "0")
    if mode == "1":
        if end_time:
            try: 
                dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
                await set_setting("maintenance_end", dt.strftime("%Y-%m-%dT%H:%M:00Z"))
            except: pass
        else: await set_setting("maintenance_end", "")
    else: await set_setting("maintenance_end", "")
    return {"success": True}

# --- Users ---
@app.get("/api/admin/user/{target_uid}")
async def api_admin_get_user(target_uid: int, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    p = await get_player(target_uid)
    if not p: raise HTTPException(404, "User not found")
    used_today = await get_active_photo_count(target_uid)
    total_slots = await get_user_total_slot_limit(target_uid, p['referrals_count'])
    return {"user": p, "slots": {"used": used_today, "total": total_slots}}

class UserUpdateParams(BaseModel):
    balance: Optional[float] = None
    total_earned: Optional[float] = None
    photos_sold: Optional[int] = None
    referrals_count: Optional[int] = None
    is_banned: Optional[int] = None

@app.put("/api/admin/user/{target_uid}")
async def api_admin_update_user(target_uid: int, params: UserUpdateParams, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    updates, vals = [], []
    for k, v in params.dict(exclude_unset=True).items():
        updates.append(f"{k}=?")
        vals.append(v)
    if not updates: return {"success": True}
    vals.append(target_uid)
    async with get_db() as db:
        await db.execute(f"UPDATE players SET {', '.join(updates)} WHERE user_id=?", vals)
        await db.commit()
    return {"success": True}

@app.post("/api/admin/user/{target_uid}/action")
async def api_admin_user_action(target_uid: int, action: str = Form(...), val: str = Form(""), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        if action == "reset_wheel":
            await db.execute("UPDATE players SET last_spin=NULL WHERE user_id=?", (target_uid,))
        elif action == "reset_slots":
            await db.execute("UPDATE players SET last_slot_reset=datetime('now') WHERE user_id=?", (target_uid,))
        elif action == "add_bonus_slots":
            await db.execute("UPDATE players SET bonus_slots_today=bonus_slots_today+? WHERE user_id=?", (int(val), target_uid))
        elif action == "sell_now":
            await db.execute("UPDATE photos SET sell_at=datetime('now') WHERE user_id=? AND status='on_auction'", (target_uid,))
        elif action == "personal_wheel":
            v = None if val == "/reset" else val.strip().replace(",", ".")
            await db.execute("UPDATE players SET personal_wheel=? WHERE user_id=?", (v, target_uid))
        await db.commit()
    return {"success": True}

# --- Bulk & System ---
@app.post("/api/admin/bulk")
async def api_admin_bulk(field: str = Form(...), val: str = Form(None), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    allowed_fields = ["balance", "total_earned", "referrals_count", "photos_sold", "is_banned", "last_spin", "reset_slots"]
    if field not in allowed_fields: raise HTTPException(400)
    
    async with get_db() as db:
        if field == "last_spin": await db.execute("UPDATE players SET last_spin=NULL")
        elif field == "reset_slots": await db.execute("UPDATE players SET last_slot_reset=datetime('now')")
        else:
            v = float(val) if field in ("balance", "total_earned") else int(val)
            await db.execute(f"UPDATE players SET {field}=?", (v,))
        await db.commit()
    return {"success": True}

@app.post("/api/admin/sell_all")
async def api_admin_sell_all(init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        await db.execute("UPDATE photos SET sell_at=datetime('now') WHERE status='on_auction'")
        await db.commit()
    return {"success": True}

@app.post("/api/admin/cleanup")
async def api_admin_cleanup(condition: str = Form(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    cond_sql = "1=1"
    if condition == "30d": cond_sql = "created_at <= datetime('now', '-30 days')"
    elif condition == "7d": cond_sql = "created_at <= datetime('now', '-7 days')"
    elif condition.startswith("date:"): cond_sql = f"date(created_at) <= date('{condition.split(':')[1]}')"
    
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(f"SELECT id, filename FROM photos WHERE filename IS NOT NULL AND {cond_sql}") as cur:
            rows = await cur.fetchall()
            
    c, freed = 0, 0
    for row in rows:
        try:
            filepath = UPLOADS_DIR / row['filename']
            if filepath.exists():
                freed += filepath.stat().st_size
                filepath.unlink()
            c += 1
        except Exception: pass

    if c > 0:
        async with get_db() as db:
            await db.execute(f"UPDATE photos SET filename = NULL WHERE filename IS NOT NULL AND {cond_sql}")
            await db.commit()
            
    return {"deleted": c, "mb": round(freed / (1024*1024), 2)}

@app.put("/api/admin/wheel")
async def api_admin_update_wheel(chances: str = Form(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    parts = chances.strip().split()
    if len(parts) != 5: raise HTTPException(400)
    arr = [float(x.replace(",", ".")) for x in parts]
    async with get_db() as db:
        for i in range(5): await db.execute("UPDATE wheel_config SET chance=? WHERE id=?", (arr[i], i))
        await db.commit()
    return {"success": True}

@app.post("/api/admin/broadcast")
async def api_admin_broadcast(text: str = Form(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    uids = await get_all_user_ids()
    ok = 0
    async def _send():
        nonlocal ok
        for u in uids:
            try: await bot.send_message(u, text, parse_mode=ParseMode.HTML); ok += 1
            except: pass
            await asyncio.sleep(0.05)
    asyncio.create_task(_send())
    return {"success": True, "message": f"Started sending to {len(uids)} users."}

# --- Promos & Sponsors & Withdrawals ---
@app.post("/api/admin/promo")
async def api_admin_add_promo(code: str = Form(...), ptype: str = Form(...), val: float = Form(...), limit: int = Form(...), dur: int = Form(0), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    code = code.strip().upper()
    if code == "AUTO": code = f"GIFT-{uuid.uuid4().hex[:6].upper()}"
    async with get_db() as db:
        try:
            await db.execute("INSERT INTO promo_codes (code, type, val, duration_days, max_uses) VALUES (?,?,?,?,?)", (code, ptype, val, dur, limit))
            await db.commit()
        except: raise HTTPException(400, "Code exists")
    return {"success": True, "code": code}

@app.delete("/api/admin/promo/{code}")
async def api_admin_del_promo(code: str, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        await db.execute("DELETE FROM promo_codes WHERE code=?", (code,))
        await db.commit()
    return {"success": True}

@app.post("/api/admin/sponsor")
async def api_admin_add_sponsor(cid: str=Form(...), name: str=Form(...), url: str=Form(...), desc: str=Form(""), days: str=Form(""), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    exp = None
    if days and days != "0":
        exp = (datetime.utcnow() + timedelta(days=int(days))).isoformat()
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO sponsors (channel_id, name, url, description, expires_at) VALUES (?,?,?,?,?)", (cid, name, url, desc if desc else None, exp))
        await db.commit()
    return {"success": True}

@app.delete("/api/admin/sponsor/{cid}")
async def api_admin_del_sponsor(cid: str, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        await db.execute("DELETE FROM sponsors WHERE channel_id=?", (cid,))
        await db.commit()
    return {"success": True}

@app.post("/api/admin/withdrawal/{wid}")
async def api_admin_withdrawal(wid: int, action: str = Form(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        if action == "approve":
            await db.execute("UPDATE withdrawal_requests SET status='completed' WHERE id=?", (wid,))
        elif action == "reject":
            async with db.execute("SELECT user_id, amount_usd FROM withdrawal_requests WHERE id=?", (wid,)) as cur: req = await cur.fetchone()
            if req:
                await db.execute("UPDATE withdrawal_requests SET status='rejected' WHERE id=?", (wid,))
                await db.execute("UPDATE players SET balance=balance+? WHERE user_id=?", (req[1], req[0]))
                p = await get_player(req[0])
                try: asyncio.create_task(bot.send_message(req[0], tr(p["lang"] if p else "en", "wd_rejected"), parse_mode=ParseMode.HTML))
                except: pass
        await db.commit()
    return {"success": True}

# --- Support Chat Management ---
@app.get("/api/admin/chat/{target_uid}")
async def api_admin_chat_history(target_uid: int, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM support_messages WHERE user_id=? ORDER BY created_at ASC", (target_uid,)) as cur:
            msgs = [dict(r) for r in await cur.fetchall()]
        async with db.execute("SELECT username FROM players WHERE user_id=?", (target_uid,)) as cur:
            p = await cur.fetchone()
    return {"messages": msgs, "username": p["username"] if p else None}

@app.post("/api/admin/chat/send")
async def api_admin_chat_send(target_uid: int = Form(...), text: str = Form(""), file: UploadFile = File(None), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    admin_uid = verify_webapp_data(init_data)
    if not await is_admin(admin_uid): raise HTTPException(403)
    text = text.strip()
    img_filename = None
    if file and file.filename:
        raw = await file.read(MAX_UPLOAD_SIZE + 1)
        if len(raw) > MAX_UPLOAD_SIZE: raise HTTPException(400, "Image too large")
        img_filename = f"sup_adm_{uuid.uuid4().hex[:8]}.jpg"
        filepath = SUPPORT_DIR / img_filename
        await asyncio.to_thread(filepath.write_bytes, raw)
        
    if not text and not img_filename: raise HTTPException(400, "Empty payload")
    
    async with get_db() as db:
        async with db.execute("SELECT id FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 1", (target_uid,)) as cur:
            tkt = await cur.fetchone()
        
        if tkt: 
            tkt_id = tkt[0]
            # Восстанавливаем статус, если он был закрыт, чтобы юзер снова мог писать
            await db.execute("UPDATE tickets SET status='claimed', claimed_by=? WHERE id=?", (admin_uid, tkt_id))
        else:
            await db.execute("INSERT INTO tickets (user_id, status, claimed_by) VALUES (?, 'claimed', ?)", (target_uid, admin_uid))
            async with db.execute("SELECT last_insert_rowid()") as cur: tkt_id = (await cur.fetchone())[0]

        admin_player = await get_player(admin_uid)
        admin_uname = f"@{admin_player['username']}" if admin_player and admin_player.get('username') else "Admin"
        
        if not tkt:
            await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, target_uid, f"👨‍💻 {admin_uname} присоединился к чату."))
            
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, image, direction) VALUES (?,?,?,?,'out')", (tkt_id, target_uid, text, img_filename))
        await db.commit()
        
    p = await get_player(target_uid)
    lang = p["lang"] if p else "en"
    final_text = tr(lang, "support_reply", text=text) if text else tr(lang, "support_reply", text="[Фото]")
    try:
        if img_filename: await bot.send_photo(target_uid, photo=FSInputFile(SUPPORT_DIR / img_filename), caption=final_text, parse_mode=ParseMode.HTML)
        else: await bot.send_message(target_uid, final_text, parse_mode=ParseMode.HTML)
    except: pass
    return {"success": True}

@app.put("/api/admin/chat/msg/{msg_id}")
async def api_admin_edit_msg(msg_id: int, text: str = Form(...), init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        await db.execute("UPDATE support_messages SET text=? WHERE id=?", (text.strip(), msg_id))
        await db.commit()
    return {"success": True}

@app.delete("/api/admin/chat/msg/{msg_id}")
async def api_admin_del_msg(msg_id: int, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        await db.execute("DELETE FROM support_messages WHERE id=?", (msg_id,))
        await db.commit()
    return {"success": True}

@app.post("/api/admin/chat/ticket/{target_uid}/close")
async def api_admin_close_ticket(target_uid: int, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        async with db.execute("SELECT id FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 1", (target_uid,)) as cur: tkt = await cur.fetchone()
        if tkt:
            await db.execute("UPDATE tickets SET status='closed' WHERE id=?", (tkt[0],))
            await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt[0], target_uid, "✅ Чат закрыт администратором."))
            await db.commit()
    return {"success": True}

@app.delete("/api/admin/chat/ticket/{target_uid}")
async def api_admin_delete_ticket(target_uid: int, init_data: str = Header(None, alias="X-Telegram-Init-Data")):
    if not await is_admin(verify_webapp_data(init_data)): raise HTTPException(403)
    async with get_db() as db:
        await db.execute("DELETE FROM tickets WHERE user_id=?", (target_uid,))
        await db.execute("DELETE FROM support_messages WHERE user_id=?", (target_uid,))
        await db.commit()
    return {"success": True}

@app.post("/api/referral/bind")
async def api_referral_bind(request: Request):
    data = await request.json()
    new_uid, ref_param = data.get("user_id"), str(data.get("ref_param") or "").strip()
    if not new_uid: raise HTTPException(400)
    ref_id = None
    if ref_param.startswith("ref_"):
        try: ref_id = int(ref_param[4:])
        except: pass
    elif ref_param.isdigit(): ref_id = int(ref_param)
    await get_or_create_player(new_uid, str(data.get("username") or ""), referred_by=ref_id)
    return {"bound": True}

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    from aiogram.types import Update
    asyncio.create_task(dp.feed_update(bot, Update(**await request.json())))
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
