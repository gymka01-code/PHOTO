import asyncio
import logging
import os
import random
import urllib.parse
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
from typing import List
from pydantic import BaseModel

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    Message, WebAppInfo, FSInputFile
)
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

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
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
SPONSORS_DIR.mkdir(parents=True, exist_ok=True)
SUPPORT_DIR.mkdir(parents=True, exist_ok=True)

_BOT_USERNAME_CACHE = os.path.join(_VOLUME, ".bot_username")

RUB_TO_USD_RATE = 92.0
COMMISSION_PCT  = 0.02
SINGLE_MIN_RUB, SINGLE_MAX_RUB = 92, 460
PACK_MIN_RUB, PACK_MAX_RUB, PACK_SIZE = 500, 2500, 5
MIN_REFERRALS_WITHDRAW = 3
MIN_DELAY_SECS = 30
MAX_UPLOAD_SIZE = 10 * 1024 * 1024

VIP_TIERS = [
    (0,  43200,  3), (3,  39600,  6), (5,  36000,  9),
    (10, 32400, 12), (25, 28800, 15), (50, 25200, 18),
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

class AdminPanel(StatesGroup):
    wait_sponsor_forward = State()
    wait_sponsor_desc = State()
    wait_sponsor_days = State()
    wait_edit_sp_name = State()
    wait_edit_sp_desc = State()
    wait_edit_sp_days = State()
    wait_user_id_search = State()
    wait_edit_user_balance = State()
    wait_edit_user_slots = State()
    wait_edit_user_refs = State()
    wait_edit_user_earned = State()
    wait_edit_user_vip = State()
    wait_edit_user_sold = State()
    wait_bonus_slots = State()
    wait_broadcast = State()
    wait_new_admin_id = State()
    wait_wheel_global = State()
    wait_wheel_personal = State()
    wait_bulk_field_value = State()
    wait_maintenance_time = State()
    wait_promo_type = State()
    wait_promo_val = State()
    wait_promo_limit = State()
    wait_promo_duration = State()
    wait_promo_code = State()
    wait_cleanup_date = State()

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

def rub_to_usd(rub: float) -> float:
    return round(rub / RUB_TO_USD_RATE, 2)

def apply_commission(usd: float) -> float:
    return round(usd * (1 - COMMISSION_PCT), 2)

def vip_level(refs: int) -> int:
    for i in range(len(VIP_TIERS)-1, -1, -1):
        if refs >= VIP_TIERS[i][0]: return i
    return 0

def vip_max_delay(refs: int) -> int:
    return VIP_TIERS[vip_level(refs)][1]

def vip_slot_limit(refs: int) -> int:
    return VIP_TIERS[vip_level(refs)][2]

def usd_to_stars(usd: float) -> int:
    return math.floor(usd / 0.012)

def make_share_url(ref_url: str) -> str:
    text = "Твоя камера теперь печатает деньги. 🖼💰\nЗалетай по моей ссылке: 🔗"
    return f"https://t.me/share/url?url={urllib.parse.quote(ref_url, safe='')}&text={urllib.parse.quote(text, safe='')}"

def parse_sqlite_date(date_str: str) -> datetime | None:
    if not date_str: return None
    clean_str = date_str.split('.')[0].replace('Z', '').replace('T', ' ')
    try:
        return datetime.strptime(clean_str, "%Y-%m-%d %H:%M:%S")
    except:
        return None

# ═══════════════════════════════════════════════════════════════
#  SECURITY & DB
# ═══════════════════════════════════════════════════════════════
def verify_webapp_data(init_data: str) -> int:
    if not init_data: raise HTTPException(401, "Missing Telegram Init Data")
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

def get_db():
    return aiosqlite.connect(DB_PATH, timeout=20.0)

async def init_db():
    async with get_db() as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")

        await db.execute("""CREATE TABLE IF NOT EXISTS players (user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0.0, total_earned REAL DEFAULT 0.0, photos_sold INTEGER DEFAULT 0, referrals_count INTEGER DEFAULT 0, referred_by INTEGER DEFAULT NULL, lang TEXT DEFAULT 'en', last_seen TEXT DEFAULT (datetime('now')), created_at TEXT DEFAULT (datetime('now')))""")
        
        for col in ["extra_slots INTEGER DEFAULT 0", "last_spin TEXT DEFAULT NULL", "is_banned INTEGER DEFAULT 0", "personal_wheel TEXT DEFAULT NULL", "last_slot_reset TEXT DEFAULT NULL", "bonus_slots_today INTEGER DEFAULT 0"]:
            try: await db.execute(f"ALTER TABLE players ADD COLUMN {col}")
            except Exception: pass
            
        await db.execute("""CREATE TABLE IF NOT EXISTS user_slots (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, is_permanent INTEGER DEFAULT 0, expires_at TEXT)""")
        
        async with db.execute("SELECT user_id, extra_slots FROM players WHERE extra_slots > 0") as cur:
            rows = await cur.fetchall()
            for r in rows:
                for _ in range(r[1]):
                    await db.execute("INSERT INTO user_slots (user_id, is_permanent) VALUES (?, 1)", (r[0],))
            if rows:
                await db.execute("UPDATE players SET extra_slots = 0")
                
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
#  ОСТАЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════
async def notify_referrer(referrer_id: int, new_user_name: str):
    try:
        await bot.send_message(referrer_id, f"🔔 <b>Новый реферал!</b>\nПользователь @{new_user_name} присоединился по вашей ссылке.", parse_mode=ParseMode.HTML)
    except:
        pass

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
            await db.execute("UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?", (referrer_id,))
            await db.commit()

        p = await get_player(new_user_id)
        display = f"@{p['username']}" if p and p.get("username") else str(new_user_id)
        asyncio.create_task(notify_referrer(referrer_id, display))
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
            async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur:
                return dict(await cur.fetchone()), True
                
        if username and username != (row["username"] or ""):
            await db.execute("UPDATE players SET username=? WHERE user_id=?", (username, user_id))
            await db.commit()
            return dict(row) | {"username": username}, False
        return dict(row), False

async def get_player(user_id: int) -> dict | None:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id=?", (user_id,)) as cur:
            r = await cur.fetchone()
        return dict(r) if r else None

async def get_player_photos(user_id: int, lang: str = "en") -> list:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM photos WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (user_id,)) as cur:
            rows = await cur.fetchall()
    return [dict(r) | {"status_label": tr(lang, "status_auction") if r["status"] == "on_auction" else ""} for r in rows]

async def get_active_photo_count(user_id: int) -> int:
    async with get_db() as db:
        async with db.execute("""
            SELECT COUNT(*) FROM photos 
            WHERE user_id=? 
            AND date(datetime(created_at, '+3 hours')) = date(datetime('now', '+3 hours'))
            AND created_at >= IFNULL((SELECT last_slot_reset FROM players WHERE user_id=?), '1970-01-01')
        """, (user_id, user_id)) as cur:
            return (await cur.fetchone())[0]

async def get_user_total_slot_limit(user_id: int, refs: int) -> int:
    base = vip_slot_limit(refs)
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM user_slots WHERE user_id=? AND (is_permanent=1 OR expires_at > datetime('now'))", (user_id,)) as cur:
            extra = (await cur.fetchone())[0]
        async with db.execute("SELECT bonus_slots_today FROM players WHERE user_id=?", (user_id,)) as cur:
            bonus_row = await cur.fetchone()
            bonus = bonus_row[0] if bonus_row and bonus_row[0] else 0
    return base + extra + bonus

async def get_referral_count(user_id: int) -> int:
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM players WHERE referred_by=?", (user_id,)) as cur:
            return (await cur.fetchone())[0]

async def get_all_user_ids() -> list[int]:
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM players") as cur:
            return [r[0] for r in await cur.fetchall()]

async def get_referral_list(referrer_id: int) -> list:
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT r.referred_id, r.created_at, p.username, CASE WHEN p.photos_sold > 0 THEN 1 ELSE 0 END AS is_active FROM referrals r LEFT JOIN players p ON p.user_id = r.referred_id WHERE r.referrer_id = ? ORDER BY r.created_at DESC", (referrer_id,)) as cur:
            return [dict(r) for r in await cur.fetchall()]

async def is_admin(user_id: int) -> bool:
    if ADMIN_ID and user_id == ADMIN_ID: return True
    async with get_db() as db:
        async with db.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) as cur:
            return bool(await cur.fetchone())

async def get_admin_ids() -> set[int]:
    ids = {ADMIN_ID} if ADMIN_ID else set()
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM admins") as cur:
            ids.update(r[0] for r in await cur.fetchall())
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
    except Exception:
        return False

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

try:
    _bot_username = Path(_BOT_USERNAME_CACHE).read_text().strip()
except:
    _bot_username = None

async def dispatch_support_ticket(uid: int, tkt_id: int, text: str, claimed_by: int = None, img_name: str = None):
    p = await get_player(uid)
    uname = f"@{p['username']}" if p and p.get('username') else f"ID {uid}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔒 Закрыть тикет", callback_data=f"crm_tclose:{tkt_id}")]])

    caption = f"💬 <b>Тикет #{tkt_id}</b> | {uname}\n\n{text}"
    target_ids = [claimed_by] if claimed_by else await get_admin_ids()

    for aid in target_ids:
        try: 
            if img_name:
                await bot.send_photo(aid, photo=FSInputFile(SUPPORT_DIR / img_name), caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb)
            else:
                await bot.send_message(aid, caption, parse_mode=ParseMode.HTML, reply_markup=kb)
        except: pass

async def auction_worker():
    while True:
        try:
            notifications = []
            async with get_db() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM photos WHERE status='on_auction' AND sell_at<=?", (datetime.utcnow().isoformat(),)) as cur:
                    due = await cur.fetchall()

                for ph in due:
                    buyer, sale_rub = random.choice(FAKE_USERS), ph["sale_rub"] or random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
                    gross = rub_to_usd(float(sale_rub))
                    net = apply_commission(gross)
                    
                    await db.execute("UPDATE photos SET status='sold', sold_at=datetime('now'), buyer=?, final_price=?, sale_rub=? WHERE id=?", (buyer, net, sale_rub, ph["id"]))
                    await db.execute("UPDATE players SET balance=balance+?, total_earned=total_earned+?, photos_sold=photos_sold+1 WHERE user_id=?", (net, net, ph["user_id"]))
                    
                    async with db.execute("SELECT balance, lang FROM players WHERE user_id=?", (ph["user_id"],)) as cur:
                        if p_row := await cur.fetchone():
                            notifications.append({"uid": ph["user_id"], "lang": p_row["lang"], "rub": sale_rub, "gross": gross, "net": net, "buyer": buyer, "bal": p_row["balance"]})
                if due: await db.commit()

            for n in notifications:
                try: await bot.send_message(n["uid"], tr(n["lang"], "sold", rub=int(n["rub"]), gross=n["gross"], commission=round(n["gross"]-n["net"],2), net=n["net"], buyer=n["buyer"], balance=round(n["bal"], 2)), parse_mode=ParseMode.HTML)
                except: pass
                await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"auction_worker error: {e}")
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
        if now >= next_run:
            next_run += timedelta(days=1)
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
        except Exception as e:
            logger.error(f"monitor_withdrawals_worker error: {e}")

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

# Универсальная отправка или редактирование (возврат в меню)
async def send_or_edit(obj, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    if isinstance(obj, CallbackQuery):
        try:
            await obj.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            pass
    else:
        await obj.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)

@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user, args_str = message.from_user, (command.args or "").strip()
    
    if not await is_admin(user.id):
        if await get_setting("maintenance_mode") == "1":
            end_time = await get_setting("maintenance_end")
            text = "🛠 <b>Идут технические работы!</b>\nБот временно недоступен. Пожалуйста, подождите."
            if end_time:
                text += f"\nОриентировочное время окончания: {end_time.replace('T', ' ').replace('Z', '')} UTC"
            return await message.answer(text, parse_mode=ParseMode.HTML)
            
    referrer_id = None
    if args_str:
        try: referrer_id = int(args_str[4:] if args_str.startswith("ref_") else args_str)
        except ValueError: pass

    p, _ = await get_or_create_player(user.id, user.username or "", referred_by=referrer_id)

    if p.get("is_banned"):
        return await message.answer("🚫 <b>Ваш аккаунт заблокирован.</b>\nЕсли вы считаете это ошибкой, просто напишите сообщение сюда, и оно будет доставлено в службу поддержки.", parse_mode=ParseMode.HTML)

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
    if not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, cb.from_user.id):
        return await cb.answer("❌ You haven't subscribed yet!", show_alert=True)
    await cb.answer("✅ Subscription confirmed!")
    try: await cb.message.delete()
    except: pass
    p = await get_player(cb.from_user.id)
    if p: await _process_start(cb.message, cb.from_user.id, p)

# ═══════════════════════════════════════════════════════════════
#  АДМИН ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════════
@dp.message(Command("admin"))
@dp.message(Command("panel"))
async def cmd_admin(obj, state: FSMContext=None):
    uid = obj.from_user.id
    if not await is_admin(uid): return
    if state: await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="crm_stats")],
        [InlineKeyboardButton(text="👤 Юзеры", callback_data="crm_users"), InlineKeyboardButton(text="🎧 Тикеты", callback_data="crm_tickets")],
        [InlineKeyboardButton(text="🤝 Спонсоры", callback_data="crm_sponsors"), InlineKeyboardButton(text="💳 Выводы", callback_data="crm_wd")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="crm_broadcast"), InlineKeyboardButton(text="👮 Админы", callback_data="crm_admins")],
        [InlineKeyboardButton(text="🎁 Промокоды", callback_data="crm_promo")],
        [InlineKeyboardButton(text="⚙️ Изменить параметры всех", callback_data="crm_bulk")],
        [InlineKeyboardButton(text="🧹 Очистка памяти (Фото)", callback_data="crm_cleanup_menu")],
        [InlineKeyboardButton(text="🛠 Тех. работы", callback_data="crm_maintenance")],
    ])
    await send_or_edit(obj, "👑 <b>Админ Панель CRM</b>\nВыберите раздел:", reply_markup=kb)

@dp.callback_query(F.data == "crm_main")
async def cq_crm_main(cb: CallbackQuery, state: FSMContext):
    await cmd_admin(cb, state)

# ═════ ОЧИСТКА ПАМЯТИ ═════
@dp.callback_query(F.data == "crm_cleanup_menu")
async def cq_cleanup_menu(obj):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Старше 30 дней", callback_data="crm_cleanup:30d")],
        [InlineKeyboardButton(text="🗑 Старше 7 дней", callback_data="crm_cleanup:7d")],
        [InlineKeyboardButton(text="📅 До определенной даты", callback_data="crm_cleanup:date")],
        [InlineKeyboardButton(text="🧨 Удалить ВСЕ файлы фото", callback_data="crm_cleanup_confirm_all")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]
    ])
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE filename IS NOT NULL") as cur:
            c = (await cur.fetchone())[0]
    await send_or_edit(obj, f"🧹 <b>Очистка памяти сервера</b>\n\nСейчас фотографий на сервере (с файлами): <b>{c}</b>\n\nУдаление файлов не ломает аккаунты, вместо фото пользователи будут видеть заглушку (🖼).\n\nВыберите фильтр удаления:", reply_markup=kb)

async def perform_cleanup(condition_sql: str, params: tuple):
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(f"SELECT id, filename FROM photos WHERE filename IS NOT NULL AND {condition_sql}", params) as cur:
            rows = await cur.fetchall()

    deleted_count = 0
    freed_bytes = 0
    for row in rows:
        try:
            filepath = UPLOADS_DIR / row['filename']
            if filepath.exists():
                freed_bytes += filepath.stat().st_size
                filepath.unlink()
            deleted_count += 1
        except Exception: pass

    if deleted_count > 0:
        async with get_db() as db:
            await db.execute(f"UPDATE photos SET filename = NULL WHERE filename IS NOT NULL AND {condition_sql}", params)
            await db.commit()

    return deleted_count, freed_bytes / (1024 * 1024)

@dp.callback_query(F.data.startswith("crm_cleanup:"))
async def cq_cleanup_action(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split(":")[1]
    if action == "30d":
        c, mb = await perform_cleanup("created_at <= datetime('now', '-30 days')", ())
        await cb.answer(f"Удалено {c} файлов. Освобождено {mb:.2f} MB", show_alert=True)
        await cq_cleanup_menu(cb)
    elif action == "7d":
        c, mb = await perform_cleanup("created_at <= datetime('now', '-7 days')", ())
        await cb.answer(f"Удалено {c} файлов. Освобождено {mb:.2f} MB", show_alert=True)
        await cq_cleanup_menu(cb)
    elif action == "date":
        await state.set_state(AdminPanel.wait_cleanup_date)
        await cb.message.edit_text("📅 <b>Удаление до конкретной даты</b>\n\nВведите дату в формате <b>ГГГГ-ММ-ДД</b> (например: 2024-05-15).\n\nБудут удалены все файлы фото, загруженные до этой даты включительно.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_cleanup_menu")]]))

@dp.message(AdminPanel.wait_cleanup_date)
async def cleanup_date_save(m: Message, state: FSMContext):
    try:
        dt = datetime.strptime(m.text.strip(), "%Y-%m-%d").date()
        c, mb = await perform_cleanup("date(created_at) <= date(?)", (dt.isoformat(),))
        await m.answer(f"✅ Успешно!\nУдалено <b>{c}</b> файлов.\nОсвобождено <b>{mb:.2f} MB</b> памяти.", parse_mode=ParseMode.HTML)
        await state.clear()
        await cq_cleanup_menu(m)
    except Exception:
        await m.answer("❌ Ошибка формата. Пожалуйста, используйте формат ГГГГ-ММ-ДД (например 2024-05-15).")

@dp.callback_query(F.data == "crm_cleanup_confirm_all")
async def cq_cleanup_all(cb: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚨 Да, удалить вообще ВСЕ фото", callback_data="crm_cleanup_do_all")],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_cleanup_menu")]
    ])
    await cb.message.edit_text("🧨 <b>ВНИМАНИЕ!</b>\n\nВы собираетесь удалить файлы <b>ВСЕХ</b> загруженных фотографий за всё время.\nВы уверены?", parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data == "crm_cleanup_do_all")
async def cq_cleanup_do_all(cb: CallbackQuery):
    c, mb = await perform_cleanup("1=1", ())
    await send_or_edit(cb, f"✅ <b>Полная очистка завершена!</b>\n\nУдалено файлов: <b>{c}</b>\nОсвобождено: <b>{mb:.2f} MB</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="crm_cleanup_menu")]]))

# ═════ ТЕХ РАБОТЫ ═════
@dp.callback_query(F.data == "crm_maintenance")
async def cq_maintenance(obj):
    mode = await get_setting("maintenance_mode")
    end_time = await get_setting("maintenance_end")
    status = "ВКЛЮЧЕНЫ 🔴" if mode == "1" else "ВЫКЛЮЧЕНЫ 🟢"
    text = f"🛠 <b>Технические работы</b>\n\nСтатус: <b>{status}</b>\nОкончание: <b>{end_time.replace('T', ' ').replace('Z', '') if end_time else 'Не задано'}</b>\n\n<i>Админы имеют доступ к боту всегда.</i>"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Включить 🔴" if mode == "0" else "Выключить 🟢", callback_data="crm_maint_toggle")],
        [InlineKeyboardButton(text="⏳ Установить время окончания", callback_data="crm_maint_time")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]
    ])
    await send_or_edit(obj, text, reply_markup=kb)

@dp.callback_query(F.data == "crm_maint_toggle")
async def cq_maint_toggle(cb: CallbackQuery):
    mode = await get_setting("maintenance_mode")
    await set_setting("maintenance_mode", "0" if mode == "1" else "1")
    await cq_maintenance(cb)

@dp.callback_query(F.data == "crm_maint_time")
async def cq_maint_time(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_maintenance_time)
    await cb.message.edit_text("⏳ <b>Время окончания тех. работ</b>\n\nВведите дату и время по UTC (Лондон).\nФормат: <b>ГГГГ-ММ-ДД ЧЧ:ММ</b>\n<i>Пример: 2024-06-15 14:30</i>\n\nИли отправьте <code>/clear</code> чтобы убрать таймер.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_maintenance")]]))

@dp.message(AdminPanel.wait_maintenance_time)
async def maint_time_save(m: Message, state: FSMContext):
    txt = m.text.strip()
    if txt == "/clear":
        await set_setting("maintenance_end", "")
    else:
        try:
            dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
            await set_setting("maintenance_end", dt.strftime("%Y-%m-%dT%H:%M:00Z"))
        except:
            return await m.answer("❌ Неверный формат! Используйте ГГГГ-ММ-ДД ЧЧ:ММ")
    await state.clear()
    await m.answer("✅ Время сохранено.")
    await cq_maintenance(m)

# ═════ ПРОМОКОДЫ ═════
@dp.callback_query(F.data == "crm_promo")
async def cq_promo(obj):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="crm_promo_add")],
        [InlineKeyboardButton(text="📋 Активные промокоды", callback_data="crm_promo_list")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]
    ])
    await send_or_edit(obj, "🎁 <b>Управление Промокодами</b>", reply_markup=kb)

@dp.callback_query(F.data == "crm_promo_list")
async def cq_promo_list(obj):
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM promo_codes WHERE uses < max_uses") as cur:
            promos = await cur.fetchall()
            
    if not promos:
        return await send_or_edit(obj, "Нет активных промокодов.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_promo")]]))
        
    text = "📋 <b>Активные Промокоды:</b>\n\n"
    kb_list = []
    for p in promos:
        typ = "USD" if p['type'] == 'usd' else "Прокрутка" if p['type'] == 'spin' else "Слоты"
        val = f"${p['val']}" if p['type'] == 'usd' else int(p['val'])
        dur = f"({p['duration_days']} дн.)" if p['type'] == 'slot' and p['duration_days'] > 0 else "(Навсегда)" if p['type'] == 'slot' else ""
        text += f"🏷 <code>{p['code']}</code>\nТип: <b>{typ}</b> | Награда: <b>{val}</b> {dur}\nИспользований: <b>{p['uses']} / {p['max_uses']}</b>\n\n"
        kb_list.append([InlineKeyboardButton(text=f"❌ Удалить {p['code']}", callback_data=f"crm_promo_del:{p['code']}")])
        
    kb_list.append([InlineKeyboardButton(text="🔙 Назад", callback_data="crm_promo")])
    await send_or_edit(obj, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))

@dp.callback_query(F.data.startswith("crm_promo_del:"))
async def cq_promo_del(cb: CallbackQuery):
    code = cb.data.split(":")[1]
    async with get_db() as db:
        await db.execute("DELETE FROM promo_codes WHERE code=?", (code,))
        await db.commit()
    await cb.answer(f"Удален: {code}", show_alert=True)
    await cq_promo_list(cb)

@dp.callback_query(F.data == "crm_promo_add")
async def cq_promo_add(cb: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Доллары на баланс", callback_data="crm_prt:usd")],
        [InlineKeyboardButton(text="🎡 Доп. прокрутка колеса", callback_data="crm_prt:spin")],
        [InlineKeyboardButton(text="🎰 Слоты (Временно)", callback_data="crm_prt:slot_temp")],
        [InlineKeyboardButton(text="🎰 Слоты (Навсегда)", callback_data="crm_prt:slot_perm")],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_promo")]
    ])
    await cb.message.edit_text("Выберите ТИП награды:", reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_prt:"))
async def cq_promo_type(cb: CallbackQuery, state: FSMContext):
    ptype = cb.data.split(":")[1]
    await state.update_data(promo_type=ptype)
    
    if ptype == "spin":
        await state.update_data(promo_val=1)
        await state.set_state(AdminPanel.wait_promo_limit)
        await cb.message.edit_text("Введите МАКСИМАЛЬНОЕ количество активаций (например: 100):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_promo")]]))
    else:
        await state.set_state(AdminPanel.wait_promo_val)
        hint = "СУММУ USD" if ptype == "usd" else "КОЛИЧЕСТВО СЛОТОВ"
        await cb.message.edit_text(f"Введите {hint} (число):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_promo")]]))

@dp.message(AdminPanel.wait_promo_val)
async def p_val(m: Message, state: FSMContext):
    try:
        val = float(m.text.replace(",", "."))
        await state.update_data(promo_val=val)
        
        ptype = (await state.get_data())['promo_type']
        if ptype == "slot_temp":
            await state.set_state(AdminPanel.wait_promo_duration)
            await m.answer("Введите КОЛИЧЕСТВО ДНЕЙ жизни слотов (например, 7):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_promo")]]))
        else:
            await state.set_state(AdminPanel.wait_promo_limit)
            await m.answer("Введите МАКСИМАЛЬНОЕ количество активаций (например: 100):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_promo")]]))
    except:
        await m.answer("Ошибка ввода. Введите число.")

@dp.message(AdminPanel.wait_promo_duration)
async def p_dur(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Целое число!")
    await state.update_data(promo_dur=int(m.text))
    await state.set_state(AdminPanel.wait_promo_limit)
    await m.answer("Введите МАКСИМАЛЬНОЕ количество активаций (например: 100):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_promo")]]))

@dp.message(AdminPanel.wait_promo_limit)
async def p_limit(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Целое число!")
    await state.update_data(promo_limit=int(m.text))
    await state.set_state(AdminPanel.wait_promo_code)
    await m.answer("Введите ТЕКСТ ПРОМОКОДА (напр: SUMMERY2024)\nИли отправьте <code>/auto</code> для случайного.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_promo")]]))

@dp.message(AdminPanel.wait_promo_code)
async def p_code(m: Message, state: FSMContext):
    code = m.text.strip().upper()
    if code == "/AUTO":
        code = f"GIFT-{uuid.uuid4().hex[:6].upper()}"
        
    data = await state.get_data()
    ptype = data['promo_type']
    val = data['promo_val']
    limit = data['promo_limit']
    dur = data.get('promo_dur', 0)
    
    db_type = "slot" if ptype.startswith("slot") else ptype
    
    async with get_db() as db:
        try:
            await db.execute("INSERT INTO promo_codes (code, type, val, duration_days, max_uses) VALUES (?,?,?,?,?)",
                             (code, db_type, val, dur, limit))
            await db.commit()
            await m.answer(f"✅ Промокод создан!\n\nКод: <code>{code}</code>\nЛимит: {limit}", parse_mode=ParseMode.HTML)
            await state.clear()
            await cq_promo_list(m)
        except Exception:
            await m.answer("❌ Промокод с таким текстом уже существует. Введите другой:")

# ═════ ОБЫЧНЫЕ СТАТИСТИКИ ═════
@dp.callback_query(F.data == "crm_stats")
async def cq_stats(obj):
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM players") as cur: total = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM players WHERE date(created_at) = date('now')") as cur: t_day = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM players WHERE created_at >= datetime('now', '-7 days')") as cur: t_week = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM players WHERE created_at >= datetime('now', '-30 days')") as cur: t_month = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM players WHERE date(last_seen) = date('now')") as cur: act_today = (await cur.fetchone())[0]

    text = f"📊 <b>Статистика пользователей</b>\n\n👥 Всего: <b>{total}</b>\n🔥 Активных сегодня (DAU): <b>{act_today}</b>\n\n📈 <b>Новые регистрации:</b>\n• За сегодня: <b>+{t_day}</b>\n• За 7 дней: <b>+{t_week}</b>\n• За 30 дней: <b>+{t_month}</b>"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Обновить", callback_data="crm_stats")], [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]])
    await send_or_edit(obj, text, reply_markup=kb)

@dp.callback_query(F.data == "crm_admins")
async def cq_admins(obj):
    async with get_db() as db:
        async with db.execute("SELECT user_id, username FROM admins") as cur: db_admins = await cur.fetchall()
    text = f"👮 <b>Администраторы:</b>\nГлавный админ: <code>{ADMIN_ID}</code>\n"
    for a in db_admins: text += f"• <code>{a[0]}</code> (@{a[1] or '?'})\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить", callback_data="crm_admin_add"), InlineKeyboardButton(text="❌ Удалить", callback_data="crm_admin_del_menu")], [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]])
    await send_or_edit(obj, text, reply_markup=kb)

@dp.callback_query(F.data == "crm_admin_add")
async def cq_admin_add(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_new_admin_id)
    await cb.message.edit_text("Отправьте Telegram ID нового администратора:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_admins")]]))

@dp.message(AdminPanel.wait_new_admin_id)
async def msg_admin_add(m: Message, state: FSMContext):
    if not m.text.strip().isdigit(): return await m.answer("❌ ID только из цифр.")
    new_id = int(m.text.strip())
    async with get_db() as db:
        async with db.execute("SELECT username FROM players WHERE user_id=?", (new_id,)) as cur:
            row = await cur.fetchone()
        await db.execute("INSERT OR REPLACE INTO admins (user_id, username, added_by) VALUES (?, ?, ?)", (new_id, row[0] if row else None, m.from_user.id))
        await db.commit()
    await state.clear()
    await m.answer(f"✅ Админ {new_id} добавлен.")
    await cq_admins(m)

@dp.callback_query(F.data == "crm_admin_del_menu")
async def cq_admin_del_menu(cb: CallbackQuery):
    async with get_db() as db:
        async with db.execute("SELECT user_id, username FROM admins") as cur: db_admins = await cur.fetchall()
    if not db_admins: return await cb.answer("Нет добавленных админов.", show_alert=True)
    kb = [[InlineKeyboardButton(text=f"❌ {a[0]} (@{a[1] or '?'})", callback_data=f"crm_admin_del:{a[0]}")] for a in db_admins] + [[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_admins")]]
    await cb.message.edit_text("Кого удалить?", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("crm_admin_del:"))
async def cq_admin_del(cb: CallbackQuery):
    async with get_db() as db:
        await db.execute("DELETE FROM admins WHERE user_id=?", (int(cb.data.split(":")[1]),))
        await db.commit()
    await cb.answer("Удален.")
    await cq_admins(cb) 

@dp.callback_query(F.data == "crm_users")
async def cq_crm_users(obj, state: FSMContext=None):
    if state: await state.set_state(AdminPanel.wait_user_id_search)
    await send_or_edit(obj, "👤 <b>Поиск пользователя</b>\nОтправьте ID:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]))

@dp.message(AdminPanel.wait_user_id_search)
async def admin_search_user(message: Message, state: FSMContext):
    if not message.text.strip().isdigit(): return await message.answer("❌ ID только из цифр.")
    uid = int(message.text.strip())
    if not await get_player(uid): return await message.answer("❌ Не найден.")
    await state.update_data(edit_user_id=uid)
    await show_user_control_panel(message, uid, state)

async def show_user_control_panel(obj, uid: int, state: FSMContext):
    p = await get_player(uid)
    used_today = await get_active_photo_count(uid)
    total_slots = await get_user_total_slot_limit(uid, p['referrals_count'])

    can_spin = "Да"
    ls_dt = parse_sqlite_date(p.get("last_spin"))
    if ls_dt and (ls_dt + timedelta(hours=24)) > datetime.utcnow():
        can_spin = "Нет (Кулдаун)"

    text = f"👤 <b>Аккаунт: <code>{uid}</code></b> {'(🚫 ЗАБЛОКИРОВАН)' if p.get('is_banned') else ''}\n"
    text += f"Имя: @{p.get('username') or 'Нет'}\nБаланс: <b>${p['balance']:.2f}</b>\nЗаработано: <b>${p['total_earned']:.2f}</b>\n"
    text += f"Продано фото: <b>{p['photos_sold']}</b>\n"
    text += f"Рефералы: <b>{p['referrals_count']}</b> (VIP {vip_level(p['referrals_count'])})\n"
    text += f"Слоты сегодня: <b>{used_today} / {total_slots}</b>\n"
    text += f"Колесо: <b>{can_spin}</b>\n"
    text += f"Персональная рулетка: <b>{'ВКЛ' if p.get('personal_wheel') else 'ВЫКЛ'}</b>"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Баланс", callback_data="c_usr_bal"), InlineKeyboardButton(text="📈 Заработано", callback_data="c_usr_earn")],
        [InlineKeyboardButton(text="📸 Продано фото", callback_data="c_usr_sold"), InlineKeyboardButton(text="🤝 Рефералы", callback_data="c_usr_ref")],
        [InlineKeyboardButton(text="🌟 Уровень VIP", callback_data="c_usr_vip"), InlineKeyboardButton(text="🎯 Рулетка", callback_data="c_usr_pwheel")],
        [InlineKeyboardButton(text="🎰 Настроить слоты", callback_data="c_usr_slots")],
        [InlineKeyboardButton(text="⚡ Продать фото сейчас", callback_data="c_usr_sellnow")],
        [InlineKeyboardButton(text="🔄 Сбросить Колесо", callback_data="c_usr_wheel"), InlineKeyboardButton(text="🔓 Разблок." if p.get('is_banned') else "🚫 Блок.", callback_data="c_usr_ban")],
        [InlineKeyboardButton(text="🔙 Назад к поиску", callback_data="crm_users")]
    ])
    await send_or_edit(obj, text, reply_markup=kb)

@dp.callback_query(F.data.startswith("c_usr_"))
async def cq_c_usr_actions(cb: CallbackQuery, state: FSMContext):
    uid = (await state.get_data()).get("edit_user_id")
    if not uid: return await cb.answer("⏳ Сессия устарела", show_alert=True)
    
    action = cb.data.split("_")[2]
    if action == "cancel": return await show_user_control_panel(cb, uid, state)
    if action == "wheel":
        async with get_db() as db:
            await db.execute("UPDATE players SET last_spin=NULL WHERE user_id=?", (uid,))
            await db.commit()
        await cb.answer("✅ Сброшен!")
        return await show_user_control_panel(cb, uid, state)
    if action == "sellnow":
        async with get_db() as db:
            async with db.execute("SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'", (uid,)) as cur:
                cnt = (await cur.fetchone())[0]
            await db.execute("UPDATE photos SET sell_at=datetime('now') WHERE user_id=? AND status='on_auction'", (uid,))
            await db.commit()
        await cb.answer(f"✅ КД сброшен! {cnt} фото продадутся в течение 15 сек.")
        return await show_user_control_panel(cb, uid, state)
    if action == "ban":
        p = await get_player(uid)
        async with get_db() as db:
            await db.execute("UPDATE players SET is_banned=? WHERE user_id=?", (0 if p.get('is_banned') else 1, uid))
            await db.commit()
        await cb.answer("✅ Статус изменен")
        return await show_user_control_panel(cb, uid, state)
    if action == "pwheel":
        await state.set_state(AdminPanel.wait_wheel_personal)
        return await cb.message.edit_text("🎯 <b>Персональная рулетка юзера</b>\n\nВведите 5 чисел через пробел (шансы на: $5 | НИЧЕГО | $20 | +2 СЛОТА | ДЖЕКПОТ).\nИли отправьте <code>/reset</code> для отключения.\n\nПример (Только джекпот): <code>0 0 0 0 100</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="c_usr_cancel")]]))

    if action == "slots":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Сбросить занятые (станет 0)", callback_data="c_usr_slotreset")],
            [InlineKeyboardButton(text="➕ Выдать бонусные слоты на сегодня", callback_data="c_usr_slotbonus")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="c_usr_cancel")]
        ])
        return await cb.message.edit_text("🎰 <b>Настройка слотов пользователя</b>", parse_mode=ParseMode.HTML, reply_markup=kb)

    if action == "slotreset":
        async with get_db() as db:
            await db.execute("UPDATE players SET last_slot_reset=datetime('now') WHERE user_id=?", (uid,))
            await db.commit()
        await cb.answer("✅ Дневной лимит пользователя сброшен до 0 использованных!")
        return await show_user_control_panel(cb, uid, state)
        
    if action == "slotbonus":
        await state.set_state(AdminPanel.wait_bonus_slots)
        return await cb.message.edit_text("Введите количество БОНУСНЫХ слотов (они сгорят в полночь):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="c_usr_cancel")]]))

    prompts = {
        "bal": ("wait_edit_user_balance", "Введите БАЛАНС (например 15.50):"),
        "earn": ("wait_edit_user_earned", "Введите ЗАРАБОТАНО (например 100.00):"),
        "sold": ("wait_edit_user_sold", "Введите количество ПРОДАННЫХ ФОТО (целое число):"),
        "ref": ("wait_edit_user_refs", "Введите РЕФЕРАЛОВ (целое число):"),
        "vip": ("wait_edit_user_vip", "Введите ЖЕЛАЕМЫЙ VIP УРОВЕНЬ (от 0 до 5):")
    }
    
    if action in prompts:
        await state.set_state(getattr(AdminPanel, prompts[action][0]))
        await cb.message.edit_text(prompts[action][1], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="c_usr_cancel")]]))

@dp.message(AdminPanel.wait_bonus_slots)
async def e_bonus_slots(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("❌ Целое число!")
    uid = (await state.get_data())["edit_user_id"]
    async with get_db() as db:
        await db.execute("UPDATE players SET bonus_slots_today=bonus_slots_today+? WHERE user_id=?", (int(m.text), uid))
        await db.commit()
    await m.answer("✅ Бонусные слоты выданы!")
    await show_user_control_panel(m, uid, state)

@dp.message(AdminPanel.wait_wheel_personal)
async def pwheel_save(m: Message, state: FSMContext):
    uid = (await state.get_data())["edit_user_id"]
    if m.text.strip().lower() == "/reset":
        async with get_db() as db:
            await db.execute("UPDATE players SET personal_wheel=NULL WHERE user_id=?", (uid,))
            await db.commit()
        await m.answer("✅ Отключено.")
        return await show_user_control_panel(m, uid, state)
    
    parts = m.text.strip().split()
    if len(parts) != 5: return await m.answer("❌ Должно быть ровно 5 чисел через пробел!")
    try:
        [float(x.replace(",",".")) for x in parts]
        async with get_db() as db:
            await db.execute("UPDATE players SET personal_wheel=? WHERE user_id=?", (" ".join(parts).replace(",","."), uid))
            await db.commit()
        await m.answer("✅ Сохранено.")
        await show_user_control_panel(m, uid, state)
    except: await m.answer("❌ Ошибка в числах.")

@dp.message(AdminPanel.wait_edit_user_balance)
async def e_bal(m: Message, state: FSMContext):
    try:
        val = float(m.text.replace(",", "."))
        if val < 0: raise ValueError()
        async with get_db() as db:
            await db.execute("UPDATE players SET balance=? WHERE user_id=?", (val, (await state.get_data())["edit_user_id"]))
            await db.commit()
        await show_user_control_panel(m, (await state.get_data())["edit_user_id"], state)
    except: await m.answer("❌ Число не может быть отрицательным!")

@dp.message(AdminPanel.wait_edit_user_earned)
async def e_earn(m: Message, state: FSMContext):
    try:
        val = float(m.text.replace(",", "."))
        if val < 0: raise ValueError()
        async with get_db() as db:
            await db.execute("UPDATE players SET total_earned=? WHERE user_id=?", (val, (await state.get_data())["edit_user_id"]))
            await db.commit()
        await show_user_control_panel(m, (await state.get_data())["edit_user_id"], state)
    except: await m.answer("❌ Ошибка ввода!")

@dp.message(AdminPanel.wait_edit_user_sold)
async def e_sold(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("❌ Целое число больше или равное 0.")
    val = int(m.text)
    uid = (await state.get_data())["edit_user_id"]
    async with get_db() as db:
        await db.execute("UPDATE players SET photos_sold=? WHERE user_id=?", (val, uid))
        await db.commit()
    await show_user_control_panel(m, uid, state)

@dp.message(AdminPanel.wait_edit_user_refs)
async def e_refs(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("❌ Целое число.")
    async with get_db() as db:
        await db.execute("UPDATE players SET referrals_count=? WHERE user_id=?", (int(m.text), (await state.get_data())["edit_user_id"]))
        await db.commit()
    await show_user_control_panel(m, (await state.get_data())["edit_user_id"], state)

@dp.message(AdminPanel.wait_edit_user_vip)
async def e_vip(m: Message, state: FSMContext):
    if not m.text.isdigit() or not (0 <= int(m.text) <= 5): return await m.answer("❌ Число от 0 до 5.")
    req_refs = VIP_TIERS[int(m.text)][0]
    async with get_db() as db:
        await db.execute("UPDATE players SET referrals_count=? WHERE user_id=?", (req_refs, (await state.get_data())["edit_user_id"]))
        await db.commit()
    await m.answer(f"✅ VIP изменен")
    await show_user_control_panel(m, (await state.get_data())["edit_user_id"], state)

# ═════ МАССОВОЕ ИЗМЕНЕНИЕ ПАРАМЕТРОВ ═════
BULK_FIELDS = {
    "balance": ("💰 Баланс", "REAL", "UPDATE players SET balance=? WHERE 1=1"),
    "total_earned": ("📈 Заработано (total_earned)", "REAL", "UPDATE players SET total_earned=? WHERE 1=1"),
    "referrals_count": ("🤝 Рефералы", "INT", "UPDATE players SET referrals_count=? WHERE 1=1"),
    "photos_sold": ("📸 Продано фото", "INT", "UPDATE players SET photos_sold=? WHERE 1=1"),
    "is_banned": ("🚫 Бан (0=нет, 1=да)", "INT", "UPDATE players SET is_banned=? WHERE 1=1"),
    "last_spin": ("🎡 Сбросить Колесо (введите NULL)", "NULL", "UPDATE players SET last_spin=NULL WHERE 1=1"),
    "reset_slots": ("🔄 Сбросить занятые слоты сегодня всем", "NULL", "UPDATE players SET last_slot_reset=datetime('now') WHERE 1=1")
}

@dp.callback_query(F.data == "crm_bulk")
async def call_bulk(cb: CallbackQuery, state: FSMContext):
    await cq_bulk(cb, state)

async def cq_bulk(obj, state: FSMContext=None):
    lines = "\n".join(f"• <code>{k}</code> — {v[0]}" for k, v in BULK_FIELDS.items())
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=v[0], callback_data=f"crm_bulk_f:{k}")] for k, v in BULK_FIELDS.items()
    ] + [
        [InlineKeyboardButton(text="🎡 Глобальная Рулетка (Шансы)", callback_data="crm_wheel")],
        [InlineKeyboardButton(text="⚡ Продать ВСЕ фото ВСЕМ", callback_data="crm_sellall_confirm")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]
    ])
    await send_or_edit(obj, f"⚙️ <b>Массовое изменение параметров</b>\n\nВыберите параметр для изменения у ВСЕХ аккаунтов:\n\n{lines}\n\n⚠️ <b>Действие применяется ко ВСЕМ пользователям!</b>", reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_bulk_f:"))
async def cq_bulk_field(cb: CallbackQuery, state: FSMContext):
    field = cb.data.split(":")[1]
    if field not in BULK_FIELDS: return await cb.answer("Ошибка")
    fname, ftype, _ = BULK_FIELDS[field]
    await state.update_data(bulk_field=field)
    if ftype == "NULL":
        async with get_db() as db:
            await db.execute(BULK_FIELDS[field][2])
            await db.commit()
        await cb.message.edit_text(f"✅ <b>{fname}</b> — выполнено для всех пользователей!", parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_bulk")]]))
        return
    hint = "число (дробное, напр. 15.50)" if ftype == "REAL" else "целое число"
    await state.set_state(AdminPanel.wait_bulk_field_value)
    await cb.message.edit_text(
        f"⚙️ <b>Изменение: {fname}</b>\n\nВведите новое значение ({hint}) для ВСЕХ аккаунтов.\n\nИли нажмите Отмена.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_bulk")]])
    )

@dp.message(AdminPanel.wait_bulk_field_value)
async def bulk_field_save(m: Message, state: FSMContext):
    field = (await state.get_data()).get("bulk_field")
    if not field or field not in BULK_FIELDS: return await m.answer("❌ Ошибка сессии.")
    fname, ftype, sql = BULK_FIELDS[field]
    try:
        if ftype == "REAL": val = float(m.text.replace(",", "."))
        else: val = int(m.text.strip())
        async with get_db() as db:
            await db.execute(sql, (val,))
            async with db.execute("SELECT COUNT(*) FROM players") as cur: total = (await cur.fetchone())[0]
            await db.commit()
        await state.clear()
        await m.answer(f"✅ Параметр <b>{fname}</b> установлен в <code>{val}</code> для всех <b>{total}</b> аккаунтов!", parse_mode=ParseMode.HTML)
        await cq_bulk(m, state)
    except ValueError:
        await m.answer("❌ Неверный формат. Введите число.")

# ═════ ГЛОБАЛЬНАЯ РУЛЕТКА (внутри bulk) ═════
@dp.callback_query(F.data == "crm_wheel")
async def call_wheel(cb: CallbackQuery):
    await cq_wheel(cb)

async def cq_wheel(obj):
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur:
            items = await cur.fetchall()
            
    text = "🎡 <b>Глобальные настройки Рулетки</b>\n\nТекущие шансы:\n"
    for it in items: text += f"• <b>{it['label']}</b> — {it['chance']}%\n"
    text += "\nЧтобы изменить, нажмите кнопку ниже и введите сразу 5 чисел (шансов) через пробел."
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✏️ Изменить всё сразу", callback_data="crm_wheel_edit")], [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_bulk")]])
    await send_or_edit(obj, text, reply_markup=kb)

@dp.callback_query(F.data == "crm_wheel_edit")
async def cq_wheel_edit(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_wheel_global)
    await cb.message.edit_text("Введите 5 чисел через пробел.\nПорядок: $5 | НИЧЕГО | $20 | +2 СЛОТА | ДЖЕКПОТ\n\nПример: <code>10 75 10 4.5 0.5</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_wheel")]]))

@dp.message(AdminPanel.wait_wheel_global)
async def wheel_global_save(m: Message, state: FSMContext):
    parts = m.text.strip().split()
    if len(parts) != 5: return await m.answer("❌ Должно быть ровно 5 чисел через пробел!")
    try:
        chances = [float(x.replace(",",".")) for x in parts]
        async with get_db() as db:
            for i in range(5):
                await db.execute("UPDATE wheel_config SET chance=? WHERE id=?", (chances[i], i))
            await db.commit()
        await m.answer("✅ Глобальные шансы обновлены!")
        await state.clear()
        await cq_wheel(m)
    except ValueError:
        await m.answer("❌ Ошибка. Введите только числа.")

# ═════ СБРОС КД ПРОДАЖ (ГЛОБАЛЬНЫЙ, внутри bulk) ═════
@dp.callback_query(F.data == "crm_sellall_confirm")
async def cq_sellall_confirm(obj):
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE status='on_auction'") as cur:
            cnt = (await cur.fetchone())[0]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Да, продать все {cnt} фото сейчас", callback_data="crm_sellall_do")],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_bulk")],
    ])
    await send_or_edit(obj, f"⚡ <b>Сброс КД продаж для ВСЕХ</b>\n\nСейчас на аукционе: <b>{cnt}</b> фото.\n\nВсе они будут проданы в течение <b>15 секунд</b>.\n\nВы уверены?", reply_markup=kb)

@dp.callback_query(F.data == "crm_sellall_do")
async def cq_sellall_do(cb: CallbackQuery):
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE status='on_auction'") as cur:
            cnt = (await cur.fetchone())[0]
        await db.execute("UPDATE photos SET sell_at=datetime('now') WHERE status='on_auction'")
        await db.commit()
    await send_or_edit(cb, f"✅ <b>КД сброшен!</b>\n\n<b>{cnt}</b> фото будут проданы в течение 15 секунд.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 К параметрам", callback_data="crm_bulk")]]))


# ═════ СПОНСОРЫ ═════
@dp.callback_query(F.data == "crm_sponsors")
async def call_sponsors(cb: CallbackQuery):
    await cq_sponsors(cb)

async def cq_sponsors(obj):
    sponsors = await get_sponsors()
    kb = [[InlineKeyboardButton(text=f"⚙️ {s['name']}", callback_data=f"crm_sp_manage:{s['channel_id']}")] for s in sponsors] + [[InlineKeyboardButton(text="➕ Добавить", callback_data="crm_sp_add")], [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]
    await send_or_edit(obj, f"🤝 <b>Спонсоры ({len(sponsors)})</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

async def cq_sp_manage_ui(obj, cid: str):
    sp = next((s for s in await get_sponsors() if s["channel_id"] == cid), None)
    if not sp:
        if isinstance(obj, CallbackQuery): await obj.answer("Не найден")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Имя", callback_data=f"crm_sp_edit:{cid}:name"), InlineKeyboardButton(text="📝 Описание", callback_data=f"crm_sp_edit:{cid}:desc")],
        [InlineKeyboardButton(text="⏳ Таймер", callback_data=f"crm_sp_edit:{cid}:days"), InlineKeyboardButton(text="❌ Удалить", callback_data=f"crm_sp_del:{cid}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_sponsors")]
    ])
    await send_or_edit(obj, f"⚙️ <b>{sp['name']}</b>\nОпис: {sp['description'] or 'Нет'}\nИстекает: {sp['expires_at'] or 'Никогда'}", reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_sp_manage:"))
async def cq_sp_manage(cb: CallbackQuery):
    await cq_sp_manage_ui(cb, cb.data.split(":")[1])

@dp.callback_query(F.data.startswith("crm_sp_edit:"))
async def cq_sp_edit(cb: CallbackQuery, state: FSMContext):
    _, cid, field = cb.data.split(":")
    await state.update_data(edit_cid=cid)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data=f"crm_sp_manage:{cid}")]])
    if field == "name":
        await state.set_state(AdminPanel.wait_edit_sp_name)
        await cb.message.edit_text("Новое имя:", reply_markup=kb)
    elif field == "desc":
        await state.set_state(AdminPanel.wait_edit_sp_desc)
        await cb.message.edit_text("Новое описание (или /skip):", reply_markup=kb)
    elif field == "days":
        await state.set_state(AdminPanel.wait_edit_sp_days)
        await cb.message.edit_text("Таймер (дни, или ГГГГ-ММ-ДД ЧЧ:ММ, или /skip):", reply_markup=kb)

@dp.message(AdminPanel.wait_edit_sp_name)
async def e_sp_n(m: Message, state: FSMContext):
    cid = (await state.get_data())["edit_cid"]
    async with get_db() as db:
        await db.execute("UPDATE sponsors SET name=? WHERE channel_id=?", (m.text.strip(), cid))
        await db.commit()
    await state.clear(); await m.answer("✅ Обновлено."); await cq_sp_manage_ui(m, cid)

@dp.message(AdminPanel.wait_edit_sp_desc)
async def e_sp_d(m: Message, state: FSMContext):
    cid = (await state.get_data())["edit_cid"]
    async with get_db() as db:
        await db.execute("UPDATE sponsors SET description=? WHERE channel_id=?", (None if m.text=="/skip" else m.text.strip(), cid))
        await db.commit()
    await state.clear(); await m.answer("✅ Обновлено."); await cq_sp_manage_ui(m, cid)

@dp.message(AdminPanel.wait_edit_sp_days)
async def e_sp_dy(m: Message, state: FSMContext):
    cid = (await state.get_data())["edit_cid"]
    v, exp = m.text.strip(), None
    if v != "/skip":
        if v.isdigit(): exp = (datetime.utcnow() + timedelta(days=int(v))).isoformat()
        else:
            try: exp = datetime.strptime(v, "%Y-%m-%d %H:%M").isoformat()
            except: return await m.answer("Неверный формат!")
    async with get_db() as db:
        await db.execute("UPDATE sponsors SET expires_at=?, notified=0 WHERE channel_id=?", (exp, cid))
        await db.commit()
    await state.clear(); await m.answer("✅ Обновлено."); await cq_sp_manage_ui(m, cid)

@dp.callback_query(F.data.startswith("crm_sp_del:"))
async def cq_sp_del(cb: CallbackQuery):
    async with get_db() as db:
        await db.execute("DELETE FROM sponsors WHERE channel_id=?", (cb.data.split(":")[1],))
        await db.commit()
    await cq_sponsors(cb)

@dp.callback_query(F.data == "crm_sp_add")
async def cq_sp_add(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_sponsor_forward)
    await cb.message.edit_text(
        "1️⃣ <b>Добавление спонсора</b>\n\nПерешлите мне любое сообщение из канала спонсора.\n\n"
        "Либо просто отправьте <code>@username</code> канала или ссылку (https://t.me/...).", 
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_sponsors")]])
    )

@dp.message(AdminPanel.wait_sponsor_forward)
async def sp_add_forward(m: Message, state: FSMContext):
    chat_id_or_name = None
    if m.forward_from_chat:
        chat_id_or_name = m.forward_from_chat.id
    else:
        text = m.text.strip()
        if "t.me/" in text:
            chat_id_or_name = "@" + text.split("t.me/")[1].split("/")[0].split("?")[0]
        elif text.startswith("@"):
            chat_id_or_name = text
        elif text.replace("-","").isdigit():
            chat_id_or_name = int(text)

    if not chat_id_or_name:
        return await m.answer("❌ Не удалось определить канал. Перешлите пост из канала или отправьте @username.")

    try:
        chat = await bot.get_chat(chat_id_or_name)
    except Exception as e:
        return await m.answer(f"❌ Ошибка получения инфы о канале: {e}\nБот должен быть админом в канале (если он приватный) или канал должен быть публичным.")

    cid = str(chat.id)
    name = chat.title
    url = chat.invite_link
    if not url and chat.username:
        url = f"https://t.me/{chat.username}"
    
    if not url:
        return await m.answer("❌ У бота нет прав для получения ссылки на канал. Добавьте его в админы.")

    fn = ""
    if chat.photo:
        try:
            fn = f"sp_{uuid.uuid4().hex[:8]}.jpg"
            file = await bot.get_file(chat.photo.small_file_id)
            await bot.download_file(file.file_path, destination=SPONSORS_DIR / fn)
        except:
            pass

    await state.update_data(cid=cid, n=name, u=url, f=fn)
    await state.set_state(AdminPanel.wait_sponsor_desc)
    await m.answer(f"✅ Канал <b>{name}</b> найден!\n\n2️⃣ Отправьте ОПИСАНИЕ (или /skip):", parse_mode=ParseMode.HTML)

@dp.message(AdminPanel.wait_sponsor_desc)
async def sp_add_5(m: Message, state: FSMContext):
    await state.update_data(d=None if m.text=="/skip" else m.text.strip()); await state.set_state(AdminPanel.wait_sponsor_days); await m.answer("3️⃣ На сколько ДНЕЙ добавить? (или /skip):")

@dp.message(AdminPanel.wait_sponsor_days)
async def sp_add_6(m: Message, state: FSMContext):
    dt_data = await state.get_data()
    v = m.text.strip()
    exp = None
    if v != "/skip":
        if v.isdigit(): exp = (datetime.utcnow() + timedelta(days=int(v))).isoformat()
        else:
            try: exp = datetime.strptime(v, "%Y-%m-%d %H:%M").isoformat()
            except: return await m.answer("Неверный формат!")
    async with get_db() as db:
        await db.execute("INSERT OR REPLACE INTO sponsors (channel_id, name, url, avatar_filename, description, expires_at) VALUES (?,?,?,?,?,?)", (dt_data["cid"], dt_data["n"], dt_data["u"], dt_data["f"], dt_data["d"], exp))
        await db.commit()
    await state.clear(); await m.answer("✅ Спонсор добавлен!"); await cq_sponsors(m)

# ═════ ОТВЕТЫ АДМИНА В ТИКЕТАХ (С ФОТО) ═════
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
        
        if status == 'closed': return await message.reply("⚠️ Закрыт.")
        if status == 'open':
            await db.execute("UPDATE tickets SET status='claimed', claimed_by=? WHERE id=?", (message.from_user.id, tkt_id))
            await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, uid, f"👨‍💻 @{message.from_user.username or 'Admin'} в чате."))
            
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, image, direction) VALUES (?,?,?,?,'out')", (tkt_id, uid, reply_text, image_name))
        await db.commit()
        
    p = await get_player(uid)
    try:
        final_text = tr(p["lang"] if p else "en", "support_reply", text=reply_text) if reply_text else tr(p["lang"] if p else "en", "support_reply", text="[Фото]")
        if image_name:
            await bot.send_photo(uid, photo=FSInputFile(SUPPORT_DIR / image_name), caption=final_text, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(uid, final_text, parse_mode=ParseMode.HTML)
        await message.reply("✅ Отправлено пользователю.")
    except Exception:
        await message.reply("❌ Юзер заблокировал бота, но он увидит ответ в приложении.")

@dp.callback_query(F.data == "crm_tickets")
async def call_tickets(cb: CallbackQuery):
    await cq_tickets(cb)

async def cq_tickets(obj):
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT t.id, t.user_id, p.username FROM tickets t LEFT JOIN players p ON t.user_id=p.user_id WHERE t.status='open' LIMIT 10") as cur:
            tkts = await cur.fetchall()
    if not tkts: return await send_or_edit(obj, "Нет тикетов.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]))
    kb = [[InlineKeyboardButton(text=f"Смотреть #{t['id']} (@{t['username'] or t['user_id']})", callback_data=f"crm_tview:{t['id']}")] for t in tkts] + [[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]
    await send_or_edit(obj, "🎧 <b>Тикеты:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("crm_tview:"))
async def cq_tview(cb: CallbackQuery):
    tkt_id = int(cb.data.split(":")[1])
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT t.id, t.user_id, t.status, p.username FROM tickets t LEFT JOIN players p ON t.user_id=p.user_id WHERE t.id=?", (tkt_id,)) as cur:
            tkt = await cur.fetchone()
        if not tkt or tkt["status"] != "open": return await cb.answer("Взят/закрыт.", show_alert=True)
        async with db.execute("SELECT text FROM support_messages WHERE ticket_id=? AND direction='in' ORDER BY created_at DESC LIMIT 1", (tkt_id,)) as cur:
            msg = await cur.fetchone()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🙋‍♂️ Взять", callback_data=f"crm_tclaim:{tkt_id}")], [InlineKeyboardButton(text="🔙 К списку", callback_data="crm_tickets")]])
    await cb.message.edit_text(f"📨 <b>Тикет #{tkt_id}</b>\n\n<i>{msg['text'] if msg else '[Фото]'}</i>", parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_tclaim:"))
async def cq_tclaim(cb: CallbackQuery):
    tkt_id = int(cb.data.split(":")[1])
    async with get_db() as db:
        async with db.execute("SELECT user_id, status FROM tickets WHERE id=?", (tkt_id,)) as cur:
            tkt = await cur.fetchone()
        if not tkt or tkt[1] != "open": return await cb.answer("Уже взят.", show_alert=True)
        await db.execute("UPDATE tickets SET status='claimed', claimed_by=? WHERE id=?", (cb.from_user.id, tkt_id))
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, tkt[0], f"👨‍💻 @{cb.from_user.username or 'Admin'} в чате."))
        await db.commit()
    await cb.message.edit_text(f"✅ <b>Тикет #{tkt_id}</b> у вас. Делайте Reply на это сообщение, чтобы ответить. Можно прикреплять фото.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔒 Закрыть", callback_data=f"crm_tclose:{tkt_id}")]]))

@dp.callback_query(F.data.startswith("crm_tclose:"))
async def cq_tclose(cb: CallbackQuery):
    tkt_id = int(cb.data.split(":")[1])
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM tickets WHERE id=?", (tkt_id,)) as cur: uid = (await cur.fetchone())[0]
        await db.execute("UPDATE tickets SET status='closed' WHERE id=?", (tkt_id,))
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, uid, "✅ Завершен."))
        await db.commit()
    await cb.message.edit_text(f"✅ Тикет #{tkt_id} закрыт.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 К списку", callback_data="crm_tickets")]]))

@dp.callback_query(F.data == "crm_wd")
async def call_wd(cb: CallbackQuery):
    await cq_wd(cb)

async def cq_wd(obj):
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT wr.*, p.username FROM withdrawal_requests wr LEFT JOIN players p ON p.user_id = wr.user_id WHERE wr.status='pending' ORDER BY wr.is_priority DESC, wr.created_at ASC LIMIT 10") as cur:
            wds = await cur.fetchall()
    if not wds: return await send_or_edit(obj, "Нет активных заявок.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]))
    text = "💳 <b>Выводы:</b>\n\n"
    kb = []
    for w in wds:
        text += f"Req <code>{w['id']}</code> | {'⭐' if w['is_priority'] else ''}{w['user_id']} | <b>${w['amount_usd']:.2f}</b>\n"
        kb.append([InlineKeyboardButton(text=f"✅ #{w['id']}", callback_data=f"crm_wdok:{w['id']}"), InlineKeyboardButton(text=f"❌ #{w['id']}", callback_data=f"crm_wdrej_do:{w['id']}:notify")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")])
    await send_or_edit(obj, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("crm_wdok:"))
async def cq_wdok(cb: CallbackQuery):
    async with get_db() as db:
        await db.execute("UPDATE withdrawal_requests SET status='completed' WHERE id=?", (cb.data.split(":")[1],))
        await db.commit()
    await cq_wd(cb)

@dp.callback_query(F.data.startswith("crm_wdrej_do:"))
async def cq_wdrej_do(cb: CallbackQuery):
    wid = int(cb.data.split(":")[1])
    async with get_db() as db:
        async with db.execute("SELECT user_id, amount_usd FROM withdrawal_requests WHERE id=?", (wid,)) as cur: req = await cur.fetchone()
        await db.execute("UPDATE withdrawal_requests SET status='rejected' WHERE id=?", (wid,))
        await db.execute("UPDATE players SET balance=balance+? WHERE user_id=?", (req[1], req[0]))
        await db.commit()
    p = await get_player(req[0])
    try: await bot.send_message(req[0], tr(p["lang"] if p else "en", "wd_rejected"), parse_mode=ParseMode.HTML)
    except: pass
    await cq_wd(cb)

@dp.callback_query(F.data == "crm_broadcast")
async def cq_broad(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_broadcast)
    await cb.message.edit_text("Текст рассылки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="crm_main")]]))

@dp.message(AdminPanel.wait_broadcast)
async def broad_step(m: Message, state: FSMContext):
    await state.clear(); uids = await get_all_user_ids(); ok=0
    await m.answer("📣 Начали...")
    for u in uids:
        try: await bot.send_message(u, m.html_text, parse_mode=ParseMode.HTML); ok+=1
        except: pass
        await asyncio.sleep(0.05)
    await m.answer(f"✅ Доставлено: {ok}")
    await cmd_admin(m)

# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP (Защищенные эндпоинты)
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
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

    if await get_setting("maintenance_mode") == "1":
        if not await is_admin(uid):
            return JSONResponse(status_code=403, content={"error": "maintenance", "end_time": await get_setting("maintenance_end")})

    player, _ = await get_or_create_player(uid, username)
    lang = player.get("lang", "en")
    
    if player.get("is_banned"):
        return JSONResponse(status_code=403, content={"error": "banned"})

    if not await is_admin(uid) and not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, uid):
        return JSONResponse(status_code=402, content={"error": "subscription_required", "channels": [{"id": REQUIRED_CHANNEL_ID, "url": REQUIRED_CHANNEL_URL, "name": REQUIRED_CHANNEL_NAME}], "message": tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en")})

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
                w_conf = []
                for i, p in enumerate(DEFAULT_WHEEL_PRIZES):
                    w_conf.append({**p, "chance": chances[i]})
            except:
                async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur:
                    w_conf = [dict(r) for r in await cur.fetchall()]
        else:
            async with db.execute("SELECT * FROM wheel_config ORDER BY id") as cur:
                w_conf = [dict(r) for r in await cur.fetchall()]
        
    photos_list = await get_player_photos(uid, lang)
    return {
        "player": player, "photos": photos_list,
        "withdraw_unlocked": ref_count >= MIN_REFERRALS_WITHDRAW, "vip_level": vip_level(ref_count),
        "vip_tiers": [{"min": t[0], "max_delay": t[1], "slots": t[2]} for t in VIP_TIERS],
        "referral_url": await referral_url(uid), "rub_rate": RUB_TO_USD_RATE,
        "active_slots": await get_active_photo_count(uid), "slot_limit": await get_user_total_slot_limit(uid, ref_count),
        "min_referrals_withdraw": MIN_REFERRALS_WITHDRAW, "withdraw_condition": tr(lang, "withdraw_locked"),
        "min_withdrawal_usd": MIN_WITHDRAWAL_USD,
        "active_auction_count": sum(1 for ph in photos_list if ph.get("status") == "on_auction"),
        "wheel": {"can_spin": can_spin, "next_spin_ms": next_spin_ms},
        "wheel_config": w_conf
    }

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
    r = random.uniform(0, total_chance)
    cur_chance = 0
    prize = prizes[-1]
    
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
async def api_buy_item(
    item: str = Form(...),
    init_data: str = Header(None, alias="X-Telegram-Init-Data")
):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    
    price = 10.0 if item == "spin" else 100.0
    
    async with get_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT balance FROM players WHERE user_id=?", (uid,)) as cur:
            p = await cur.fetchone()
            
        if not p or p["balance"] < price:
            raise HTTPException(400, detail="Недостаточно средств" if item == "slots" else "Not enough funds")
            
        await db.execute("UPDATE players SET balance=balance-? WHERE user_id=?", (price, uid))
        
        if item == "spin":
            await db.execute("UPDATE players SET last_spin=NULL WHERE user_id=?", (uid,))
        elif item == "slots":
            await db.execute("INSERT INTO user_slots (user_id, is_permanent, expires_at) VALUES (?, 0, datetime('now', '+7 days'))", (uid,))
            
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
        async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code,)) as cur:
            promo = await cur.fetchone()
            
        if not promo:
            raise HTTPException(400, detail="Invalid promo code")
            
        if promo["uses"] >= promo["max_uses"]:
            raise HTTPException(400, detail="Promo code usage limit reached")
            
        async with db.execute("SELECT 1 FROM promo_uses WHERE user_id=? AND code=?", (uid, code)) as cur:
            if await cur.fetchone():
                raise HTTPException(400, detail="You already used this code")
                
        # Grant reward
        if promo["type"] == "usd":
            await db.execute("UPDATE players SET balance=balance+? WHERE user_id=?", (promo["val"], uid))
            msg = f"Added ${promo['val']} to balance!"
        elif promo["type"] == "spin":
            await db.execute("UPDATE players SET last_spin=NULL WHERE user_id=?", (uid,))
            msg = "Wheel cooldown reset!"
        elif promo["type"] == "slot":
            slots_to_add = int(promo["val"])
            is_perm = 1 if promo["duration_days"] == 0 else 0
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
    user_id: int = Form(...), 
    username: str = Form(""), 
    files: List[UploadFile] = File(...),
    init_data: str = Header(None, alias="X-Telegram-Init-Data")
):
    uid = verify_webapp_data(init_data)
    await check_maintenance(uid)
    if uid != user_id: raise HTTPException(403)

    p, _ = await get_or_create_player(uid, username)
    if p.get("is_banned"): raise HTTPException(403)
    
    ref_c, act = p.get("referrals_count", 0), await get_active_photo_count(uid)
    lim = await get_user_total_slot_limit(uid, ref_c)
    if act + len(files) > lim: raise HTTPException(403, "Limit reached")

    saved_files = []
    file_hashes = []
    for f in files:
        raw = await f.read(MAX_UPLOAD_SIZE + 1)
        if len(raw) > MAX_UPLOAD_SIZE:
            raise HTTPException(400, "File too large")

        photo_hash = hashlib.sha256(raw).hexdigest()
        async with get_db() as db:
            async with db.execute("SELECT 1 FROM photos WHERE photo_hash=?", (photo_hash,)) as cur:
                if await cur.fetchone():
                    raise HTTPException(400, detail="duplicate_photo")
        file_hashes.append(photo_hash)

        fn = f"{uuid.uuid4().hex}.jpg"
        filepath = UPLOADS_DIR / fn
        await asyncio.to_thread(filepath.write_bytes, raw)
        saved_files.append(fn)

    is_pack, results = len(saved_files) >= PACK_SIZE, []
    rub_each = [random.randint(PACK_MIN_RUB, PACK_MAX_RUB)//PACK_SIZE]*len(saved_files) if is_pack else [random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB) for _ in range(len(saved_files))]
    bid = uuid.uuid4().hex

    vip_lvl = vip_level(ref_c)
    sale_min_secs = max((10 - vip_lvl) * 3600, 3600)
    sale_max_secs = max((12 - vip_lvl) * 3600, sale_min_secs + 3600)

    async with get_db() as db:
        for i, fn in enumerate(saved_files):
            sat = (datetime.utcnow() + timedelta(seconds=random.randint(sale_min_secs, sale_max_secs))).isoformat()
            sr, pu, pid = rub_each[i], apply_commission(rub_to_usd(rub_each[i])), uuid.uuid4().hex
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
    if refs < MIN_REFERRALS_WITHDRAW or missing_subs:
        raise HTTPException(403, detail="conditions_not_met")

    balance = round(p["balance"] or 0, 2)

    if balance < MIN_WITHDRAWAL_USD:
        raise HTTPException(400, detail=json.dumps({"error": "min_balance", "min": MIN_WITHDRAWAL_USD}))

    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'", (uid,)) as cur:
            active_count = (await cur.fetchone())[0]
    if active_count > 0:
        raise HTTPException(400, detail=json.dumps({"error": "active_sales", "count": active_count}))

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
async def api_support_send(
    text: str = Form(""),
    file: UploadFile = File(None),
    init_data: str = Header(None, alias="X-Telegram-Init-Data")
):
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
        async with db.execute("SELECT id, claimed_by FROM tickets WHERE user_id=? AND status IN ('open', 'claimed')", (uid,)) as cur:
            tkt = await cur.fetchone()
        if not tkt:
            await db.execute("INSERT INTO tickets (user_id) VALUES (?)", (uid,))
            await db.commit()
            async with db.execute("SELECT last_insert_rowid()") as cur: tkt_id = (await cur.fetchone())[0]
            cb = None
        else: tkt_id, cb = tkt["id"], tkt["claimed_by"]
        
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, image, direction) VALUES (?,?,?,?,'in')", (tkt_id, uid, text, img_filename))
        await db.commit()
        
    await dispatch_support_ticket(uid, tkt_id, text, cb, img_filename)
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
    elif ref_param.isdigit():
        ref_id = int(ref_param)
        
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
