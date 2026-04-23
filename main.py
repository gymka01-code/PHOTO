import asyncio
import logging
import os
import random
import urllib.parse
import uuid
import math
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

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
    LabeledPrice,
    PreCheckoutQuery
)
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
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

UPLOADS_DIR = Path(os.path.join(_VOLUME, "uploads"))
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

SPONSORS_DIR = Path(os.path.join(_VOLUME, "uploads", "sponsors"))
SPONSORS_DIR.mkdir(parents=True, exist_ok=True)

_BOT_USERNAME_CACHE = os.path.join(_VOLUME, ".bot_username")

MAX_FILE_SIZE = 10 * 1024 * 1024

RUB_TO_USD_RATE        = 92.0
COMMISSION_PCT         = 0.02
SINGLE_MIN_RUB         = 200
SINGLE_MAX_RUB         = 600
PACK_MIN_RUB           = 1_000
PACK_MAX_RUB           = 3_000
PACK_SIZE              = 5
MIN_REFERRALS_WITHDRAW = 3

VIP_TIERS = [
    (0,  3600,  5),
    (3,  2700, 10),
    (5,  1800, 15),
    (10,  900, 20),
    (25,  300, 25),
    (50,   60, 30),
]
MIN_DELAY_SECS = 30

REQUIRED_CHANNEL_ID   = "@dsdfsdfawer"
REQUIRED_CHANNEL_URL  = "https://t.me/dsdfsdfawer"
REQUIRED_CHANNEL_NAME = "PhotoFlip Community"

FAKE_USERS = [
    "u***r7", "a***2", "m***k9", "p***y4", "t***3", "j***8", "k***5", "s***1",
    "PhotoNinja_7", "SniperLens_3", "PixelHunter_2", "SnapMaster_5",
    "Дмитрий Волков", "Артем Степанов", "Сергей Карпов", "Никита Миронов",
    "crypto_king_77", "usdt_master", "p2p_shark", "arbitrage_pro"
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

WHEEL_PRIZES = [
    {"id": 0, "type": "usd", "val": 0.05, "label": "$0.05", "color": "#1e3a8a", "chance": 25},
    {"id": 1, "type": "lose", "val": 0, "label": "Lose", "color": "#111111", "chance": 40},
    {"id": 2, "type": "usd", "val": 0.20, "label": "$0.20", "color": "#4c1d95", "chance": 15},
    {"id": 3, "type": "slot", "val": 1, "label": "+1 Slot", "color": "#b45309", "chance": 15},
    {"id": 4, "type": "usd", "val": 1.00, "label": "$1.00!", "color": "#064e3b", "chance": 5},
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AdminPanel(StatesGroup):
    wait_sponsor_id    = State()
    wait_sponsor_name  = State()
    wait_sponsor_url   = State()
    wait_sponsor_photo = State()
    wait_sponsor_desc  = State()
    wait_sponsor_days  = State()
    
    wait_edit_sp_name  = State()
    wait_edit_sp_desc  = State()
    wait_edit_sp_days  = State()
    
    wait_ticket_reply  = State()
    wait_broadcast     = State()

_T = {
    "en": {
        "welcome": "👋 Welcome to <b>PhotoFlip</b>!\n\n📸 Upload photos → Valuation → Auction → Earn USD\n\n💰 Balance: <b>${balance:.2f}</b>\n⭐ VIP Level: <b>{vip}</b> · Slots: <b>{slots}</b>\n\n🔗 Your referral link:\n<code>{ref_url}</code>\n\nInvite <b>3 friends</b> to unlock withdrawal.\n\nTap below to open PhotoFlip:",
        "btn_open": "📸 Open PhotoFlip",
        "btn_referrals": "🤝 Referrals",
        "btn_share": "📤 Share",
        "sold": "✅ <b>Photo sold!</b>\n\n💴 Price: <b>{rub:,} ₽</b> → <b>${gross}</b>\n📉 Fee (2%): <b>−${commission}</b>\n💰 Credited: <b>${net}</b>\n\nBalance: <b>${balance:.2f}</b>",
        "support_reply": "📨 <b>Support reply:</b>\n\n{text}",
        "remind": "⏰ <b>Reminder!</b> Your photos are live. Invite friends to unlock withdrawals: <code>{ref_url}</code>",
        "withdraw_locked": "Invite <b>3 friends</b> to unlock withdrawal.",
        "withdraw_processing": "✅ Withdrawal requested! Processing takes 1-7 business days.",
        "unsub_warning": "⚠️ <b>Warning!</b> You have an active withdrawal request, but you unsubscribed from our partner channels. Resubscribe within 12 hours or your request will be cancelled.",
        "resub_thanks": "✅ <b>Thank you!</b> We verified your subscription. Your withdrawal will process normally.",
        "wd_rejected": "❌ <b>Your withdrawal request was cancelled.</b> Funds returned to your balance."
    },
    "ru": {
        "welcome": "👋 Добро пожаловать в <b>PhotoFlip</b>!\n\n📸 Загрузи фото → Оценка → Аукцион → Заработай USD\n\n💰 Баланс: <b>${balance:.2f}</b>\n⭐ VIP Уровень: <b>{vip}</b> · Слотов: <b>{slots}</b>\n\n🔗 Ваша реферальная ссылка:\n<code>{ref_url}</code>\n\nПригласите <b>3 друзей</b> для активации вывода.",
        "btn_open": "📸 Открыть PhotoFlip",
        "btn_referrals": "🤝 Рефералы",
        "btn_share": "📤 Поделиться",
        "sold": "✅ <b>Ваше фото продано!</b>\n\n💴 Цена: <b>{rub:,} ₽</b> → <b>${gross}</b>\n📉 Комиссия (2%): <b>−${commission}</b>\n💰 Начислено: <b>${net}</b>\n\nБаланс: <b>${balance:.2f}</b>",
        "support_reply": "📨 <b>Ответ поддержки:</b>\n\n{text}",
        "remind": "⏰ <b>Напоминание!</b> Ваши фото на аукционе. Приглашайте друзей для вывода: <code>{ref_url}</code>",
        "withdraw_locked": "Пригласите <b>3 друзей</b> для активации вывода.",
        "withdraw_processing": "✅ Заявка принята! Выплаты занимают 1-7 рабочих дней.",
        "unsub_warning": "⚠️ <b>Внимание!</b> У вас есть активная заявка на вывод, но вы отписались от спонсоров. Подпишитесь обратно в течение 12 часов, иначе заявка будет аннулирована.",
        "resub_thanks": "✅ <b>Спасибо!</b> Подписка проверена. Обработка заявки продолжается.",
        "wd_rejected": "❌ <b>Ваша заявка на вывод была отклонена.</b> Средства возвращены на баланс."
    }
}

def tr(lang: str, key: str, **kw) -> str:
    tmpl = _T.get(lang, _T["en"]).get(key, _T["en"].get(key, key))
    return tmpl.format(**kw) if kw else tmpl

def rub_to_usd(rub: float) -> float: return round(rub / RUB_TO_USD_RATE, 2)
def apply_commission(usd: float) -> float: return round(usd * (1 - COMMISSION_PCT), 2)

def vip_level(refs: int) -> int:
    lvl = 0
    for i, (thr, _, _) in enumerate(VIP_TIERS):
        if refs >= thr: lvl = i
    return lvl

def vip_max_delay(refs: int) -> int: return VIP_TIERS[vip_level(refs)][1]
def vip_slot_limit(refs: int) -> int: return VIP_TIERS[vip_level(refs)][2]
def usd_to_stars(usd: float) -> int: return math.floor(usd / 0.012)

def make_share_url(ref_url: str) -> str:
    text = "Твоя камера теперь печатает деньги. Серьезно. 🖼💰\nPhotoFlip — это как биржа, только вместо акций — твои фото.\nЗалетай по моей ссылке: 🔗\nПроверим, чей лот купят быстрее? 😉"
    return f"https://t.me/share/url?url={urllib.parse.quote(ref_url, safe='')}&text={urllib.parse.quote(text, safe='')}"

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# --- INIT DB ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS players (user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0.0, total_earned REAL DEFAULT 0.0, photos_sold INTEGER DEFAULT 0, referrals_count INTEGER DEFAULT 0, referred_by INTEGER DEFAULT NULL, lang TEXT DEFAULT 'en', last_seen TEXT DEFAULT (datetime('now')), created_at TEXT DEFAULT (datetime('now')))""")
        
        # БЕЗОПАСНАЯ МИГРАЦИЯ ДЛЯ СУЩЕСТВУЮЩИХ БД
        for col in ["extra_slots INTEGER DEFAULT 0", "last_spin TEXT DEFAULT NULL"]:
            try:
                await db.execute(f"ALTER TABLE players ADD COLUMN {col}")
            except Exception:
                pass
                
        await db.execute("""CREATE TABLE IF NOT EXISTS photos (id TEXT PRIMARY KEY, user_id INTEGER, filename TEXT, batch_id TEXT, base_price REAL, final_price REAL, sale_rub REAL DEFAULT 0, status TEXT DEFAULT 'pending', sell_at TEXT, sold_at TEXT, buyer TEXT, created_at TEXT DEFAULT (datetime('now')), FOREIGN KEY(user_id) REFERENCES players(user_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS referrals (referrer_id INTEGER, referred_id INTEGER PRIMARY KEY, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsors (channel_id TEXT PRIMARY KEY, name TEXT, url TEXT, avatar_filename TEXT, created_at TEXT DEFAULT (datetime('now')))""")
        
        # МИГРАЦИЯ СПОНСОРОВ
        for col in ["description TEXT", "expires_at TEXT", "notified INTEGER DEFAULT 0"]:
            try:
                await db.execute(f"ALTER TABLE sponsors ADD COLUMN {col}")
            except Exception:
                pass
                
        await db.execute("""CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, status TEXT DEFAULT 'open', claimed_by INTEGER DEFAULT NULL, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS support_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INTEGER, user_id INTEGER, text TEXT, direction TEXT, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawal_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount_usd REAL, stars INTEGER DEFAULT 0, method TEXT, is_priority INTEGER DEFAULT 0, status TEXT DEFAULT 'pending', warning_sent_at TEXT DEFAULT NULL, created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY, username TEXT, added_by INTEGER, created_at TEXT DEFAULT (datetime('now')))""")
        await db.commit()

# --- HELPERS ---
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

async def get_player_photos(user_id: int, lang: str = "en") -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM photos WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (user_id,)) as cur:
            rows = await cur.fetchall()
    return [dict(r) | {"status_label": tr(lang, "status_auction") if r["status"] == "on_auction" else ""} for r in rows]

async def get_active_photo_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'", (user_id,)) as cur:
            return (await cur.fetchone())[0]

async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM players WHERE referred_by=?", (user_id,)) as cur:
            return (await cur.fetchone())[0]

async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM players") as cur:
            return [r[0] for r in await cur.fetchall()]

async def get_referral_list(referrer_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT r.referred_id, r.created_at, p.username, CASE WHEN p.photos_sold > 0 THEN 1 ELSE 0 END AS is_active FROM referrals r LEFT JOIN players p ON p.user_id = r.referred_id WHERE r.referrer_id = ? ORDER BY r.created_at DESC", (referrer_id,)) as cur:
            return [dict(r) for r in await cur.fetchall()]

async def is_admin(user_id: int) -> bool:
    if ADMIN_ID and user_id == ADMIN_ID: return True
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) as cur:
            return bool(await cur.fetchone())

async def get_admin_ids() -> set[int]:
    ids: set[int] = {ADMIN_ID} if ADMIN_ID else set()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM admins") as cur:
            ids.update(r[0] for r in await cur.fetchall())
    return ids

async def get_sponsors():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM sponsors") as cur:
            rows = await cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["avatar"] = f"{WEBAPP_URL}/uploads/sponsors/{d['avatar_filename']}" if d.get("avatar_filename") else ""
        result.append(d)
    return result

async def is_subscribed_to_channel(channel_id: str, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        return member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
    except Exception: return False

async def check_all_subs(user_id: int) -> list:
    sponsors = await get_sponsors()
    missing = []
    for sp in sponsors:
        if not await is_subscribed_to_channel(sp["channel_id"], user_id):
            missing.append(sp)
    return missing

async def referral_url(user_id: int) -> str:
    global _bot_username
    if not _bot_username:
        try:
            me = await bot.get_me()
            _bot_username = me.username
            with open(_BOT_USERNAME_CACHE, "w") as f: f.write(_bot_username)
        except: pass
    return f"https://t.me/{_bot_username}?start=ref_{user_id}" if _bot_username else ""

_bot_username = None
try:
    with open(_BOT_USERNAME_CACHE) as f: _bot_username = f.read().strip()
except: pass

async def _bind_referral(new_user_id: int, referrer_id: int) -> bool:
    if referrer_id == new_user_id: return False
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT 1 FROM players WHERE user_id=?", (referrer_id,)) as cur:
                if not await cur.fetchone(): return False
            async with db.execute("SELECT referred_by FROM players WHERE user_id=?", (new_user_id,)) as cur:
                row = await cur.fetchone()
                if row is None or row[0] is not None:
                    return False
            await db.execute("UPDATE players SET referred_by=? WHERE user_id=?", (referrer_id, new_user_id))
            await db.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)", (referrer_id, new_user_id))
            await db.execute("UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?", (referrer_id,))
            await db.commit()

        try:
            new_player = await get_player(new_user_id)
            display = f"@{new_player['username']}" if new_player and new_player.get("username") else str(new_user_id)
            await bot.send_message(referrer_id, f"🔔 <b>Новый реферал!</b>\nПользователь {display} присоединился по вашей ссылке.", parse_mode=ParseMode.HTML)
        except Exception: pass
        return True
    except Exception as e:
        logger.error(f"Referral binding error: {e}")
        return False

# --- WORKERS ---
async def auction_worker():
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                now = datetime.utcnow().isoformat()
                async with db.execute("SELECT * FROM photos WHERE status='on_auction' AND sell_at<=?", (now,)) as cur:
                    due = await cur.fetchall()

                for photo in due:
                    buyer = random.choice(FAKE_USERS)
                    sale_rub = photo["sale_rub"] or random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB)
                    gross = rub_to_usd(float(sale_rub))
                    net = apply_commission(gross)
                    
                    await db.execute("UPDATE photos SET status='sold', sold_at=datetime('now'), buyer=?, final_price=?, sale_rub=? WHERE id=?", (buyer, net, sale_rub, photo["id"]))
                    await db.execute("UPDATE players SET balance=balance+?, total_earned=total_earned+?, photos_sold=photos_sold+1 WHERE user_id=?", (net, net, photo["user_id"]))
                    await db.commit()

                    p = await get_player(photo["user_id"])
                    if p:
                        try: await bot.send_message(photo["user_id"], tr(p["lang"], "sold", rub=int(sale_rub), gross=gross, commission=round(gross-net,2), net=net, buyer=buyer, balance=round((p["balance"] or 0)+net, 2)), parse_mode=ParseMode.HTML)
                        except: pass
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
                except: pass
        except: pass

async def monitor_withdrawals_worker():
    while True:
        await asyncio.sleep(3600)
        try:
            async with aiosqlite.connect(DB_PATH) as db:
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
                            try: warn_time = datetime.strptime(warn_time_str, "%Y-%m-%d %H:%M:%S")
                            except: warn_time = datetime.fromisoformat(warn_time_str)
                            
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
        await asyncio.sleep(3600)
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                now = datetime.utcnow().isoformat()
                async with db.execute("SELECT * FROM sponsors WHERE expires_at IS NOT NULL AND expires_at <= ? AND notified = 0", (now,)) as cur:
                    expired = await cur.fetchall()

                for sp in expired:
                    for aid in await get_admin_ids():
                        try: await bot.send_message(aid, f"⚠️ <b>Внимание: Истекло время спонсора!</b>\n\nКанал: {sp['name']}\nID: <code>{sp['channel_id']}</code>\n\nПора удалить его из пула или продлить таймер.", parse_mode=ParseMode.HTML)
                        except: pass
                    await db.execute("UPDATE sponsors SET notified = 1 WHERE channel_id=?", (sp["channel_id"],))
                await db.commit()
        except Exception as e: logger.error(f"sponsor_expiry_worker error: {e}")

# --- BOT ROUTES ---
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user, args_str = message.from_user, (command.args or "").strip()
    referrer_id = None
    if args_str:
        raw_arg = args_str[4:] if args_str.startswith("ref_") else args_str
        try: referrer_id = int(raw_arg)
        except ValueError: pass

    await get_or_create_player(user.id, user.username or "")
    if referrer_id:
        await _bind_referral(user.id, referrer_id)

    if not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Subscribe to Channel", url=REQUIRED_CHANNEL_URL)],
            [InlineKeyboardButton(text="✅ I've Subscribed", callback_data=f"chksub:{args_str}")]
        ])
        await message.answer(f"👋 Welcome to <b>PhotoFlip</b>!\n\n📢 To use the bot you must subscribe to our channel first:\n<a href='{REQUIRED_CHANNEL_URL}'><b>{REQUIRED_CHANNEL_NAME}</b></a>\n\nAfter subscribing tap <b>✅ I've Subscribed</b>.", parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    await _process_start(message, user, args_str)

async def _process_start(target: Message, user, args: str = ""):
    p = await get_player(user.id)
    lang, ref_count = p["lang"], await get_referral_count(user.id)
    ref_url = await referral_url(user.id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tr(lang, "btn_open"), web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(text=tr(lang, "btn_share"), url=make_share_url(ref_url))] if ref_url else []
    ])
    await target.answer(tr(lang, "welcome", balance=p["balance"], vip=vip_level(ref_count), slots=VIP_TIERS[vip_level(ref_count)][2]+(p["extra_slots"] or 0), ref_url=ref_url), parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("chksub:"))
async def cb_check_sub(cb: CallbackQuery):
    if not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, cb.from_user.id):
        return await cb.answer("❌ You haven't subscribed yet!", show_alert=True)
    await cb.answer("✅ Subscription confirmed!")
    try: await cb.message.delete()
    except: pass
    await _process_start(cb.message, cb.from_user, cb.data[7:])

# --- STARS PAYMENTS ---
@dp.pre_checkout_query()
async def on_pre_checkout(pre_checkout: PreCheckoutQuery):
    await pre_checkout.answer(ok=True)

@dp.message(F.successful_payment)
async def on_successful_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    uid = message.from_user.id
    
    async with aiosqlite.connect(DB_PATH) as db:
        if payload == "buy_spin":
            await db.execute("UPDATE players SET last_spin=NULL WHERE user_id=?", (uid,))
            await message.answer("✅ <b>Purchase successful!</b>\nYou got +1 Wheel Spin. Open the app to use it.", parse_mode=ParseMode.HTML)
        elif payload == "buy_slots":
            await db.execute("UPDATE players SET extra_slots=extra_slots+5 WHERE user_id=?", (uid,))
            await message.answer("✅ <b>Purchase successful!</b>\nYou permanently gained +5 Auction Slots. Open the app to use them.", parse_mode=ParseMode.HTML)
        await db.commit()

# --- СВАЙП-ОТВЕТ ДЛЯ АДМИНОВ ---
@dp.message(F.reply_to_message)
async def admin_native_reply(message: Message):
    if not await is_admin(message.from_user.id): return
    
    replied = message.reply_to_message
    original_text = replied.text or replied.caption
    if not original_text: return
    
    match = re.search(r"Тикет #(\d+)", original_text, re.IGNORECASE)
    if not match: return
    
    tkt_id = int(match.group(1))
    reply_text = message.text or message.caption or ""
    if not reply_text: return
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, status FROM tickets WHERE id=?", (tkt_id,)) as cur:
            tkt = await cur.fetchone()
            
        if not tkt: return
        uid, status = tkt[0], tkt[1]
        
        if status == 'closed':
            return await message.reply("⚠️ Этот тикет уже закрыт.")
            
        if status == 'open':
            await db.execute("UPDATE tickets SET status='claimed', claimed_by=? WHERE id=?", (message.from_user.id, tkt_id))
            admin_name = message.from_user.username or "Admin"
            system_text = f"👨‍💻 Агент поддержки @{admin_name} подключился к диалогу."
            await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, uid, system_text))
            
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?,'out')", (tkt_id, uid, reply_text))
        await db.commit()
        
    p = await get_player(uid)
    try:
        await bot.send_message(uid, tr(p["lang"] if p else "en", "support_reply", text=reply_text), parse_mode=ParseMode.HTML)
        await message.reply(f"✅ Ответ отправлен (Тикет #{tkt_id}).")
    except:
        await message.reply("❌ Ошибка отправки пользователю (Возможно, бот заблокирован).")

# --- CRM ADMIN PANEL ---
@dp.message(Command("admin"))
@dp.message(Command("panel"))
async def cmd_admin(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤝 Спонсоры", callback_data="crm_sponsors"), InlineKeyboardButton(text="🎧 Тикеты", callback_data="crm_tickets")],
        [InlineKeyboardButton(text="💳 Выводы", callback_data="crm_wd"), InlineKeyboardButton(text="📢 Рассылка", callback_data="crm_broadcast")],
    ])
    await message.answer("👑 <b>Админ Панель CRM</b>\nВыберите раздел:", parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data == "crm_main")
async def cq_crm_main(cb: CallbackQuery, state: FSMContext):
    await cmd_admin(cb.message, state)
    try: await cb.message.delete()
    except: pass

# --- Спонсоры CRM ---
@dp.callback_query(F.data == "crm_sponsors")
async def cq_sponsors(cb: CallbackQuery):
    sponsors = await get_sponsors()
    kb = []
    for s in sponsors:
        kb.append([InlineKeyboardButton(text=f"⚙️ {s['name']}", callback_data=f"crm_sp_manage:{s['channel_id']}")])
    
    kb.append([InlineKeyboardButton(text="➕ Добавить спонсора", callback_data="crm_sp_add")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")])
    
    await cb.message.edit_text(f"🤝 <b>Управление спонсорами (Всего: {len(sponsors)})</b>\n\nВыберите канал для редактирования или добавьте новый.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("crm_sp_manage:"))
async def cq_sp_manage(cb: CallbackQuery):
    cid = cb.data.split(":")[1]
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM sponsors WHERE channel_id=?", (cid,)) as cur:
            sp = await cur.fetchone()
            
    if not sp: return await cb.answer("Спонсор не найден", show_alert=True)

    exp = sp["expires_at"][:16].replace("T", " ") if sp["expires_at"] else "Бессрочно"
    desc = sp["description"] or "Нет описания"

    text = f"⚙️ <b>Управление спонсором</b>\n\nID: <code>{sp['channel_id']}</code>\nНазвание: <b>{sp['name']}</b>\n\n📝 Описание: {desc}\n⏳ Истекает: {exp}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить Имя", callback_data=f"crm_sp_edit:{cid}:name")],
        [InlineKeyboardButton(text="📝 Изменить Описание", callback_data=f"crm_sp_edit:{cid}:desc")],
        [InlineKeyboardButton(text="⏳ Изменить Таймер", callback_data=f"crm_sp_edit:{cid}:days")],
        [InlineKeyboardButton(text="❌ Удалить спонсора", callback_data=f"crm_sp_del:{cid}")],
        [InlineKeyboardButton(text="🔙 К списку", callback_data="crm_sponsors")]
    ])
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_sp_edit:"))
async def cq_sp_edit(cb: CallbackQuery, state: FSMContext):
    _, cid, field = cb.data.split(":")
    await state.update_data(edit_cid=cid)
    
    if field == "name":
        await state.set_state(AdminPanel.wait_edit_sp_name)
        await cb.message.edit_text("Отправьте новое название канала (для плашки):")
    elif field == "desc":
        await state.set_state(AdminPanel.wait_edit_sp_desc)
        await cb.message.edit_text("Отправьте новое локальное описание (или /skip для удаления описания):")
    elif field == "days":
        await state.set_state(AdminPanel.wait_edit_sp_days)
        await cb.message.edit_text("Отправьте количество дней нахождения в спонсорах (цифрой, или /skip для бессрочного):")

@dp.message(AdminPanel.wait_edit_sp_name)
async def edit_sp_name(message: Message, state: FSMContext):
    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sponsors SET name=? WHERE channel_id=?", (message.text.strip(), data["edit_cid"]))
        await db.commit()
    await state.clear()
    await message.answer("✅ Название обновлено.\nНажмите /panel для возврата.")

@dp.message(AdminPanel.wait_edit_sp_desc)
async def edit_sp_desc(message: Message, state: FSMContext):
    data = await state.get_data()
    val = None if message.text.strip() == "/skip" else message.text.strip()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sponsors SET description=? WHERE channel_id=?", (val, data["edit_cid"]))
        await db.commit()
    await state.clear()
    await message.answer("✅ Описание обновлено.\nНажмите /panel для возврата.")

@dp.message(AdminPanel.wait_edit_sp_days)
async def edit_sp_days(message: Message, state: FSMContext):
    data = await state.get_data()
    val = message.text.strip()
    exp = None
    if val != "/skip":
        try:
            days = int(val)
            exp = (datetime.utcnow() + timedelta(days=days)).isoformat()
        except:
            return await message.answer("Пожалуйста, отправьте только число или /skip.")
            
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sponsors SET expires_at=?, notified=0 WHERE channel_id=?", (exp, data["edit_cid"]))
        await db.commit()
    await state.clear()
    await message.answer("✅ Таймер обновлен.\nНажмите /panel для возврата.")

@dp.callback_query(F.data.startswith("crm_sp_del:"))
async def cq_sp_del(cb: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sponsors WHERE channel_id=?", (cb.data.split(":")[1],))
        await db.commit()
    await cb.answer("🗑 Спонсор удален!", show_alert=True)
    await cq_sponsors(cb)

# Добавление спонсора
@dp.callback_query(F.data == "crm_sp_add")
async def cq_sp_add(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_sponsor_id)
    await cb.message.edit_text("1️⃣ Отправьте числовой ID канала (например: <code>-100123456789</code>).\n<i>Бот должен быть админом в этом канале!</i>", parse_mode=ParseMode.HTML)

@dp.message(AdminPanel.wait_sponsor_id)
async def sp_id_step(message: Message, state: FSMContext):
    await state.update_data(channel_id=message.text.strip())
    await state.set_state(AdminPanel.wait_sponsor_name)
    await message.answer("2️⃣ Отправьте красивое название канала (для плашки):")

@dp.message(AdminPanel.wait_sponsor_name)
async def sp_name_step(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminPanel.wait_sponsor_url)
    await message.answer("3️⃣ Отправьте ссылку на канал (можно приватную <code>https://t.me/+...</code>):", parse_mode=ParseMode.HTML)

@dp.message(AdminPanel.wait_sponsor_url)
async def sp_url_step(message: Message, state: FSMContext):
    await state.update_data(url=message.text.strip())
    await state.set_state(AdminPanel.wait_sponsor_photo)
    await message.answer("4️⃣ Отправьте ФОТО (картинку) для аватарки канала (или нажмите /skip).")

@dp.message(AdminPanel.wait_sponsor_photo)
async def sp_photo_step(message: Message, state: FSMContext):
    filename = ""
    if message.photo:
        filename = f"sponsor_{uuid.uuid4().hex[:8]}.jpg"
        await bot.download(message.photo[-1].file_id, destination=SPONSORS_DIR / filename)
    await state.update_data(filename=filename)
    await state.set_state(AdminPanel.wait_sponsor_desc)
    await message.answer("5️⃣ Напишите ЛОКАЛЬНОЕ ОПИСАНИЕ канала (зачем на него подписаться), или нажмите /skip:")

@dp.message(AdminPanel.wait_sponsor_desc)
async def sp_desc_step(message: Message, state: FSMContext):
    desc = None if message.text.strip() == "/skip" else message.text.strip()
    await state.update_data(desc=desc)
    await state.set_state(AdminPanel.wait_sponsor_days)
    await message.answer("6️⃣ Сколько дней канал будет в спонсорах? Отправьте цифру (например: 7), или нажмите /skip (будет висеть бессрочно):")

@dp.message(AdminPanel.wait_sponsor_days)
async def sp_days_step(message: Message, state: FSMContext):
    data = await state.get_data()
    val = message.text.strip()
    exp = None
    if val != "/skip":
        try:
            exp = (datetime.utcnow() + timedelta(days=int(val))).isoformat()
        except:
            return await message.answer("Пожалуйста, отправьте только цифру или /skip.")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO sponsors (channel_id, name, url, avatar_filename, description, expires_at) VALUES (?,?,?,?,?,?)", (data["channel_id"], data["name"], data["url"], data["filename"], data["desc"], exp))
        await db.commit()
    
    await state.clear()
    await message.answer("✅ <b>Спонсор успешно добавлен!</b>\nНажмите /panel для возврата.", parse_mode=ParseMode.HTML)


# --- Тикеты CRM ---
@dp.callback_query(F.data == "crm_tickets")
async def cq_tickets(cb: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT t.id, t.user_id, p.username FROM tickets t LEFT JOIN players p ON t.user_id=p.user_id WHERE t.status='open' LIMIT 10") as cur:
            tkts = await cur.fetchall()
            
    if not tkts: return await cb.message.edit_text("Нет новых (открытых) тикетов.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]))
    
    kb = [[InlineKeyboardButton(text=f"Смотреть #{t['id']} (@{t['username'] or t['user_id']})", callback_data=f"crm_tview:{t['id']}")] for t in tkts]
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")])
    await cb.message.edit_text("🎧 <b>Открытые тикеты:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("crm_tview:"))
async def cq_tview(cb: CallbackQuery):
    tkt_id = int(cb.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT t.id, t.user_id, t.status, p.username FROM tickets t LEFT JOIN players p ON t.user_id=p.user_id WHERE t.id=?", (tkt_id,)) as cur:
            tkt = await cur.fetchone()
        if not tkt or tkt["status"] != "open": return await cb.answer("Уже взят или закрыт.", show_alert=True)
        
        async with db.execute("SELECT text FROM support_messages WHERE ticket_id=? AND direction='in' ORDER BY created_at DESC LIMIT 1", (tkt_id,)) as cur:
            msg = await cur.fetchone()
            msg_text = msg["text"] if msg else "Нет текста"

    uname = tkt["username"] or tkt["user_id"]
    text = f"📨 <b>Новый Тикет #{tkt_id}</b> от @{uname}\n\nПоследнее сообщение:\n<i>{msg_text}</i>\n\nВыберите действие:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🙋‍♂️ Взять в работу", callback_data=f"crm_tclaim:{tkt_id}")],
        [InlineKeyboardButton(text="🔙 К списку", callback_data="crm_tickets")]
    ])
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_tclaim:"))
async def cq_tclaim(cb: CallbackQuery):
    tkt_id = int(cb.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id, status FROM tickets WHERE id=?", (tkt_id,)) as cur:
            tkt = await cur.fetchone()
        if not tkt or tkt["status"] != "open": return await cb.answer("Уже взят или закрыт.", show_alert=True)
        
        await db.execute("UPDATE tickets SET status='claimed', claimed_by=? WHERE id=?", (cb.from_user.id, tkt_id))
        
        admin_name = cb.from_user.username or "Admin"
        system_text = f"👨‍💻 Агент поддержки @{admin_name} подключился к диалогу."
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, tkt["user_id"], system_text))
        await db.commit()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 Закрыть тикет", callback_data=f"crm_tclose:{tkt_id}")]
    ])
    await cb.message.edit_text(f"✅ Вы взяли <b>Тикет #{tkt_id}</b> в работу.\nТеперь просто <b>ответьте (Reply)</b> на любое сообщение пользователя из этого тикета.", parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_tclose:"))
async def cq_tclose(cb: CallbackQuery):
    tkt_id = int(cb.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM tickets WHERE id=?", (tkt_id,)) as cur:
            uid = (await cur.fetchone())[0]
        await db.execute("UPDATE tickets SET status='closed' WHERE id=?", (tkt_id,))
        system_text = "✅ Диалог завершен. Если остались вопросы — отправьте новое сообщение."
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?, 'system')", (tkt_id, uid, system_text))
        await db.commit()
    
    await cb.message.edit_text(f"✅ Тикет #{tkt_id} закрыт.")

# --- Выводы CRM ---
@dp.callback_query(F.data == "crm_wd")
async def cq_wd(cb: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT wr.*, p.username FROM withdrawal_requests wr LEFT JOIN players p ON p.user_id = wr.user_id WHERE wr.status='pending' ORDER BY wr.is_priority DESC, wr.created_at ASC LIMIT 10") as cur:
            wds = await cur.fetchall()
            
    if not wds: return await cb.message.edit_text("Нет активных заявок на вывод.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]))
    
    text = "💳 <b>Заявки на вывод (топ 10):</b>\n\n"
    kb = []
    for w in wds:
        prio = "⭐ " if w["is_priority"] else ""
        amt = f"${w['amount_usd']:.2f}" if w['method']=='usd' else f"{w['stars']} ⭐"
        uname = f"@{w['username']}" if w['username'] else f"ID {w['user_id']}"
        text += f"Req <code>{w['id']}</code> | {uname} | {prio}<b>{amt}</b>\n"
        kb.append([
            InlineKeyboardButton(text=f"✅ Выплачено #{w['id']}", callback_data=f"crm_wdok:{w['id']}"),
            InlineKeyboardButton(text=f"❌ Отклонить #{w['id']}", callback_data=f"crm_wdrej_ask:{w['id']}")
        ])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")])
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("crm_wdok:"))
async def cq_wdok(cb: CallbackQuery):
    wid = cb.data.split(":")[1]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE withdrawal_requests SET status='completed' WHERE id=?", (wid,))
        await db.commit()
    await cb.answer("✅ Отмечено как выплаченное", show_alert=True)
    await cq_wd(cb)

@dp.callback_query(F.data.startswith("crm_wdrej_ask:"))
async def cq_wdrej_ask(cb: CallbackQuery):
    wid = int(cb.data.split(":")[1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📣 Да, уведомить юзера", callback_data=f"crm_wdrej_do:{wid}:notify")],
        [InlineKeyboardButton(text="🤫 Нет, тихо отклонить", callback_data=f"crm_wdrej_do:{wid}:silent")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_wd")]
    ])
    await cb.message.edit_text(f"Вы отклоняете заявку #{wid}. Баланс будет возвращен пользователю.\n\nУведомить пользователя сообщением?", reply_markup=kb)

@dp.callback_query(F.data.startswith("crm_wdrej_do:"))
async def cq_wdrej_do(cb: CallbackQuery):
    _, wid, mode = cb.data.split(":")
    wid = int(wid)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, amount_usd FROM withdrawal_requests WHERE id=?", (wid,)) as cur:
            req = await cur.fetchone()
        await db.execute("UPDATE withdrawal_requests SET status='rejected' WHERE id=?", (wid,))
        await db.execute("UPDATE players SET balance=balance+? WHERE user_id=?", (req[1], req[0]))
        await db.commit()
    
    if mode == "notify":
        p = await get_player(req[0])
        try: await bot.send_message(req[0], tr(p["lang"] if p else "en", "wd_rejected"), parse_mode=ParseMode.HTML)
        except: pass
        await cb.answer("❌ ОТКЛОНЕНА, юзер уведомлен.", show_alert=True)
    else:
        await cb.answer("❌ ОТКЛОНЕНА тихо.", show_alert=True)
        
    await cq_wd(cb)

# --- Рассылка ---
@dp.callback_query(F.data == "crm_broadcast")
async def cq_broad(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_broadcast)
    await cb.message.edit_text("Отправьте текст для рассылки всем пользователям:")

@dp.message(AdminPanel.wait_broadcast)
async def broad_step(message: Message, state: FSMContext):
    user_ids = await get_all_user_ids()
    delivered = 0
    await message.answer(f"📣 Начинаем рассылку ({len(user_ids)} юзеров)...")
    for uid in user_ids:
        try:
            await bot.send_message(uid, message.html_text, parse_mode=ParseMode.HTML)
            delivered += 1
        except: pass
        await asyncio.sleep(0.05)
    await message.answer(f"✅ Рассылка завершена. Доставлено: {delivered}")
    await state.clear()


# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    try: await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True, request_timeout=30)
    except: pass
    
    try:
        global _bot_username
        me = await bot.get_me()
        _bot_username = me.username
        with open(_BOT_USERNAME_CACHE, "w") as f: f.write(_bot_username)
    except Exception as e:
        logger.error(f"Failed to get bot username: {e}")

    t1 = asyncio.create_task(auction_worker())
    t2 = asyncio.create_task(reminder_worker())
    t3 = asyncio.create_task(monitor_withdrawals_worker())
    t4 = asyncio.create_task(sponsor_expiry_worker())
    yield
    t1.cancel(); t2.cancel(); t3.cancel(); t4.cancel()
    try: await bot.delete_webhook()
    except: pass

app = FastAPI(title="PhotoFlip CRM API", lifespan=lifespan)
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
async def api_get_player(user_id: int, username: str = ""):
    player, _ = await get_or_create_player(user_id, username)
    lang = player.get("lang", "en")
    
    if not await is_admin(user_id) and not await is_subscribed_to_channel(REQUIRED_CHANNEL_ID, user_id):
        return JSONResponse(status_code=402, content={"error": "subscription_required", "channels": [{"id": REQUIRED_CHANNEL_ID, "url": REQUIRED_CHANNEL_URL, "name": REQUIRED_CHANNEL_NAME}], "message": tr(lang, "sub_required_ru") if lang == "ru" else tr(lang, "sub_required_en")})

    ref_count = await get_referral_count(user_id)
    player["referrals_count"] = ref_count
    
    if username and username != player.get("username"):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE players SET username=? WHERE user_id=?", (username, user_id))
            await db.commit()
            
    # Вычисление данных для колеса
    can_spin = False
    next_spin_ms = 0
    if player.get("last_spin"):
        try:
            last = datetime.fromisoformat(player["last_spin"])
            diff = (last + timedelta(hours=24)) - datetime.utcnow()
            if diff.total_seconds() <= 0: can_spin = True
            else: next_spin_ms = int(diff.total_seconds() * 1000)
        except: can_spin = True
    else:
        can_spin = True
        
    return {
        "player": player,
        "photos": await get_player_photos(user_id, lang),
        "withdraw_unlocked": ref_count >= MIN_REFERRALS_WITHDRAW,
        "vip_level": vip_level(ref_count),
        "vip_tiers": [{"min": t[0], "max_delay": t[1], "slots": t[2]} for t in VIP_TIERS],
        "referral_url": await referral_url(user_id),
        "rub_rate": RUB_TO_USD_RATE,
        "active_slots": await get_active_photo_count(user_id),
        "slot_limit": vip_slot_limit(ref_count) + (player.get("extra_slots") or 0),
        "min_referrals_withdraw": MIN_REFERRALS_WITHDRAW,
        "withdraw_condition": tr(lang, "withdraw_locked"),
        "vip_priority_note": tr(lang, "vip_priority"),
        "wheel": {"can_spin": can_spin, "next_spin_ms": next_spin_ms}
    }

@app.post("/api/wheel/spin")
async def api_wheel_spin(request: Request):
    data = await request.json()
    user_id = data.get("user_id")
    player = await get_player(user_id)
    if not player: raise HTTPException(404)
    
    if player.get("last_spin"):
        last = datetime.fromisoformat(player["last_spin"])
        if datetime.utcnow() < last + timedelta(hours=24):
            raise HTTPException(403, "Spin cooldown active.")

    rand = random.uniform(0, 100)
    current = 0
    prize = WHEEL_PRIZES[1] # Default to lose
    for p in WHEEL_PRIZES:
        current += p["chance"]
        if rand <= current:
            prize = p
            break

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET last_spin=datetime('now') WHERE user_id=?", (user_id,))
        if prize["type"] == "usd":
            await db.execute("UPDATE players SET balance=balance+?, total_earned=total_earned+? WHERE user_id=?", (prize["val"], prize["val"], user_id))
        elif prize["type"] == "slot":
            await db.execute("UPDATE players SET extra_slots=extra_slots+? WHERE user_id=?", (prize["val"], user_id))
        await db.commit()

    return {"success": True, "prize": prize}

@app.post("/api/buy/item")
async def api_buy_item(request: Request):
    data = await request.json()
    user_id = data.get("user_id")
    item = data.get("item")
    
    if item == "spin":
        title, desc, payload, price = "Extra Spin", "1 additional Wheel Spin", "buy_spin", 20
    elif item == "slots":
        title, desc, payload, price = "+5 Auction Slots", "Permanent slots for your photos", "buy_slots", 50
    else:
        raise HTTPException(400, "Invalid item")
        
    link = await bot.create_invoice_link(
        title=title,
        description=desc,
        payload=payload,
        provider_token="", # Для Telegram Stars (XTR) всегда пусто!
        currency="XTR",
        prices=[LabeledPrice(label=title, amount=price)]
    )
    return {"invoice_url": link}


@app.put("/api/player/{user_id}/lang")
async def api_set_lang(user_id: int, request: Request):
    lang = (await request.json()).get("lang", "en")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET lang=? WHERE user_id=?", (lang, user_id))
        await db.commit()
    return {"lang": lang}

@app.get("/api/referrals/{user_id}")
async def api_referrals(user_id: int):
    return {"referrals": await get_referral_list(user_id), "referrals_count": await get_referral_count(user_id), "referral_url": await referral_url(user_id)}

@app.post("/api/upload")
async def api_upload(user_id: int = Form(...), username: str = Form(""), files: List[UploadFile] = File(...)):
    player, _ = await get_or_create_player(user_id, username)
    
    files_data = [(f.filename or "photo.jpg", await f.read()) for f in files]
    ref_count, active = await get_referral_count(user_id), await get_active_photo_count(user_id)
    slot_limit = vip_slot_limit(ref_count) + (player.get("extra_slots") or 0)
    
    if active + len(files_data) > slot_limit: raise HTTPException(403, "Slot limit reached.")

    is_pack = len(files_data) >= PACK_SIZE
    rub_each = [random.randint(PACK_MIN_RUB, PACK_MAX_RUB)//PACK_SIZE]*len(files_data) if is_pack else [random.randint(SINGLE_MIN_RUB, SINGLE_MAX_RUB) for _ in range(len(files_data))]
    batch_id, max_delay, results = uuid.uuid4().hex, vip_max_delay(ref_count), []

    async with aiosqlite.connect(DB_PATH) as db:
        for idx, (orig_name, raw) in enumerate(files_data):
            filename = f"{uuid.uuid4().hex}.jpg"
            (UPLOADS_DIR / filename).write_bytes(raw)
            sell_at = (datetime.utcnow() + timedelta(seconds=random.randint(MIN_DELAY_SECS, max(max_delay, MIN_DELAY_SECS + 1)))).isoformat()
            sale_rub, prev_usd, pid = rub_each[idx], apply_commission(rub_to_usd(rub_each[idx])), uuid.uuid4().hex
            await db.execute("INSERT INTO photos (id, user_id, filename, batch_id, base_price, final_price, sale_rub, status, sell_at) VALUES (?,?,?,?,?,?,?,'on_auction',?)", (pid, user_id, filename, batch_id, prev_usd, prev_usd, sale_rub, sell_at))
            results.append({"photo_id": pid, "filename": filename, "base_price": prev_usd, "preview_rub": sale_rub, "status": "on_auction"})
        await db.commit()
    return {"batch_id": batch_id, "is_pack": is_pack, "photos": results, "total_rub": sum(rub_each), "slot_limit": slot_limit, "active_after": active + len(files_data)}

@app.post("/api/withdraw/check")
async def api_withdraw_check(request: Request):
    user_id = (await request.json()).get("user_id")
    missing = await check_all_subs(user_id)
    if missing: return {"ok": False, "channels": missing}
    return {"ok": True}

@app.post("/api/withdraw")
@app.post("/api/withdraw/stars")
async def api_withdraw_both(request: Request):
    is_stars = "stars" in request.url.path
    user_id = (await request.json()).get("user_id")
    player = await get_player(user_id)
    if not player: raise HTTPException(404)
    lang, ref_count = player.get("lang", "en"), await get_referral_count(user_id)

    if ref_count < MIN_REFERRALS_WITHDRAW: raise HTTPException(403, tr(lang, "withdraw_locked"))
    if await check_all_subs(user_id): raise HTTPException(403, "Subscribe to sponsors first.")
    if (player["balance"] or 0) <= 0: raise HTTPException(400, "Empty balance.")

    amt_usd = round(player["balance"], 2)
    stars = usd_to_stars(amt_usd) if is_stars else 0
    prio = 1 if vip_level(ref_count) >= 1 else 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE players SET balance=0 WHERE user_id=?", (user_id,))
        await db.execute("INSERT INTO withdrawal_requests (user_id, amount_usd, stars, method, is_priority, status) VALUES (?,?,?,?,?,'pending')", (user_id, amt_usd, stars, "stars" if is_stars else "usd", prio))
        await db.commit()

    for aid in await get_admin_ids():
        try: await bot.send_message(aid, f"💳 <b>{'Stars' if is_stars else 'USD'} Withdrawal</b>\n{'⭐ VIP PRIORITY' if prio else ''}\nUser: <code>{user_id}</code>\nAmount: <b>{stars} ⭐</b>" if is_stars else f"💳 <b>USD Withdrawal</b>\n{'⭐ VIP PRIORITY' if prio else ''}\nUser: <code>{user_id}</code>\nAmount: <b>${amt_usd:.2f}</b>", parse_mode=ParseMode.HTML)
        except: pass

    return {"success": True, "message": tr(lang, "withdraw_processing")}

@app.post("/api/support/send")
async def api_support_send(request: Request):
    data = await request.json()
    user_id, text = data.get("user_id"), data.get("text", "").strip()
    if not user_id or not text: raise HTTPException(400)
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, claimed_by FROM tickets WHERE user_id=? AND status IN ('open', 'claimed')", (user_id,)) as cur:
            tkt = await cur.fetchone()
        
        if not tkt:
            await db.execute("INSERT INTO tickets (user_id) VALUES (?)", (user_id,))
            await db.commit()
            async with db.execute("SELECT last_insert_rowid()") as cur:
                tkt_id = (await cur.fetchone())[0]
            claimed_by = None
        else:
            tkt_id, claimed_by = tkt["id"], tkt["claimed_by"]
            
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?,'in')", (tkt_id, user_id, text))
        await db.commit()
        
    p = await get_player(user_id)
    uname = p["username"] if p else str(user_id)

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔒 Закрыть тикет", callback_data=f"crm_tclose:{tkt_id}")]])

    if claimed_by:
        try: await bot.send_message(claimed_by, f"💬 <b>Тикет #{tkt_id}</b> | @{uname}\n\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
        except: pass
    else:
        admin_ids = await get_admin_ids()
        for aid in admin_ids:
            try: await bot.send_message(aid, f"🆘 <b>Новый Тикет #{tkt_id}</b>\nОт: @{uname}\n\n{text[:300]}", parse_mode=ParseMode.HTML, reply_markup=kb)
            except: pass

    return {"success": True}

@app.get("/api/support/messages/{user_id}")
async def api_support_messages(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM support_messages WHERE user_id=? ORDER BY created_at ASC", (user_id,)) as cur:
            rows = await cur.fetchall()
    return {"messages": [dict(r) for r in rows]}

@app.post("/api/referral/bind")
async def api_referral_bind(request: Request):
    data = await request.json()
    new_user_id, ref_param = data.get("user_id"), str(data.get("ref_param") or "").strip()
    if not new_user_id: raise HTTPException(400)
    referrer_id = int(ref_param[4:]) if ref_param.startswith("ref_") else int(ref_param) if ref_param.isdigit() else None
    
    await get_or_create_player(new_user_id, str(data.get("username") or ""))
    bound = False
    if referrer_id:
        bound = await _bind_referral(new_user_id, referrer_id)
    return {"bound": bound}

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    from aiogram.types import Update
    await dp.feed_update(bot, Update(**await request.json()))
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
