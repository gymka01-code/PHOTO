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

SPONSORS_DIR = Path(os.path.join(UPLOADS_DIR, "sponsors"))
SPONSORS_DIR.mkdir(parents=True, exist_ok=True)

_BOT_USERNAME_CACHE = os.path.join(_VOLUME, ".bot_username")

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  FSM STATES (Admin CRM)
# ═══════════════════════════════════════════════════════════════
class AdminPanel(StatesGroup):
    wait_broadcast_msg = State()
    wait_sponsor_id    = State()
    wait_sponsor_name  = State()
    wait_sponsor_url   = State()
    wait_sponsor_photo = State()
    wait_ticket_reply  = State()
    wait_close_ticket  = State()

# ═══════════════════════════════════════════════════════════════
#  TRANSLATIONS & HELPERS
# ═══════════════════════════════════════════════════════════════
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
        "ticket_claimed": "👨‍💻 <b>Admin @{admin} joined the chat.</b> Please describe your issue.",
        "ticket_closed": "✅ <b>Your ticket has been closed.</b> If you need further help, please send a new message."
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
        "ticket_claimed": "👨‍💻 <b>Администратор @{admin} подключился к диалогу.</b> Пожалуйста, опишите вашу проблему.",
        "ticket_closed": "✅ <b>Ваш запрос закрыт.</b> Если у вас остались вопросы, просто напишите новое сообщение."
    }
}

def tr(lang: str, key: str, **kw) -> str:
    tmpl = _T.get(lang, _T["en"]).get(key, _T["en"].get(key, key))
    return tmpl.format(**kw) if kw else tmpl

def rub_to_usd(rub: float) -> float: return round(rub / RUB_TO_USD_RATE, 2)
def apply_commission(usd: float) -> float: return round(usd * (1 - COMMISSION_PCT), 2)
def vip_level(refs: int) -> int: return next((i for i, (thr, _, _) in enumerate(VIP_TIERS) if refs >= thr), 0)
def vip_max_delay(refs: int) -> int: return VIP_TIERS[vip_level(refs)][1]
def vip_slot_limit(refs: int) -> int: return VIP_TIERS[vip_level(refs)][2]
def usd_to_stars(usd: float) -> int: return math.floor(usd / 0.012)

# ═══════════════════════════════════════════════════════════════
#  DATABASE INIT & CRM TABLES
# ═══════════════════════════════════════════════════════════════
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS players (
            user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0.0,
            total_earned REAL DEFAULT 0.0, photos_sold INTEGER DEFAULT 0,
            referrals_count INTEGER DEFAULT 0, referred_by INTEGER DEFAULT NULL,
            lang TEXT DEFAULT 'en', last_seen TEXT DEFAULT (datetime('now')),
            created_at TEXT DEFAULT (datetime('now'))
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS photos (
            id TEXT PRIMARY KEY, user_id INTEGER, filename TEXT, batch_id TEXT,
            base_price REAL, final_price REAL, sale_rub REAL DEFAULT 0,
            status TEXT DEFAULT 'pending', sell_at TEXT, sold_at TEXT,
            buyer TEXT, created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(user_id) REFERENCES players(user_id)
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS quests (
            user_id INTEGER, channel_id TEXT, completed INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, channel_id)
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS referrals (
            referrer_id INTEGER, referred_id INTEGER PRIMARY KEY, created_at TEXT DEFAULT (datetime('now'))
        )""")
        
        # --- CRM ТАБЛИЦЫ ---
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsors (
            channel_id TEXT PRIMARY KEY, name TEXT, url TEXT, avatar_filename TEXT, created_at TEXT DEFAULT (datetime('now'))
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, status TEXT DEFAULT 'open',
            claimed_by INTEGER DEFAULT NULL, created_at TEXT DEFAULT (datetime('now'))
        )""")

        await db.execute("""CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INTEGER, user_id INTEGER,
            text TEXT, direction TEXT, created_at TEXT DEFAULT (datetime('now'))
        )""")
        try: await db.execute("ALTER TABLE support_messages ADD COLUMN ticket_id INTEGER")
        except: pass
        
        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount_usd REAL,
            stars INTEGER DEFAULT 0, method TEXT, is_priority INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending', warning_sent_at TEXT DEFAULT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )""")

        await db.execute("""CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY, username TEXT, added_by INTEGER, created_at TEXT DEFAULT (datetime('now'))
        )""")
        await db.commit()

# ═══════════════════════════════════════════════════════════════
#  CRM HELPERS
# ═══════════════════════════════════════════════════════════════
async def get_sponsors():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM sponsors") as cur:
            rows = await cur.fetchall()
    
    result = []
    for r in rows:
        d = dict(r)
        # Формируем публичную ссылку на аватарку
        if d.get("avatar_filename"):
            d["avatar"] = f"{WEBAPP_URL}/uploads/sponsors/{d['avatar_filename']}"
        else:
            d["avatar"] = ""
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

async def create_or_get_ticket(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM tickets WHERE user_id=? AND status IN ('open', 'claimed')", (user_id,)) as cur:
            row = await cur.fetchone()
        if row: return row[0]
        
        await db.execute("INSERT INTO tickets (user_id) VALUES (?)", (user_id,))
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cur:
            return (await cur.fetchone())[0]

async def alert_admins_new_ticket(ticket_id: int, user_id: int, text: str, username: str):
    admin_ids = await get_admin_ids()
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🙋‍♂️ Взять в работу", callback_data=f"crm_claim:{ticket_id}")
    ]])
    uname = f"@{username}" if username else f"ID {user_id}"
    for aid in admin_ids:
        try:
            await bot.send_message(
                aid, f"🆘 <b>Новый тикет #{ticket_id}</b>\nОт: {uname}\n\n{text[:200]}...",
                parse_mode=ParseMode.HTML, reply_markup=kb
            )
        except: pass

# ═══════════════════════════════════════════════════════════════
#  PLAYER HELPERS (Truncated for brevity, standard logic)
# ═══════════════════════════════════════════════════════════════
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
    return [dict(r) | {"status_label": tr(lang, "status_auction") if r["status"] == "on_auction" else ""} for r in rows]

async def get_active_photo_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM photos WHERE user_id=? AND status='on_auction'", (user_id,)) as cur:
            return (await cur.fetchone())[0]

async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM players WHERE referred_by=?", (user_id,)) as cur:
            return (await cur.fetchone())[0]

async def get_referral_list(referrer_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT r.referred_id, r.created_at, p.username, CASE WHEN p.photos_sold > 0 THEN 1 ELSE 0 END AS is_active FROM referrals r LEFT JOIN players p ON p.user_id = r.referred_id WHERE r.referrer_id = ? ORDER BY r.created_at DESC", (referrer_id,)) as cur:
            return [dict(r) for r in await cur.fetchall()]

async def get_admin_ids() -> set[int]:
    ids: set[int] = {ADMIN_ID} if ADMIN_ID else set()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM admins") as cur:
            ids.update(r[0] for r in await cur.fetchall())
    return ids

async def is_admin(user_id: int) -> bool:
    if ADMIN_ID and user_id == ADMIN_ID: return True
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) as cur:
            return bool(await cur.fetchone())

# ═══════════════════════════════════════════════════════════════
#  WORKERS
# ═══════════════════════════════════════════════════════════════
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
        except: pass
        await asyncio.sleep(15)

async def monitor_withdrawals_worker():
    while True:
        await asyncio.sleep(3600)
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
                async with db.execute("SELECT id, user_id, warning_sent_at FROM withdrawal_requests WHERE status='pending' AND created_at > ?", (seven_days_ago,)) as cur:
                    requests = await cur.fetchall()

                for req in requests:
                    uid, wid, warn_time = req["user_id"], req["id"], req["warning_sent_at"]
                    missing = await check_all_subs(uid)
                    p = await get_player(uid)
                    lang = p["lang"] if p else "en"

                    if missing and not warn_time:
                        try:
                            await bot.send_message(uid, tr(lang, "unsub_warning"), parse_mode=ParseMode.HTML)
                            await db.execute("UPDATE withdrawal_requests SET warning_sent_at=datetime('now') WHERE id=?", (wid,))
                            await db.commit()
                        except: pass
                    elif not missing and warn_time:
                        try:
                            await bot.send_message(uid, tr(lang, "resub_thanks"), parse_mode=ParseMode.HTML)
                            await db.execute("UPDATE withdrawal_requests SET warning_sent_at=NULL WHERE id=?", (wid,))
                            await db.commit()
                        except: pass
        except: pass

# ═══════════════════════════════════════════════════════════════
#  BOT ROUTING & ADMIN PANEL
# ═══════════════════════════════════════════════════════════════
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user = message.from_user
    args = (command.args or "").strip()
    referrer_id = int(args[4:]) if args.startswith("ref_") else int(args) if args.isdigit() else None
    
    await get_or_create_player(user.id, user.username or "")
    if referrer_id and referrer_id != user.id:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT referred_by FROM players WHERE user_id=?", (user.id,)) as cur:
                p = await cur.fetchone()
            if p and p[0] is None:
                await db.execute("UPDATE players SET referred_by=? WHERE user_id=?", (referrer_id, user.id))
                await db.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)", (referrer_id, user.id))
                await db.execute("UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?", (referrer_id,))
                await db.commit()
                try: await bot.send_message(referrer_id, REFERRAL_NOTIFY_TMPL.format(username=user.username or user.id, user_id=user.id), parse_mode=ParseMode.HTML)
                except: pass

    # Welcome msg
    p = await get_player(user.id)
    lang, ref_count = p["lang"], await get_referral_count(user.id)
    me = await bot.get_me()
    ref_url = f"https://t.me/{me.username}?start=ref_{user.id}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tr(lang, "btn_open"), web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    await message.answer(tr(lang, "welcome", balance=p["balance"], vip=vip_level(ref_count), slots=vip_slot_limit(ref_count), ref_url=ref_url), parse_mode=ParseMode.HTML, reply_markup=kb)

# --- ADMIN PANEL ---
@dp.message(Command("admin"))
@dp.message(Command("panel"))
async def cmd_admin(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    await state.clear()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤝 Спонсоры", callback_data="crm_sponsors_list"), InlineKeyboardButton(text="🎧 Тикеты", callback_data="crm_tickets_list")],
        [InlineKeyboardButton(text="💳 Выводы", callback_data="crm_wd_list"), InlineKeyboardButton(text="📢 Рассылка", callback_data="crm_broadcast")],
    ])
    await message.answer("👑 <b>Админ Панель CRM</b>\nВыберите раздел:", parse_mode=ParseMode.HTML, reply_markup=kb)

# --- Спонсоры ---
@dp.callback_query(F.data == "crm_sponsors_list")
async def cq_sponsors_list(cb: CallbackQuery):
    sponsors = await get_sponsors()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ Удалить {s['name']}", callback_data=f"crm_sponsor_del:{s['channel_id']}")] for s in sponsors
    ] + [[InlineKeyboardButton(text="➕ Добавить спонсора", callback_data="crm_sponsor_add")], [InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]])
    await cb.message.edit_text(f"🤝 <b>Спонсоры (Всего: {len(sponsors)})</b>\n\nЗдесь можно добавить или удалить каналы.", parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data == "crm_sponsor_add")
async def cq_sponsor_add(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPanel.wait_sponsor_id)
    await cb.message.edit_text("1️⃣ Отправьте числовой ID канала (например: <code>-100123456789</code>).\n<i>Не забудьте добавить бота в этот канал администратором!</i>", parse_mode=ParseMode.HTML)

@dp.message(AdminPanel.wait_sponsor_id)
async def sponsor_id_step(message: Message, state: FSMContext):
    await state.update_data(channel_id=message.text.strip())
    await state.set_state(AdminPanel.wait_sponsor_name)
    await message.answer("2️⃣ Отправьте красивое название канала (оно будет в плашке):")

@dp.message(AdminPanel.wait_sponsor_name)
async def sponsor_name_step(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminPanel.wait_sponsor_url)
    await message.answer("3️⃣ Отправьте ссылку на канал (можно приватную <code>https://t.me/+...</code>):", parse_mode=ParseMode.HTML)

@dp.message(AdminPanel.wait_sponsor_url)
async def sponsor_url_step(message: Message, state: FSMContext):
    await state.update_data(url=message.text.strip())
    await state.set_state(AdminPanel.wait_sponsor_photo)
    await message.answer("4️⃣ Отправьте ФОТО (картинку) для аватарки канала (или нажмите /skip, если без фото).")

@dp.message(AdminPanel.wait_sponsor_photo)
async def sponsor_photo_step(message: Message, state: FSMContext):
    data = await state.get_data()
    filename = ""
    if message.photo:
        photo = message.photo[-1]
        filename = f"sponsor_{uuid.uuid4().hex[:8]}.jpg"
        file_path = SPONSORS_DIR / filename
        await bot.download(photo.file_id, destination=file_path)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO sponsors (channel_id, name, url, avatar_filename) VALUES (?,?,?,?)",
                         (data["channel_id"], data["name"], data["url"], filename))
        await db.commit()
    
    await state.clear()
    await message.answer("✅ <b>Спонсор успешно добавлен!</b>\nНажмите /admin для возврата.", parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("crm_sponsor_del:"))
async def cq_sponsor_del(cb: CallbackQuery):
    ch_id = cb.data.split(":")[1]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sponsors WHERE channel_id=?", (ch_id,))
        await db.commit()
    await cb.answer("🗑 Спонсор удален!", show_alert=True)
    await cq_sponsors_list(cb)

# --- Поддержка (Тикеты) ---
@dp.callback_query(F.data.startswith("crm_claim:"))
async def cq_claim_ticket(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id): return
    ticket_id = int(cb.data.split(":")[1])
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)) as cur:
            tkt = await cur.fetchone()
        if not tkt: return await cb.answer("Тикет не найден.")
        if tkt["status"] != "open": return await cb.answer("Тикет уже взят другим админом или закрыт.", show_alert=True)
        
        await db.execute("UPDATE tickets SET status='claimed', claimed_by=? WHERE id=?", (cb.from_user.id, ticket_id))
        await db.commit()

    admin_name = cb.from_user.username or cb.from_user.first_name
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✍️ Ответить", callback_data=f"crm_reply:{ticket_id}")],
        [InlineKeyboardButton(text="🔒 Закрыть тикет", callback_data=f"crm_close:{ticket_id}")]
    ])
    await cb.message.edit_text(cb.message.html_text + f"\n\n<i>✅ Взят в работу: @{admin_name}</i>", reply_markup=kb, parse_mode=ParseMode.HTML)
    
    p = await get_player(tkt["user_id"])
    if p:
        try: await bot.send_message(tkt["user_id"], tr(p["lang"], "ticket_claimed", admin=admin_name), parse_mode=ParseMode.HTML)
        except: pass

@dp.callback_query(F.data.startswith("crm_reply:"))
async def cq_reply_ticket(cb: CallbackQuery, state: FSMContext):
    ticket_id = int(cb.data.split(":")[1])
    await state.set_state(AdminPanel.wait_ticket_reply)
    await state.update_data(ticket_id=ticket_id)
    await cb.message.reply("✍️ Напишите текст ответа. Для отмены отправьте /cancel")
    await cb.answer()

@dp.message(AdminPanel.wait_ticket_reply)
async def ticket_reply_step(message: Message, state: FSMContext):
    data = await state.get_data()
    ticket_id = data["ticket_id"]
    text = message.text or message.caption or ""
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM tickets WHERE id=?", (ticket_id,)) as cur:
            uid = (await cur.fetchone())[0]
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?,'out')", (ticket_id, uid, text))
        await db.commit()
    
    p = await get_player(uid)
    try:
        await bot.send_message(uid, tr(p["lang"] if p else "en", "support_reply", text=text), parse_mode=ParseMode.HTML)
        await message.answer("✅ Ответ отправлен пользователю.")
    except:
        await message.answer("❌ Ошибка доставки.")
    await state.clear()

@dp.callback_query(F.data.startswith("crm_close:"))
async def cq_close_ticket(cb: CallbackQuery, state: FSMContext):
    ticket_id = int(cb.data.split(":")[1])
    await state.set_state(AdminPanel.wait_close_ticket)
    await state.update_data(ticket_id=ticket_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, уведомить", callback_data="close_notify_yes"), InlineKeyboardButton(text="Нет, молча", callback_data="close_notify_no")]
    ])
    await cb.message.answer("Вы хотите закрыть тикет. Уведомить пользователя стандартным текстом?", reply_markup=kb)
    await cb.answer()

@dp.callback_query(AdminPanel.wait_close_ticket, F.data.startswith("close_notify_"))
async def close_ticket_step(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    ticket_id = data["ticket_id"]
    notify = cb.data == "close_notify_yes"
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM tickets WHERE id=?", (ticket_id,)) as cur:
            uid = (await cur.fetchone())[0]
        await db.execute("UPDATE tickets SET status='closed' WHERE id=?", (ticket_id,))
        await db.commit()
    
    if notify:
        p = await get_player(uid)
        try: await bot.send_message(uid, tr(p["lang"] if p else "en", "ticket_closed"), parse_mode=ParseMode.HTML)
        except: pass

    await cb.message.edit_text("✅ Тикет успешно закрыт.")
    await state.clear()

# --- Выводы ---
@dp.callback_query(F.data == "crm_wd_list")
async def cq_wd_list(cb: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM withdrawal_requests WHERE status='pending' ORDER BY is_priority DESC, created_at ASC LIMIT 10") as cur:
            wds = await cur.fetchall()
            
    if not wds: return await cb.message.edit_text("Нет активных заявок на вывод.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")]]))
    
    text = "💳 <b>Заявки на вывод (топ 10):</b>\n\n"
    kb = []
    for w in wds:
        prio = "⭐ " if w["is_priority"] else ""
        mth = w["method"].upper()
        amt = f"${w['amount_usd']:.2f}" if mth=='USD' else f"{w['stars']} ⭐"
        text += f"ID <code>{w['id']}</code> | Юзер <code>{w['user_id']}</code> | {prio}<b>{amt}</b>\n"
        kb.append([
            InlineKeyboardButton(text=f"✅ {w['id']}", callback_data=f"crm_wd_ok:{w['id']}"),
            InlineKeyboardButton(text=f"❌ {w['id']}", callback_data=f"crm_wd_rej:{w['id']}")
        ])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="crm_main")])
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("crm_wd_ok:"))
async def cq_wd_ok(cb: CallbackQuery):
    wid = int(cb.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE withdrawal_requests SET status='completed' WHERE id=?", (wid,))
        await db.commit()
    await cb.answer("Заявка отмечена как ВЫПОЛНЕНА", show_alert=True)
    await cq_wd_list(cb)

@dp.callback_query(F.data.startswith("crm_wd_rej:"))
async def cq_wd_rej(cb: CallbackQuery):
    wid = int(cb.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        # Вернем баланс
        async with db.execute("SELECT user_id, amount_usd FROM withdrawal_requests WHERE id=?", (wid,)) as cur:
            req = await cur.fetchone()
        await db.execute("UPDATE withdrawal_requests SET status='rejected' WHERE id=?", (wid,))
        await db.execute("UPDATE players SET balance=balance+? WHERE user_id=?", (req[1], req[0]))
        await db.commit()
        
        try: await bot.send_message(req[0], "❌ <b>Ваша заявка на вывод отклонена.</b> Средства возвращены на баланс.", parse_mode=ParseMode.HTML)
        except: pass
    await cb.answer("Заявка ОТКЛОНЕНА, баланс возвращен.", show_alert=True)
    await cq_wd_list(cb)

@dp.callback_query(F.data == "crm_main")
async def cq_main(cb: CallbackQuery, state: FSMContext):
    await cmd_admin(cb.message, state)
    try: await cb.message.delete()
    except: pass

# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    try: await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True, request_timeout=30)
    except: pass
    t1 = asyncio.create_task(auction_worker())
    t2 = asyncio.create_task(reminder_worker())
    t3 = asyncio.create_task(monitor_withdrawals_worker())
    yield
    t1.cancel(); t2.cancel(); t3.cancel()
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
    
    # Check sponsors silently for the quest ticks
    sponsors = await get_sponsors()
    async with aiosqlite.connect(DB_PATH) as db:
        for sp in sponsors:
            ok = await is_subscribed_to_channel(sp["channel_id"], user_id)
            await db.execute("INSERT OR REPLACE INTO quests (user_id, channel_id, completed) VALUES (?,?,?)", (user_id, sp["channel_id"], int(ok)))
        await db.commit()

    ref_count = await get_referral_count(user_id)
    player["referrals_count"] = ref_count
    await touch_last_seen(user_id)

    return {
        "player": player,
        "photos": await get_player_photos(user_id, lang),
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
    if (player["balance"] or 0) > 0: raise HTTPException(403, "Withdraw balance first.")
    
    files_data = [(f.filename or "photo.jpg", await f.read()) for f in files]
    ref_count, active = await get_referral_count(user_id), await get_active_photo_count(user_id)
    slot_limit = vip_slot_limit(ref_count)
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
    if missing:
        return {"ok": False, "channels": missing}
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
        await db.execute("INSERT INTO withdrawal_requests (user_id, amount_usd, stars, method, is_priority) VALUES (?,?,?,?,?)", (user_id, amt_usd, stars, "stars" if is_stars else "usd", prio))
        await db.commit()

    return {"success": True, "message": tr(lang, "withdraw_processing")}

@app.post("/api/support/send")
async def api_support_send(request: Request):
    data = await request.json()
    user_id, text = data.get("user_id"), data.get("text", "").strip()
    
    ticket_id = await create_or_get_ticket(user_id)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction) VALUES (?,?,?,'in')", (ticket_id, user_id, text))
        
        # Check if claimed
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT claimed_by FROM tickets WHERE id=?", (ticket_id,)) as cur:
            tkt = await cur.fetchone()
        await db.commit()

    player = await get_player(user_id)
    uname = player["username"] if player else ""

    if tkt and tkt["claimed_by"]:
        # Alert specific admin
        try: await bot.send_message(tkt["claimed_by"], f"📨 <b>Ответ в тикете #{ticket_id}</b> от @{uname}:\n\n{text}", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✍️ Ответить", callback_data=f"crm_reply:{ticket_id}")]]))
        except: pass
    else:
        # Alert all admins
        await alert_admins_new_ticket(ticket_id, user_id, text, uname)

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
    if not referrer_id or referrer_id == new_user_id: return {"bound": False}
    await get_or_create_player(new_user_id, str(data.get("username") or ""))
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT referred_by FROM players WHERE user_id=?", (new_user_id,)) as cur:
            if (await cur.fetchone())[0] is None:
                await db.execute("UPDATE players SET referred_by=? WHERE user_id=?", (referrer_id, new_user_id))
                await db.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)", (referrer_id, new_user_id))
                await db.execute("UPDATE players SET referrals_count=referrals_count+1 WHERE user_id=?", (referrer_id,))
                await db.commit()
                return {"bound": True}
    return {"bound": False}

@app.post(WEBHOOK_PATH)
async def bot_webhook(request: Request):
    from aiogram.types import Update
    await dp.feed_update(bot, Update(**await request.json()))
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)