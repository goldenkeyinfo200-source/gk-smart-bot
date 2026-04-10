import os
import re
import json
import uuid
import html
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("gk_crm_pro")

# =========================================================
# ENV
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "").strip()
CONTACT_PHONE = os.getenv("CONTACT_PHONE", "+998999997973").strip()

# Credentials: 3 xil variantni қабул қилади
GOOGLE_SERVICE_FILE = os.getenv("GOOGLE_SERVICE_FILE", "").strip()
GSPREAD_CREDENTIALS_JSON = os.getenv("GSPREAD_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN topilmadi")
if not SPREADSHEET_URL:
    raise RuntimeError("SPREADSHEET_URL topilmadi")

# =========================================================
# BOT / DP
# =========================================================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# =========================================================
# FSM
# =========================================================
class LeadForm(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_service = State()
    waiting_region = State()
    waiting_district = State()
    waiting_note = State()

class SpecialAgentForm(StatesGroup):
    waiting_name = State()
    waiting_phone = State()

# =========================================================
# GOOGLE SHEETS
# =========================================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_gspread_client():
    # 1) GSPREAD_CREDENTIALS_JSON
    if GSPREAD_CREDENTIALS_JSON:
        try:
            info = json.loads(GSPREAD_CREDENTIALS_JSON)
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
            logger.info("Using GSPREAD_CREDENTIALS_JSON")
            return gspread.authorize(creds)
        except Exception as e:
            logger.exception("GSPREAD_CREDENTIALS_JSON error: %s", e)

    # 2) GOOGLE_CREDENTIALS_JSON
    if GOOGLE_CREDENTIALS_JSON:
        try:
            info = json.loads(GOOGLE_CREDENTIALS_JSON)
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
            logger.info("Using GOOGLE_CREDENTIALS_JSON")
            return gspread.authorize(creds)
        except Exception as e:
            logger.exception("GOOGLE_CREDENTIALS_JSON error: %s", e)

    # 3) GOOGLE_SERVICE_FILE
    if GOOGLE_SERVICE_FILE and os.path.exists(GOOGLE_SERVICE_FILE):
        try:
            creds = Credentials.from_service_account_file(GOOGLE_SERVICE_FILE, scopes=SCOPES)
            logger.info("Using GOOGLE_SERVICE_FILE")
            return gspread.authorize(creds)
        except Exception as e:
            logger.exception("GOOGLE_SERVICE_FILE error: %s", e)

    raise RuntimeError(
        "Google credentials topilmadi. "
        "GSPREAD_CREDENTIALS_JSON yoki GOOGLE_CREDENTIALS_JSON yoki GOOGLE_SERVICE_FILE kerak."
    )

gc = get_gspread_client()
sh = gc.open_by_url(SPREADSHEET_URL)

def get_or_create_ws(title: str, headers: List[str]):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=3000, cols=max(20, len(headers) + 5))
        ws.append_row(headers)
        return ws

    current_headers = ws.row_values(1)
    if not current_headers:
        ws.append_row(headers)
    return ws

AGENTS_HEADERS = [
    "agent_id", "full_name", "phone", "telegram_id", "username",
    "is_active", "created_at"
]
SPECIAL_AGENTS_HEADERS = [
    "special_agent_id", "full_name", "phone", "telegram_id", "username",
    "is_active", "bonus_note", "ref_code", "created_at"
]
LEADS_HEADERS = [
    "lead_id", "created_at", "client_name", "client_phone", "service",
    "region", "district", "note", "client_telegram_id", "client_username",
    "status", "assigned_agent_id", "assigned_agent_name", "assigned_at",
    "rejected_by", "completed_at", "special_agent_id", "special_agent_name",
    "source", "ref_code"
]
ADMINS_HEADERS = ["telegram_id", "full_name", "is_active", "created_at"]
LOGS_HEADERS = ["created_at", "level", "event", "details"]
SETTINGS_HEADERS = ["key", "value"]
BONUSES_HEADERS = [
    "bonus_id", "created_at", "lead_id", "special_agent_id", "special_agent_name",
    "special_agent_tg_id", "client_name", "service", "amount", "currency",
    "status", "paid_at", "paid_by", "note"
]

agents_ws = get_or_create_ws("Agents", AGENTS_HEADERS)
special_agents_ws = get_or_create_ws("SpecialAgents", SPECIAL_AGENTS_HEADERS)
leads_ws = get_or_create_ws("Leads", LEADS_HEADERS)
admins_ws = get_or_create_ws("ADMINS", ADMINS_HEADERS)
logs_ws = get_or_create_ws("Logs", LOGS_HEADERS)
settings_ws = get_or_create_ws("Settings", SETTINGS_HEADERS)
bonuses_ws = get_or_create_ws("Bonuses", BONUSES_HEADERS)

# =========================================================
# HELPERS
# =========================================================
def now_dt() -> datetime:
    return datetime.now()

def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")

def clean_text(v: Any) -> str:
    return str(v).strip() if v is not None else ""

def escape(s: Any) -> str:
    return html.escape(clean_text(s))

def normalize_phone(phone: str) -> str:
    phone = clean_text(phone)
    digits = re.sub(r"\D+", "", phone)

    if digits.startswith("998") and len(digits) >= 12:
        return "+" + digits[:12]
    if len(digits) == 9:
        return "+998" + digits
    if phone.startswith("+"):
        return phone
    return phone

def bool_from_sheet(value: Any) -> bool:
    return clean_text(value).lower() in ("true", "1", "yes", "ha", "active")

def generate_id(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"

def get_records(ws) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception as e:
        logger.error("get_all_records error (%s): %s", ws.title, e)
        return []

def find_row_by_value(ws, column_name: str, value: str) -> Optional[int]:
    records = get_records(ws)
    for idx, row in enumerate(records, start=2):
        if clean_text(row.get(column_name)) == clean_text(value):
            return idx
    return None

def update_cell_by_header(ws, row_index: int, header: str, value: Any):
    headers = ws.row_values(1)
    if header not in headers:
        raise ValueError(f"{ws.title} da '{header}' ustun topilmadi")
    col_index = headers.index(header) + 1
    ws.update_cell(row_index, col_index, value)

def append_log(level: str, event: str, details: str = ""):
    try:
        logs_ws.append_row([now_str(), level, event, details])
    except Exception as e:
        logger.error("log yozilmadi: %s", e)

def get_setting(key: str, default: str = "") -> str:
    for row in get_records(settings_ws):
        if clean_text(row.get("key")) == key:
            return clean_text(row.get("value"))
    if default != "":
        try:
            settings_ws.append_row([key, default])
        except Exception:
            pass
    return default

def set_setting_if_missing(key: str, value: str):
    for row in get_records(settings_ws):
        if clean_text(row.get("key")) == key:
            return
    settings_ws.append_row([key, value])

def init_default_settings():
    set_setting_if_missing("company_name", "Golden Key Smart AI")
    set_setting_if_missing("contact_phone", CONTACT_PHONE)
    set_setting_if_missing("special_bonus_amount", "100000")
    set_setting_if_missing("bonus_currency", "UZS")
    set_setting_if_missing(
        "special_bonus_text",
        "Siz yuborgan mijozning ishi yakunlandi. Bonusni ofisdan olib ketishingiz mumkin."
    )

init_default_settings()

# =========================================================
# MENUS
# =========================================================
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Заявка қолдириш")],
            [KeyboardButton(text="🤝 Махсус агент бўлиш")],
            [KeyboardButton(text="☎️ Алоқа")],
        ],
        resize_keyboard=True
    )

def services_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Уй сотиш"), KeyboardButton(text="🏠 Уй сотиб олиш")],
            [KeyboardButton(text="🏢 Ижарага бериш"), KeyboardButton(text="🏢 Ижарага олиш")],
            [KeyboardButton(text="🏦 Ипотека"), KeyboardButton(text="📄 Кадастр")],
            [KeyboardButton(text="⬅️ Орқага")],
        ],
        resize_keyboard=True
    )

def back_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Орқага")]],
        resize_keyboard=True
    )

def new_lead_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Олдим", callback_data=f"take:{lead_id}"),
                InlineKeyboardButton(text="❌ Рад этилди", callback_data=f"reject:{lead_id}")
            ]
        ]
    )

def taken_lead_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Бажарилди", callback_data=f"done:{lead_id}")]
        ]
    )

# =========================================================
# ADMINS
# =========================================================
def get_admin_ids() -> List[int]:
    ids = []
    for row in get_records(admins_ws):
        if bool_from_sheet(row.get("is_active")):
            tg = clean_text(row.get("telegram_id"))
            if tg.isdigit():
                ids.append(int(tg))
    return ids

def is_admin(user_id: int) -> bool:
    return user_id in get_admin_ids()

def add_admin_if_needed(user_id: int, full_name: str):
    idx = find_row_by_value(admins_ws, "telegram_id", str(user_id))
    if idx is None:
        admins_ws.append_row([str(user_id), full_name, "TRUE", now_str()])

# =========================================================
# AGENTS
# =========================================================
def get_active_agents() -> List[Dict[str, Any]]:
    rows = []
    for row in get_records(agents_ws):
        tg_id = clean_text(row.get("telegram_id"))
        is_active = clean_text(row.get("is_active")).lower()
        if tg_id.isdigit() and is_active in ("true", "1", "yes", "ha", "active"):
            rows.append(row)
    return rows

def get_agent_by_telegram_id(tg_id: int) -> Optional[Dict[str, Any]]:
    for row in get_records(agents_ws):
        if clean_text(row.get("telegram_id")) == str(tg_id):
            return row
    return None

def register_or_update_agent(agent_code: str, tg_id: int, full_name: str, username: str):
    row_idx = find_row_by_value(agents_ws, "telegram_id", str(tg_id))
    if row_idx:
        update_cell_by_header(agents_ws, row_idx, "agent_id", agent_code)
        update_cell_by_header(agents_ws, row_idx, "full_name", full_name)
        update_cell_by_header(agents_ws, row_idx, "username", username or "")
        update_cell_by_header(agents_ws, row_idx, "is_active", "TRUE")
        return

    code_idx = find_row_by_value(agents_ws, "agent_id", agent_code)
    if code_idx:
        update_cell_by_header(agents_ws, code_idx, "telegram_id", str(tg_id))
        update_cell_by_header(agents_ws, code_idx, "full_name", full_name)
        update_cell_by_header(agents_ws, code_idx, "username", username or "")
        update_cell_by_header(agents_ws, code_idx, "is_active", "TRUE")
        return

    agents_ws.append_row([
        agent_code,
        full_name,
        "",
        str(tg_id),
        username or "",
        "TRUE",
        now_str()
    ])

def get_agent_stats() -> Dict[str, Dict[str, int]]:
    stats: Dict[str, Dict[str, int]] = {}
    for lead in get_records(leads_ws):
        agent_id = clean_text(lead.get("assigned_agent_id"))
        agent_name = clean_text(lead.get("assigned_agent_name"))
        status = clean_text(lead.get("status")).lower()

        if not agent_id:
            continue

        if agent_id not in stats:
            stats[agent_id] = {
                "taken": 0,
                "completed": 0,
                "rejected": 0,
                "name": agent_name
            }

        if status == "taken":
            stats[agent_id]["taken"] += 1
        elif status == "completed":
            stats[agent_id]["completed"] += 1
        elif status == "rejected":
            stats[agent_id]["rejected"] += 1

    return stats

# =========================================================
# SPECIAL AGENTS
# =========================================================
def get_special_agent_by_tg_id(tg_id: int) -> Optional[Dict[str, Any]]:
    for row in get_records(special_agents_ws):
        if clean_text(row.get("telegram_id")) == str(tg_id):
            return row
    return None

def get_special_agent_by_ref_code(ref_code: str) -> Optional[Dict[str, Any]]:
    for row in get_records(special_agents_ws):
        if clean_text(row.get("ref_code")) == clean_text(ref_code):
            return row
    return None

def register_special_agent(full_name: str, phone: str, tg_id: int, username: str) -> Dict[str, Any]:
    existing = get_special_agent_by_tg_id(tg_id)
    if existing:
        row_idx = find_row_by_value(special_agents_ws, "telegram_id", str(tg_id))
        ref_code = clean_text(existing.get("ref_code")) or generate_id("ref_")
        update_cell_by_header(special_agents_ws, row_idx, "full_name", full_name)
        update_cell_by_header(special_agents_ws, row_idx, "phone", normalize_phone(phone))
        update_cell_by_header(special_agents_ws, row_idx, "username", username or "")
        update_cell_by_header(special_agents_ws, row_idx, "is_active", "TRUE")
        update_cell_by_header(special_agents_ws, row_idx, "ref_code", ref_code)
        return {
            "special_agent_id": clean_text(existing.get("special_agent_id")),
            "full_name": full_name,
            "phone": normalize_phone(phone),
            "telegram_id": str(tg_id),
            "username": username or "",
            "ref_code": ref_code,
        }

    special_agent_id = generate_id("sa_")
    ref_code = generate_id("ref_")
    special_agents_ws.append_row([
        special_agent_id,
        full_name,
        normalize_phone(phone),
        str(tg_id),
        username or "",
        "TRUE",
        "",
        ref_code,
        now_str(),
    ])
    return {
        "special_agent_id": special_agent_id,
        "full_name": full_name,
        "phone": normalize_phone(phone),
        "telegram_id": str(tg_id),
        "username": username or "",
        "ref_code": ref_code,
    }

# =========================================================
# LEADS
# =========================================================
def create_lead(data: Dict[str, Any]) -> str:
    lead_id = generate_id("lead_")
    leads_ws.append_row([
        lead_id,
        now_str(),
        data.get("client_name", ""),
        normalize_phone(data.get("client_phone", "")),
        data.get("service", ""),
        data.get("region", ""),
        data.get("district", ""),
        data.get("note", ""),
        str(data.get("client_telegram_id", "")),
        data.get("client_username", ""),
        "new",
        "",
        "",
        "",
        "",
        "",
        data.get("special_agent_id", ""),
        data.get("special_agent_name", ""),
        data.get("source", "bot"),
        data.get("ref_code", ""),
    ])
    return lead_id

def get_lead_by_id(lead_id: str) -> Optional[Tuple[int, Dict[str, Any]]]:
    for idx, row in enumerate(get_records(leads_ws), start=2):
        if clean_text(row.get("lead_id")) == clean_text(lead_id):
            return idx, row
    return None

def assign_lead(lead_id: str, agent_row: Dict[str, Any]) -> Tuple[bool, str]:
    result = get_lead_by_id(lead_id)
    if not result:
        return False, "Lead topilmadi"

    row_idx, lead = result
    status = clean_text(lead.get("status")).lower()

    if status in ("taken", "completed"):
        assigned = clean_text(lead.get("assigned_agent_name"))
        return False, f"Bu lead allaqachon biriktirilgan: {assigned or 'boshqa agent'}"

    update_cell_by_header(leads_ws, row_idx, "status", "taken")
    update_cell_by_header(leads_ws, row_idx, "assigned_agent_id", clean_text(agent_row.get("agent_id")))
    update_cell_by_header(leads_ws, row_idx, "assigned_agent_name", clean_text(agent_row.get("full_name")))
    update_cell_by_header(leads_ws, row_idx, "assigned_at", now_str())
    return True, "Lead sizga biriktirildi"

def reject_lead(lead_id: str, agent_row: Dict[str, Any]) -> Tuple[bool, str]:
    result = get_lead_by_id(lead_id)
    if not result:
        return False, "Lead topilmadi"

    row_idx, lead = result
    status = clean_text(lead.get("status")).lower()

    if status == "completed":
        return False, "Lead allaqachon bajarilgan"

    assigned_agent_id = clean_text(lead.get("assigned_agent_id"))
    current_agent_id = clean_text(agent_row.get("agent_id"))

    if assigned_agent_id and assigned_agent_id != current_agent_id:
        return False, "Bu lead sizniki emas"

    update_cell_by_header(leads_ws, row_idx, "status", "rejected")
    update_cell_by_header(leads_ws, row_idx, "rejected_by", clean_text(agent_row.get("full_name")))
    return True, "Lead rad etildi"

def create_bonus_from_lead(lead_id: str, lead: Dict[str, Any]) -> Tuple[bool, str]:
    special_agent_id = clean_text(lead.get("special_agent_id"))
    if not special_agent_id:
        return False, "Maxsus agent yo'q"

    for row in get_records(bonuses_ws):
        if clean_text(row.get("lead_id")) == lead_id:
            return False, "Bonus oldin yaratilgan"

    special_agent_name = clean_text(lead.get("special_agent_name"))
    special_agent_tg_id = ""
    for sp in get_records(special_agents_ws):
        if clean_text(sp.get("special_agent_id")) == special_agent_id:
            special_agent_tg_id = clean_text(sp.get("telegram_id"))
            break

    amount = get_setting("special_bonus_amount", "100000")
    currency = get_setting("bonus_currency", "UZS")

    bonuses_ws.append_row([
        generate_id("bonus_"),
        now_str(),
        lead_id,
        special_agent_id,
        special_agent_name,
        special_agent_tg_id,
        clean_text(lead.get("client_name")),
        clean_text(lead.get("service")),
        amount,
        currency,
        "pending",
        "",
        "",
        "",
    ])
    return True, "Bonus yaratildi"

def complete_lead(lead_id: str, agent_row: Dict[str, Any]) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    result = get_lead_by_id(lead_id)
    if not result:
        return False, "Lead topilmadi", None

    row_idx, lead = result
    status = clean_text(lead.get("status")).lower()

    if status == "completed":
        return False, "Lead allaqachon bajarilgan", lead

    assigned_agent_id = clean_text(lead.get("assigned_agent_id"))
    current_agent_id = clean_text(agent_row.get("agent_id"))

    if assigned_agent_id != current_agent_id:
        return False, "Bu lead sizniki emas", lead

    update_cell_by_header(leads_ws, row_idx, "status", "completed")
    update_cell_by_header(leads_ws, row_idx, "completed_at", now_str())
    create_bonus_from_lead(lead_id, lead)
    return True, "Lead bajarildi", lead

def complete_lead_by_admin(lead_id: str) -> Tuple[bool, str]:
    result = get_lead_by_id(lead_id)
    if not result:
        return False, "Lead topilmadi"

    row_idx, lead = result
    status = clean_text(lead.get("status")).lower()
    if status == "completed":
        return False, "Lead oldin bajarilgan"

    update_cell_by_header(leads_ws, row_idx, "status", "completed")
    update_cell_by_header(leads_ws, row_idx, "completed_at", now_str())
    create_bonus_from_lead(lead_id, lead)
    return True, "Lead completed"

def build_lead_message(lead: Dict[str, Any], lead_id: str) -> str:
    txt = [
        "🆕 <b>Янги лид</b>",
        f"🆔 ID: <code>{escape(lead_id)}</code>",
        f"👤 Мижоз: {escape(lead.get('client_name'))}",
        f"📞 Телефон: {escape(lead.get('client_phone'))}",
        f"🏷 Хизмат: {escape(lead.get('service'))}",
    ]

    district = clean_text(lead.get("district"))
    region = clean_text(lead.get("region"))
    if region or district:
        txt.append(f"📍 Манзил: {escape(region)} {escape(district)}".strip())

    note = clean_text(lead.get("note"))
    if note:
        txt.append(f"📝 Изоҳ: {escape(note)}")

    sp_name = clean_text(lead.get("special_agent_name"))
    if sp_name:
        txt.append(f"🤝 Махсус агент: {escape(sp_name)}")

    txt.append(f"🕒 Вақт: {escape(lead.get('created_at'))}")
    return "\n".join(txt)

# =========================================================
# BONUSES
# =========================================================
def get_bonus_rows_for_special_agent(special_agent_id: str) -> List[Dict[str, Any]]:
    rows = []
    for row in get_records(bonuses_ws):
        if clean_text(row.get("special_agent_id")) == special_agent_id:
            rows.append(row)
    return rows

def get_bonus_stats_for_special_agent(special_agent_id: str) -> Dict[str, Any]:
    rows = get_bonus_rows_for_special_agent(special_agent_id)
    total = 0
    pending = 0
    paid = 0

    for row in rows:
        amount = int(clean_text(row.get("amount")) or "0")
        total += amount
        if clean_text(row.get("status")).lower() == "paid":
            paid += amount
        else:
            pending += amount

    return {
        "count": len(rows),
        "total": total,
        "pending": pending,
        "paid": paid,
        "currency": get_setting("bonus_currency", "UZS")
    }

def mark_bonus_paid(lead_id: str, paid_by: str) -> Tuple[bool, str]:
    for idx, row in enumerate(get_records(bonuses_ws), start=2):
        if clean_text(row.get("lead_id")) == clean_text(lead_id):
            if clean_text(row.get("status")).lower() == "paid":
                return False, "Bonus oldin to'langan"

            update_cell_by_header(bonuses_ws, idx, "status", "paid")
            update_cell_by_header(bonuses_ws, idx, "paid_at", now_str())
            update_cell_by_header(bonuses_ws, idx, "paid_by", paid_by)
            return True, "Bonus paid"
    return False, "Bonus topilmadi"

def get_all_bonus_stats() -> Dict[str, Any]:
    rows = get_records(bonuses_ws)
    total_count = len(rows)
    total_amount = 0
    paid_amount = 0
    pending_amount = 0

    for row in rows:
        amount = int(clean_text(row.get("amount")) or "0")
        total_amount += amount
        if clean_text(row.get("status")).lower() == "paid":
            paid_amount += amount
        else:
            pending_amount += amount

    return {
        "count": total_count,
        "total_amount": total_amount,
        "paid_amount": paid_amount,
        "pending_amount": pending_amount,
        "currency": get_setting("bonus_currency", "UZS")
    }

# =========================================================
# NOTIFY
# =========================================================
async def notify_admins(text: str):
    for admin_id in get_admin_ids():
        try:
            await bot.send_message(admin_id, text)
        except Exception as e:
            logger.warning("adminga xabar yuborilmadi %s: %s", admin_id, e)

async def notify_agents_about_new_lead(lead_id: str):
    result = get_lead_by_id(lead_id)
    if not result:
        return

    _, lead = result
    agents = get_active_agents()
    if not agents:
        await notify_admins(f"⚠️ Aktiv agent topilmadi. Lead: <code>{escape(lead_id)}</code>")
        return

    text = build_lead_message(lead, lead_id)
    kb = new_lead_keyboard(lead_id)

    for agent in agents:
        tg_id = clean_text(agent.get("telegram_id"))
        if not tg_id.isdigit():
            continue
        try:
            await bot.send_message(int(tg_id), text, reply_markup=kb)
        except Exception as e:
            logger.warning("agentga yuborishda xato %s: %s", tg_id, e)

async def notify_client_lead_taken(lead: Dict[str, Any], agent_name: str):
    client_tg = clean_text(lead.get("client_telegram_id"))
    if client_tg.isdigit():
        try:
            await bot.send_message(
                int(client_tg),
                f"✅ Сизнинг заявкангиз агентга бириктирилди.\n👨‍💼 Агент: <b>{escape(agent_name)}</b>"
            )
        except Exception:
            pass

async def notify_client_lead_completed(lead: Dict[str, Any], lead_id: str):
    client_tg = clean_text(lead.get("client_telegram_id"))
    if client_tg.isdigit():
        try:
            await bot.send_message(
                int(client_tg),
                f"✅ Сизнинг мурожаатингиз бўйича иш якунланди.\n🆔 ID: <code>{escape(lead_id)}</code>\nТашаккур."
            )
        except Exception:
            pass

async def notify_special_agent_bonus(lead: Dict[str, Any], lead_id: str):
    special_agent_id = clean_text(lead.get("special_agent_id"))
    if not special_agent_id:
        return

    for sp in get_records(special_agents_ws):
        if clean_text(sp.get("special_agent_id")) == special_agent_id:
            tg_id = clean_text(sp.get("telegram_id"))
            if tg_id.isdigit():
                bonus_amount = get_setting("special_bonus_amount", "100000")
                currency = get_setting("bonus_currency", "UZS")
                bonus_text = get_setting(
                    "special_bonus_text",
                    "Siz yuborgan mijozning ishi yakunlandi. Bonusni ofisdan olib ketishingiz mumkin."
                )
                try:
                    await bot.send_message(
                        int(tg_id),
                        "🎉 " + escape(bonus_text) + "\n\n"
                        f"👤 Мижоз: {escape(lead.get('client_name'))}\n"
                        f"🏷 Хизмат: {escape(lead.get('service'))}\n"
                        f"💰 Бонус: <b>{escape(bonus_amount)} {escape(currency)}</b>\n"
                        f"🆔 Lead: <code>{escape(lead_id)}</code>"
                    )
                except Exception as e:
                    logger.warning("special agent notify error: %s", e)
            break

# =========================================================
# REFERRAL MEMORY
# =========================================================
pending_ref_by_user: Dict[int, str] = {}

# =========================================================
# COMMANDS
# =========================================================
@dp.message(CommandStart())
async def start_handler(message: Message, command: CommandStart):
    args = command.args
    ref_code = ""
    if args:
        ref_code = clean_text(args)
        if ref_code:
            pending_ref_by_user[message.from_user.id] = ref_code

    txt = "👋 Хуш келибсиз.\nКеракли бўлимни менюдан танланг."
    if ref_code:
        sp = get_special_agent_by_ref_code(ref_code)
        if sp:
            txt += f"\n\n🤝 Сиз махсус агент орқали кирдингиз: <b>{escape(sp.get('full_name'))}</b>"

    await message.answer(txt, reply_markup=main_menu())

@dp.message(Command("id"))
async def id_handler(message: Message):
    username_text = f"\n👤 Username: @{escape(message.from_user.username)}" if message.from_user.username else ""
    await message.answer(f"🆔 Сизнинг Telegram ID: <code>{message.from_user.id}</code>{username_text}")

@dp.message(Command("register_agent"))
async def register_agent_handler(message: Message):
    parts = clean_text(message.text).split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Формат:\n<code>/register_agent AGENT_KOD</code>\n\nМисол:\n<code>/register_agent GK2026</code>")
        return

    try:
        agent_code = clean_text(parts[1]).upper()
        register_or_update_agent(
            agent_code=agent_code,
            tg_id=message.from_user.id,
            full_name=message.from_user.full_name,
            username=message.from_user.username or ""
        )
        append_log("INFO", "register_agent", f"{message.from_user.id} -> {agent_code}")
        await message.answer(
            f"✅ Сиз агент сифатида рўйхатдан ўтдингиз.\nAgent ID: <code>{escape(agent_code)}</code>",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.exception("register_agent error")
        await message.answer(f"❌ Агент қўшилмади: {escape(str(e))}")

@dp.message(Command("admin"))
async def admin_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Сизда admin ҳуқуқи йўқ.")
        return

    leads = get_records(leads_ws)
    agents = get_records(agents_ws)
    special_agents = get_records(special_agents_ws)
    bonus_stats = get_all_bonus_stats()

    total = len(leads)
    new_count = sum(1 for x in leads if clean_text(x.get("status")).lower() == "new")
    taken_count = sum(1 for x in leads if clean_text(x.get("status")).lower() == "taken")
    rejected_count = sum(1 for x in leads if clean_text(x.get("status")).lower() == "rejected")
    completed_count = sum(1 for x in leads if clean_text(x.get("status")).lower() == "completed")
    active_agents = sum(1 for x in agents if bool_from_sheet(x.get("is_active")))
    active_special = sum(1 for x in special_agents if bool_from_sheet(x.get("is_active")))

    txt = (
        "📊 <b>CRM PRO статистика</b>\n\n"
        f"Leads jami: <b>{total}</b>\n"
        f"🆕 Yangi: <b>{new_count}</b>\n"
        f"✅ Olingan: <b>{taken_count}</b>\n"
        f"❌ Rad etilgan: <b>{rejected_count}</b>\n"
        f"🏁 Bajarilgan: <b>{completed_count}</b>\n\n"
        f"👨‍💼 Aktiv agentlar: <b>{active_agents}</b>\n"
        f"🤝 Maxsus agentlar: <b>{active_special}</b>\n\n"
        f"💰 Bonuslar soni: <b>{bonus_stats['count']}</b>\n"
        f"💵 Jami bonus: <b>{bonus_stats['total_amount']} {bonus_stats['currency']}</b>\n"
        f"⏳ Kutilayotgan: <b>{bonus_stats['pending_amount']} {bonus_stats['currency']}</b>\n"
        f"✅ To'langan: <b>{bonus_stats['paid_amount']} {bonus_stats['currency']}</b>"
    )
    await message.answer(txt)

@dp.message(Command("top_agents"))
async def top_agents_handler(message: Message):
    stats = get_agent_stats()
    if not stats:
        await message.answer("Ҳозирча статистика йўқ.")
        return

    top = sorted(stats.items(), key=lambda x: (x[1]["completed"], x[1]["taken"]), reverse=True)[:10]
    lines = ["🏆 <b>ТОП агентлар</b>\n"]
    for i, (_, st) in enumerate(top, start=1):
        lines.append(
            f"{i}. <b>{escape(st['name'])}</b>\n"
            f"   ✅ Ёпган: {st['completed']} | 📥 Олган: {st['taken']} | ❌ Рад: {st['rejected']}"
        )
    await message.answer("\n".join(lines))

@dp.message(Command("my_stats"))
async def my_stats_handler(message: Message):
    agent = get_agent_by_telegram_id(message.from_user.id)
    if not agent:
        await message.answer("Сиз агент сифатида рўйхатдан ўтмагансиз.")
        return

    stats = get_agent_stats()
    agent_id = clean_text(agent.get("agent_id"))
    st = stats.get(agent_id, {"taken": 0, "completed": 0, "rejected": 0, "name": clean_text(agent.get("full_name"))})

    await message.answer(
        f"📊 <b>Сизнинг статистика</b>\n\n"
        f"👨‍💼 Агент: <b>{escape(st['name'])}</b>\n"
        f"📥 Олган lead: <b>{st['taken']}</b>\n"
        f"✅ Ёпилган lead: <b>{st['completed']}</b>\n"
        f"❌ Рад этилган: <b>{st['rejected']}</b>"
    )

@dp.message(Command("bonus_stats"))
async def bonus_stats_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Сизда admin ҳуқуқи йўқ.")
        return

    st = get_all_bonus_stats()
    await message.answer(
        f"💰 <b>Бонус статистика</b>\n\n"
        f"Jami bonuslar: <b>{st['count']}</b>\n"
        f"Umumiy summa: <b>{st['total_amount']} {st['currency']}</b>\n"
        f"To'langan: <b>{st['paid_amount']} {st['currency']}</b>\n"
        f"Kutilayotgan: <b>{st['pending_amount']} {st['currency']}</b>"
    )

@dp.message(Command("my_bonus"))
async def my_bonus_handler(message: Message):
    sp = get_special_agent_by_tg_id(message.from_user.id)
    if not sp:
        await message.answer("Сиз махсус агент сифатида рўйхатдан ўтмагансиз.")
        return

    st = get_bonus_stats_for_special_agent(clean_text(sp.get("special_agent_id")))
    await message.answer(
        f"💰 <b>Сизнинг бонуслар</b>\n\n"
        f"🤝 Агент: <b>{escape(sp.get('full_name'))}</b>\n"
        f"Bonuslar soni: <b>{st['count']}</b>\n"
        f"Jami: <b>{st['total']} {st['currency']}</b>\n"
        f"⏳ Kutilayotgan: <b>{st['pending']} {st['currency']}</b>\n"
        f"✅ To'langan: <b>{st['paid']} {st['currency']}</b>"
    )

@dp.message(Command("complete_lead"))
async def complete_lead_admin_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Сизда admin ҳуқуқи йўқ.")
        return

    parts = clean_text(message.text).split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Формат: <code>/complete_lead LEAD_ID</code>")
        return

    lead_id = clean_text(parts[1])
    ok, msg = complete_lead_by_admin(lead_id)
    if not ok:
        await message.answer(f"❌ {escape(msg)}")
        return

    result = get_lead_by_id(lead_id)
    if result:
        _, lead = result
        await notify_client_lead_completed(lead, lead_id)
        await notify_special_agent_bonus(lead, lead_id)

    await message.answer(f"✅ Lead yakunlandi: <code>{escape(lead_id)}</code>")

@dp.message(Command("pay_bonus"))
async def pay_bonus_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Сизда admin ҳуқуқи йўқ.")
        return

    parts = clean_text(message.text).split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Формат: <code>/pay_bonus LEAD_ID</code>")
        return

    lead_id = clean_text(parts[1])
    ok, msg = mark_bonus_paid(lead_id, message.from_user.full_name)
    if not ok:
        await message.answer(f"❌ {escape(msg)}")
        return

    for row in get_records(bonuses_ws):
        if clean_text(row.get("lead_id")) == lead_id:
            tg_id = clean_text(row.get("special_agent_tg_id"))
            if tg_id.isdigit():
                try:
                    await bot.send_message(
                        int(tg_id),
                        f"✅ Сизнинг бонусингиз тўланди.\n🆔 Lead: <code>{escape(lead_id)}</code>\n💰 {escape(row.get('amount'))} {escape(row.get('currency'))}"
                    )
                except Exception:
                    pass
            break

    await message.answer(f"✅ Bonus paid: <code>{escape(lead_id)}</code>")

# =========================================================
# MENU ACTIONS
# =========================================================
@dp.message(F.text == "☎️ Алоқа")
async def contact_handler(message: Message):
    phone = get_setting("contact_phone", CONTACT_PHONE)
    await message.answer(f"☎️ Алоқа учун: <b>{escape(phone)}</b>")

@dp.message(F.text == "📝 Заявка қолдириш")
async def lead_start_handler(message: Message, state: FSMContext):
    await state.clear()
    ref_code = pending_ref_by_user.get(message.from_user.id, "")

    if ref_code:
        sp = get_special_agent_by_ref_code(ref_code)
        if sp:
            await state.update_data(
                special_agent_id=clean_text(sp.get("special_agent_id")),
                special_agent_name=clean_text(sp.get("full_name")),
                ref_code=clean_text(sp.get("ref_code")),
                source="special_link"
            )

    await state.set_state(LeadForm.waiting_name)
    await message.answer("Исмингизни киритинг:", reply_markup=back_menu())

@dp.message(F.text == "🤝 Махсус агент бўлиш")
async def special_agent_start_handler(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(SpecialAgentForm.waiting_name)
    await message.answer("Ф.И.Ш киритинг:", reply_markup=back_menu())

@dp.message(F.text == "⬅️ Орқага")
async def back_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Асосий меню.", reply_markup=main_menu())

# =========================================================
# SPECIAL AGENT FSM
# =========================================================
@dp.message(SpecialAgentForm.waiting_name)
async def special_agent_name_handler(message: Message, state: FSMContext):
    name = clean_text(message.text)
    if len(name) < 3:
        await message.answer("Исмни тўғри киритинг.")
        return

    await state.update_data(full_name=name)
    await state.set_state(SpecialAgentForm.waiting_phone)
    await message.answer("Телефон рақам киритинг:", reply_markup=back_menu())

@dp.message(SpecialAgentForm.waiting_phone)
async def special_agent_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)
    data = await state.get_data()

    try:
        sp = register_special_agent(
            full_name=data["full_name"],
            phone=phone,
            tg_id=message.from_user.id,
            username=message.from_user.username or ""
        )

        me = await bot.get_me()
        link = f"https://t.me/{me.username}?start={sp['ref_code']}"

        await message.answer(
            "✅ Сиз махсус агент сифатида рўйхатдан ўтдингиз.\n\n"
            f"🆔 ID: <code>{escape(sp['special_agent_id'])}</code>\n"
            f"🔗 Шахсий линк:\n{escape(link)}\n\n"
            "Мижозни шу линк орқали юборинг.",
            reply_markup=main_menu()
        )
        await notify_admins(
            "🤝 <b>Янги махсус агент</b>\n"
            f"👤 {escape(sp['full_name'])}\n"
            f"📞 {escape(sp['phone'])}\n"
            f"🆔 <code>{escape(sp['special_agent_id'])}</code>"
        )
        await state.clear()
    except Exception as e:
        logger.exception("special_agent register error")
        await message.answer(f"❌ Хато: {escape(str(e))}")

# =========================================================
# LEAD FSM
# =========================================================
@dp.message(LeadForm.waiting_name)
async def lead_name_handler(message: Message, state: FSMContext):
    name = clean_text(message.text)
    if len(name) < 2:
        await message.answer("Исмингизни тўғри киритинг.")
        return

    await state.update_data(client_name=name)
    await state.set_state(LeadForm.waiting_phone)
    await message.answer("Телефон рақамингизни юбoринг:", reply_markup=back_menu())

@dp.message(LeadForm.waiting_phone)
async def lead_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)
    if len(re.sub(r"\D+", "", phone)) < 9:
        await message.answer("Телефон рақамни тўғри киритинг.")
        return

    await state.update_data(client_phone=phone)
    await state.set_state(LeadForm.waiting_service)
    await message.answer("Керакли хизмат турини танланг:", reply_markup=services_menu())

@dp.message(LeadForm.waiting_service)
async def lead_service_handler(message: Message, state: FSMContext):
    service = clean_text(message.text)
    allowed = {
        "🏠 Уй сотиш", "🏠 Уй сотиб олиш",
        "🏢 Ижарага бериш", "🏢 Ижарага олиш",
        "🏦 Ипотека", "📄 Кадастр"
    }
    if service not in allowed:
        await message.answer("Тугмалардан бирини танланг.", reply_markup=services_menu())
        return

    await state.update_data(service=service)
    await state.set_state(LeadForm.waiting_region)
    await message.answer("Ҳудудни киритинг:", reply_markup=back_menu())

@dp.message(LeadForm.waiting_region)
async def lead_region_handler(message: Message, state: FSMContext):
    region = clean_text(message.text)
    if len(region) < 2:
        await message.answer("Ҳудудни тўғри киритинг.")
        return

    await state.update_data(region=region)
    await state.set_state(LeadForm.waiting_district)
    await message.answer("Туман ёки шаҳарни киритинг:", reply_markup=back_menu())

@dp.message(LeadForm.waiting_district)
async def lead_district_handler(message: Message, state: FSMContext):
    district = clean_text(message.text)
    if len(district) < 2:
        await message.answer("Туман/шаҳарни тўғри киритинг.")
        return

    await state.update_data(district=district)
    await state.set_state(LeadForm.waiting_note)
    await message.answer("Қўшимча изоҳ ёзинг. Агар изоҳ бўлмаса '-' юборинг:", reply_markup=back_menu())

@dp.message(LeadForm.waiting_note)
async def lead_note_handler(message: Message, state: FSMContext):
    note = clean_text(message.text)
    if note == "-":
        note = ""

    data = await state.get_data()
    data.update({
        "note": note,
        "client_telegram_id": message.from_user.id,
        "client_username": message.from_user.username or "",
        "source": data.get("source", "bot"),
    })

    try:
        lead_id = create_lead(data)
        await notify_agents_about_new_lead(lead_id)

        await message.answer(
            "✅ Заявкангиз қабул қилинди.\n"
            f"🆔 ID: <code>{escape(lead_id)}</code>\n"
            "Тез орада сиз билан боғланишади.",
            reply_markup=main_menu()
        )

        await notify_admins(
            "🆕 <b>Янги lead яратилди</b>\n"
            f"🆔 <code>{escape(lead_id)}</code>\n"
            f"👤 {escape(data.get('client_name'))}\n"
            f"📞 {escape(data.get('client_phone'))}\n"
            f"🏷 {escape(data.get('service'))}"
        )
        await state.clear()
    except Exception as e:
        logger.exception("create lead error")
        await message.answer(f"❌ Lead сақланмади: {escape(str(e))}")

# =========================================================
# CALLBACKS
# =========================================================
@dp.callback_query(F.data.startswith("take:"))
async def take_lead_callback(callback: CallbackQuery):
    lead_id = callback.data.split(":", 1)[1]
    agent = get_agent_by_telegram_id(callback.from_user.id)

    if not agent:
        await callback.answer("Сиз агент эмассиз", show_alert=True)
        return

    try:
        ok, msg = assign_lead(lead_id, agent)
        if not ok:
            await callback.answer(msg, show_alert=True)
            return

        await callback.message.edit_reply_markup(reply_markup=taken_lead_keyboard(lead_id))
        await callback.answer("Lead olindi")

        result = get_lead_by_id(lead_id)
        if result:
            _, lead = result
            await notify_client_lead_taken(lead, clean_text(agent.get("full_name")))

        await notify_admins(
            f"✅ <b>Lead бириктирилди</b>\n"
            f"🆔 <code>{escape(lead_id)}</code>\n"
            f"👨‍💼 Агент: {escape(agent.get('full_name'))}"
        )
    except Exception as e:
        logger.exception("take lead error")
        await callback.answer(f"Хато: {escape(str(e))}", show_alert=True)

@dp.callback_query(F.data.startswith("reject:"))
async def reject_lead_callback(callback: CallbackQuery):
    lead_id = callback.data.split(":", 1)[1]
    agent = get_agent_by_telegram_id(callback.from_user.id)

    if not agent:
        await callback.answer("Сиз агент эмассиз", show_alert=True)
        return

    try:
        ok, msg = reject_lead(lead_id, agent)
        if not ok:
            await callback.answer(msg, show_alert=True)
            return

        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer("Lead rad etildi")

        await notify_admins(
            f"❌ <b>Lead рад этилди</b>\n"
            f"🆔 <code>{escape(lead_id)}</code>\n"
            f"👨‍💼 Агент: {escape(agent.get('full_name'))}"
        )
    except Exception as e:
        logger.exception("reject lead error")
        await callback.answer(f"Хато: {escape(str(e))}", show_alert=True)

@dp.callback_query(F.data.startswith("done:"))
async def done_lead_callback(callback: CallbackQuery):
    lead_id = callback.data.split(":", 1)[1]
    agent = get_agent_by_telegram_id(callback.from_user.id)

    if not agent:
        await callback.answer("Сиз агент эмассиз", show_alert=True)
        return

    try:
        ok, msg, lead = complete_lead(lead_id, agent)
        if not ok:
            await callback.answer(msg, show_alert=True)
            return

        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer("Lead bajarildi")

        if lead:
            await notify_client_lead_completed(lead, lead_id)
            await notify_special_agent_bonus(lead, lead_id)

        await notify_admins(
            f"🏁 <b>Lead бажарилди</b>\n"
            f"🆔 <code>{escape(lead_id)}</code>\n"
            f"👨‍💼 Агент: {escape(agent.get('full_name'))}"
        )
    except Exception as e:
        logger.exception("done lead error")
        await callback.answer(f"Хато: {escape(str(e))}", show_alert=True)

# =========================================================
# FALLBACK
# =========================================================
@dp.message()
async def fallback_handler(message: Message):
    await message.answer("Керакли бўлимни менюдан танланг.", reply_markup=main_menu())

# =========================================================
# STARTUP
# =========================================================
async def on_startup():
    logger.info("Bot ishga tushmoqda...")
    try:
        me = await bot.get_me()
        logger.info("Bot: @%s", me.username)
        _ = agents_ws.get_all_records()
        _ = leads_ws.get_all_records()
        _ = bonuses_ws.get_all_records()
        append_log("INFO", "startup", "CRM PRO bot started")
    except Exception as e:
        logger.exception("startup error: %s", e)
        raise

# =========================================================
# MAIN
# =========================================================
async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())