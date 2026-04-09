from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import gspread
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("golden_key_bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
APP_BASE_URL = os.getenv("APP_BASE_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "").strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
ENVIRONMENT = os.getenv("ENVIRONMENT", "production").strip()
PORT = int(os.getenv("PORT", "8000"))
AGENT_GROUP_ID = int(os.getenv("AGENT_GROUP_ID", "0") or 0)
GROUP_ID = int(os.getenv("GROUP_ID", "0") or 0)
BONUS_AMOUNT = int(os.getenv("BONUS_AMOUNT", "100000") or 100000)

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = []
if ADMIN_IDS_RAW:
    for x in ADMIN_IDS_RAW.split(","):
        x = x.strip()
        if x:
            ADMIN_IDS.append(int(x))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not APP_BASE_URL:
    raise RuntimeError("APP_BASE_URL is required")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required")
if not SPREADSHEET_URL:
    raise RuntimeError("SPREADSHEET_URL is required")
if not GOOGLE_CREDS_JSON:
    raise RuntimeError("GOOGLE_CREDS_JSON is required")

WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
WEBHOOK_URL = f"{APP_BASE_URL}{WEBHOOK_PATH}"

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())
router = Router()


class LeadForm(StatesGroup):
    waiting_phone = State()
    waiting_service = State()
    waiting_district = State()
    waiting_budget = State()
    waiting_note = State()
    waiting_referral = State()


SERVICES = [
    "🏠 Уй сотиб олиш",
    "🏘 Уй сотиш",
    "🔑 Ижарага олиш",
    "📤 Ижарага бериш",
    "🏦 Ипотека",
    "📑 Кадастр хизмати",
]

DISTRICTS = [
    "Кўқон шаҳар",
    "Фарғона шаҳар",
    "Қувасой",
    "Марғилон",
    "Риштон",
    "Бешариқ",
    "Боғдод",
    "Учкўприк",
    "Қува",
    "Ёзёвон",
    "Тошлоқ",
    "Олтиариқ",
    "Данғара",
    "Бошқа",
]

LEADS_HEADERS = [
    "lead_id",
    "created_at",
    "client_tg_id",
    "client_username",
    "client_full_name",
    "phone",
    "service",
    "district",
    "budget",
    "note",
    "referral_code",
    "status",
    "agent_tg_id",
    "agent_name",
    "taken_at",
    "done_at",
    "client_message_id",
    "agent_group_message_id",
]

BONUSES_HEADERS = [
    "created_at",
    "lead_id",
    "referral_code",
    "bonus_amount",
    "status",
]

AGENT_ACTIONS = {"take", "reject", "done"}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def esc(s: Any) -> str:
    if s is None:
        return ""
    return str(s)


def normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("998") and len(digits) == 12:
        return f"+{digits}"
    if digits.startswith("9") and len(digits) == 9:
        return f"+998{digits}"
    if digits.startswith("0") and len(digits) == 10:
        return f"+998{digits[1:]}"
    return phone.strip()


def phone_is_valid(phone: str) -> bool:
    normalized = normalize_phone(phone)
    return bool(re.fullmatch(r"\+998\d{9}", normalized))


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def build_main_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.button(text="📝 Заявка қолдириш")
    kb.button(text="📞 Алоқа")
    kb.button(text="ℹ️ Хизматлар")
    kb.adjust(2, 1)
    return kb.as_markup(resize_keyboard=True)


def build_phone_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(
        KeyboardButton(
            text="📱 Телефон рақамни юбориш",
            request_contact=True,
        )
    )
    return kb.as_markup(resize_keyboard=True, one_time_keyboard=True)


def build_services_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    for item in SERVICES:
        kb.button(text=item)
    kb.adjust(2, 2, 2)
    return kb.as_markup(resize_keyboard=True)


def build_districts_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    for item in DISTRICTS:
        kb.button(text=item)
    kb.adjust(2, 2, 2, 2, 2, 2, 1)
    return kb.as_markup(resize_keyboard=True)


def build_agent_inline(lead_id: str, taken: bool = False, finished: bool = False):
    kb = InlineKeyboardBuilder()
    if finished:
        kb.button(text="✅ Якунланган", callback_data=f"lead:none:{lead_id}")
    elif taken:
        kb.button(text="✅ Олдим", callback_data=f"lead:none:{lead_id}")
        kb.button(text="🏁 Бажарилди", callback_data=f"lead:done:{lead_id}")
    else:
        kb.button(text="✅ Олдим", callback_data=f"lead:take:{lead_id}")
        kb.button(text="❌ Рад этилди", callback_data=f"lead:reject:{lead_id}")
    kb.adjust(2)
    return kb.as_markup()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


class SheetsDB:
    def __init__(self, spreadsheet_url: str, creds_json: str):
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        self.spreadsheet = client.open_by_url(spreadsheet_url)

        self.leads_ws = self._get_or_create_sheet("Leads", LEADS_HEADERS)
        self.bonus_ws = self._get_or_create_sheet("Bonuses", BONUSES_HEADERS)

    def _get_or_create_sheet(self, title: str, headers: List[str]):
        try:
            ws = self.spreadsheet.worksheet(title)
        except Exception:
            ws = self.spreadsheet.add_worksheet(title=title, rows=1000, cols=max(20, len(headers) + 5))
            ws.append_row(headers)
            return ws

        first_row = ws.row_values(1)
        if first_row != headers:
            if not first_row:
                ws.append_row(headers)
            else:
                ws.resize(rows=max(ws.row_count, 2), cols=max(ws.col_count, len(headers)))
                ws.update("A1", [headers])
        return ws

    def healthcheck(self) -> str:
        return self.spreadsheet.title

    def append_lead(self, row: List[Any]) -> None:
        self.leads_ws.append_row(row, value_input_option="USER_ENTERED")

    def get_all_leads(self) -> List[Dict[str, Any]]:
        return self.leads_ws.get_all_records()

    def find_lead_row(self, lead_id: str) -> Optional[int]:
        values = self.leads_ws.col_values(1)
        for idx, val in enumerate(values, start=1):
            if idx == 1:
                continue
            if str(val).strip() == lead_id:
                return idx
        return None

    def get_lead(self, lead_id: str) -> Optional[Dict[str, Any]]:
        rows = self.get_all_leads()
        for row in rows:
            if str(row.get("lead_id", "")).strip() == lead_id:
                return row
        return None

    def update_lead_fields(self, lead_id: str, updates: Dict[str, Any]) -> bool:
        row_num = self.find_lead_row(lead_id)
        if not row_num:
            return False

        headers = self.leads_ws.row_values(1)
        for key, value in updates.items():
            if key not in headers:
                continue
            col_num = headers.index(key) + 1
            self.leads_ws.update_cell(row_num, col_num, value)
        return True

    def append_bonus(self, row: List[Any]) -> None:
        self.bonus_ws.append_row(row, value_input_option="USER_ENTERED")

    def get_daily_stats(self) -> Dict[str, int]:
        leads = self.get_all_leads()
        today = datetime.now().strftime("%Y-%m-%d")
        result = {"new": 0, "taken": 0, "done": 0, "rejected": 0}
        for lead in leads:
            created_at = esc(lead.get("created_at"))
            status = esc(lead.get("status")).lower()
            if created_at.startswith(today):
                result["new"] += 1
                if status == "taken":
                    result["taken"] += 1
                elif status == "done":
                    result["done"] += 1
                elif status == "rejected":
                    result["rejected"] += 1
        return result

    def get_monthly_stats(self) -> Dict[str, int]:
        leads = self.get_all_leads()
        month = datetime.now().strftime("%Y-%m")
        result = {"new": 0, "taken": 0, "done": 0, "rejected": 0}
        for lead in leads:
            created_at = esc(lead.get("created_at"))
            status = esc(lead.get("status")).lower()
            if created_at.startswith(month):
                result["new"] += 1
                if status == "taken":
                    result["taken"] += 1
                elif status == "done":
                    result["done"] += 1
                elif status == "rejected":
                    result["rejected"] += 1
        return result

    def get_top_agents(self, limit: int = 5) -> List[tuple[str, int]]:
        leads = self.get_all_leads()
        stats: Dict[str, int] = {}
        for lead in leads:
            if esc(lead.get("status")).lower() == "done":
                agent_name = esc(lead.get("agent_name")).strip() or f"Agent {esc(lead.get('agent_tg_id'))}"
                stats[agent_name] = stats.get(agent_name, 0) + 1
        return sorted(stats.items(), key=lambda x: x[1], reverse=True)[:limit]


db = SheetsDB(SPREADSHEET_URL, GOOGLE_CREDS_JSON)


def build_lead_text(lead: Dict[str, Any]) -> str:
    return (
        "🆕 <b>Янги лид</b>\n\n"
        f"🆔 ID: <code>{esc(lead['lead_id'])}</code>\n"
        f"👤 Мижоз: {esc(lead['client_full_name'])}\n"
        f"📞 Телефон: {esc(lead['phone'])}\n"
        f"🛎 Хизмат: {esc(lead['service'])}\n"
        f"📍 Ҳудуд: {esc(lead['district'])}\n"
        f"💰 Бюджет: {esc(lead['budget'])}\n"
        f"📝 Изоҳ: {esc(lead['note'])}\n"
        f"🤝 Referral: {esc(lead['referral_code']) or '-'}\n"
        f"📊 Статус: {esc(lead['status'])}"
    )


def build_client_success_text(lead_id: str) -> str:
    return (
        "✅ Заявкангиз қабул қилинди.\n\n"
        f"🆔 Сизнинг ID: <code>{lead_id}</code>\n"
        "Тез орада мутахассисларимиз сиз билан боғланади."
    )


def build_services_text() -> str:
    return (
        "📋 <b>Хизматларимиз</b>\n\n"
        "• Уй сотиб олиш\n"
        "• Уй сотиш\n"
        "• Ижарага олиш\n"
        "• Ижарага бериш\n"
        "• Ипотека ёрдами\n"
        "• Кадастр хизматлари"
    )


async def start_lead_form(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(LeadForm.waiting_phone)
    await message.answer(
        "Илтимос, телефон рақамингизни юборинг.",
        reply_markup=build_phone_kb(),
    )


async def notify_agents_about_lead(lead: Dict[str, Any]) -> Optional[int]:
    if not AGENT_GROUP_ID:
        logger.warning("AGENT_GROUP_ID not configured")
        return None

    sent = await bot.send_message(
        AGENT_GROUP_ID,
        build_lead_text(lead),
        reply_markup=build_agent_inline(lead["lead_id"]),
    )
    return sent.message_id


async def notify_admins(text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as e:
            logger.warning("Failed to notify admin %s: %s", admin_id, e)


async def finalize_lead(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    user = message.from_user

    lead_id = uuid.uuid4().hex[:10].upper()
    created_at = now_str()

    lead = {
        "lead_id": lead_id,
        "created_at": created_at,
        "client_tg_id": user.id,
        "client_username": user.username or "",
        "client_full_name": data.get("client_full_name") or user.full_name,
        "phone": data.get("phone", ""),
        "service": data.get("service", ""),
        "district": data.get("district", ""),
        "budget": data.get("budget", ""),
        "note": data.get("note", ""),
        "referral_code": data.get("referral_code", ""),
        "status": "new",
        "agent_tg_id": "",
        "agent_name": "",
        "taken_at": "",
        "done_at": "",
        "client_message_id": "",
        "agent_group_message_id": "",
    }

    row = [
        lead["lead_id"],
        lead["created_at"],
        lead["client_tg_id"],
        lead["client_username"],
        lead["client_full_name"],
        lead["phone"],
        lead["service"],
        lead["district"],
        lead["budget"],
        lead["note"],
        lead["referral_code"],
        lead["status"],
        lead["agent_tg_id"],
        lead["agent_name"],
        lead["taken_at"],
        lead["done_at"],
        lead["client_message_id"],
        lead["agent_group_message_id"],
    ]
    db.append_lead(row)

    client_msg = await message.answer(
        build_client_success_text(lead_id),
        reply_markup=build_main_menu(),
    )

    db.update_lead_fields(
        lead_id,
        {"client_message_id": str(client_msg.message_id)},
    )

    agent_group_message_id = await notify_agents_about_lead(lead)
    if agent_group_message_id:
        db.update_lead_fields(
            lead_id,
            {"agent_group_message_id": str(agent_group_message_id)},
        )

    await notify_admins(
        f"📥 Янги лид тушди\n"
        f"🆔 {lead_id}\n"
        f"👤 {lead['client_full_name']}\n"
        f"🛎 {lead['service']}"
    )

    await state.clear()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    text = (
        f"Assalomu alaykum, <b>{message.from_user.full_name}</b>!\n\n"
        "Golden Key professional bot’га хуш келибсиз.\n"
        "Қуйидаги менюдан фойдаланинг."
    )
    await message.answer(text, reply_markup=build_main_menu())


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Сизда рухсат йўқ.")
        return

    daily = db.get_daily_stats()
    monthly = db.get_monthly_stats()
    top_agents = db.get_top_agents()

    top_text = ""
    if top_agents:
        for i, (name, count) in enumerate(top_agents, start=1):
            top_text += f"{i}. {name} — {count} та\n"
    else:
        top_text = "Ҳозирча маълумот йўқ"

    text = (
        "📊 <b>ADMIN PANEL</b>\n\n"
        "<b>Бугун:</b>\n"
        f"• Янги: {daily['new']}\n"
        f"• Олдим: {daily['taken']}\n"
        f"• Якунланган: {daily['done']}\n"
        f"• Рад этилган: {daily['rejected']}\n\n"
        "<b>Ойлик:</b>\n"
        f"• Янги: {monthly['new']}\n"
        f"• Олдим: {monthly['taken']}\n"
        f"• Якунланган: {monthly['done']}\n"
        f"• Рад этилган: {monthly['rejected']}\n\n"
        "<b>Топ агентлар:</b>\n"
        f"{top_text}"
    )
    await message.answer(text)


@router.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    await message.answer("pong ✅")


@router.message(F.text == "📝 Заявка қолдириш")
async def menu_lead(message: Message, state: FSMContext) -> None:
    await start_lead_form(message, state)


@router.message(F.text == "📞 Алоқа")
async def menu_contact(message: Message) -> None:
    await message.answer("📞 Алоқа учун:\n<b>+998 99 999 79 73</b>")


@router.message(F.text == "ℹ️ Хизматлар")
async def menu_services(message: Message) -> None:
    await message.answer(build_services_text())


@router.message(LeadForm.waiting_phone, F.contact)
async def form_phone_contact(message: Message, state: FSMContext) -> None:
    phone = normalize_phone(message.contact.phone_number)
    if not phone_is_valid(phone):
        await message.answer("Телефон рақам нотўғри. Қайта юборинг.")
        return

    await state.update_data(
        phone=phone,
        client_full_name=message.from_user.full_name,
    )
    await state.set_state(LeadForm.waiting_service)
    await message.answer("Қайси хизмат керак?", reply_markup=build_services_kb())


@router.message(LeadForm.waiting_phone, F.text)
async def form_phone_text(message: Message, state: FSMContext) -> None:
    phone = normalize_phone(message.text)
    if not phone_is_valid(phone):
        await message.answer(
            "Телефон рақамни тўғри киритинг.\nМасалан: <code>+998901234567</code>",
            reply_markup=build_phone_kb(),
        )
        return

    await state.update_data(
        phone=phone,
        client_full_name=message.from_user.full_name,
    )
    await state.set_state(LeadForm.waiting_service)
    await message.answer("Қайси хизмат керак?", reply_markup=build_services_kb())


@router.message(LeadForm.waiting_service, F.text)
async def form_service(message: Message, state: FSMContext) -> None:
    if message.text not in SERVICES:
        await message.answer("Илтимос, менюдан танланг.", reply_markup=build_services_kb())
        return

    await state.update_data(service=message.text)
    await state.set_state(LeadForm.waiting_district)
    await message.answer("Ҳудудни танланг:", reply_markup=build_districts_kb())


@router.message(LeadForm.waiting_district, F.text)
async def form_district(message: Message, state: FSMContext) -> None:
    await state.update_data(district=message.text.strip())
    await state.set_state(LeadForm.waiting_budget)
    await message.answer(
        "Бюджет ёки нарх оралиғини ёзинг.\nМасалан: <code>400 млн</code> ёки <code>300-450 млн</code>",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(LeadForm.waiting_budget, F.text)
async def form_budget(message: Message, state: FSMContext) -> None:
    await state.update_data(budget=message.text.strip())
    await state.set_state(LeadForm.waiting_note)
    await message.answer("Қўшимча изоҳ ёзинг.\nАгар бўлмаса <code>-</code> деб юборинг.")


@router.message(LeadForm.waiting_note, F.text)
async def form_note(message: Message, state: FSMContext) -> None:
    note = message.text.strip() or "-"
    await state.update_data(note=note)
    await state.set_state(LeadForm.waiting_referral)
    await message.answer(
        "Агар сизни йўналтирган махсус агент бўлса, referral кодини ёзинг.\nБўлмаса <code>-</code> деб юборинг."
    )


@router.message(LeadForm.waiting_referral, F.text)
async def form_referral(message: Message, state: FSMContext) -> None:
    referral = message.text.strip()
    if referral == "-":
        referral = ""
    await state.update_data(referral_code=referral)
    await finalize_lead(message, state)


@router.callback_query(F.data.startswith("lead:"))
async def callback_lead_actions(callback: CallbackQuery) -> None:
    try:
        _, action, lead_id = callback.data.split(":")
    except Exception:
        await callback.answer("Нотўғри callback", show_alert=True)
        return

    if action not in AGENT_ACTIONS and action != "none":
        await callback.answer("Нотўғри амал", show_alert=True)
        return

    lead = db.get_lead(lead_id)
    if not lead:
        await callback.answer("Lead topilmadi", show_alert=True)
        return

    user = callback.from_user
    status = esc(lead.get("status")).lower()

    if action == "take":
        if status in {"taken", "done"}:
            await callback.answer("Бу лид олдин олинган.", show_alert=True)
            return

        db.update_lead_fields(
            lead_id,
            {
                "status": "taken",
                "agent_tg_id": str(user.id),
                "agent_name": user.full_name,
                "taken_at": now_str(),
            },
        )

        updated = db.get_lead(lead_id)
        await callback.message.edit_text(
            build_lead_text(updated),
            reply_markup=build_agent_inline(lead_id, taken=True),
        )

        client_tg_id = safe_int(updated.get("client_tg_id"))
        if client_tg_id:
            try:
                await bot.send_message(
                    client_tg_id,
                    "✅ Сизнинг заявкангиз мутахассис томонидан қабул қилинди.\nЯқин вақт ичида сиз билан боғланишади."
                )
            except Exception as e:
                logger.warning("Client notify failed (taken): %s", e)

        await callback.answer("Лид сизга бириктирилди")
        return

    if action == "reject":
        if status == "done":
            await callback.answer("Бу лид якунланган.", show_alert=True)
            return

        db.update_lead_fields(lead_id, {"status": "rejected"})

        updated = db.get_lead(lead_id)
        await callback.message.edit_text(
            build_lead_text(updated),
            reply_markup=build_agent_inline(lead_id, taken=False),
        )
        await callback.answer("Лид рад этилди")
        return

    if action == "done":
        if status != "taken":
            await callback.answer("Аввал лидни олиш керак.", show_alert=True)
            return

        if str(lead.get("agent_tg_id", "")).strip() != str(user.id):
            await callback.answer("Бу лидни фақат олган агент якунлай олади.", show_alert=True)
            return

        db.update_lead_fields(
            lead_id,
            {
                "status": "done",
                "done_at": now_str(),
            },
        )

        updated = db.get_lead(lead_id)
        await callback.message.edit_text(
            build_lead_text(updated),
            reply_markup=build_agent_inline(lead_id, finished=True),
        )

        client_tg_id = safe_int(updated.get("client_tg_id"))
        if client_tg_id:
            try:
                await bot.send_message(
                    client_tg_id,
                    "🎉 Сизнинг мурожаатингиз якунланди.\nGolden Key хизматларидан фойдаланганингиз учун раҳмат."
                )
            except Exception as e:
                logger.warning("Client notify failed (done): %s", e)

        referral_code = esc(updated.get("referral_code")).strip()
        if referral_code:
            db.append_bonus([
                now_str(),
                lead_id,
                referral_code,
                BONUS_AMOUNT,
                "pending",
            ])

            if referral_code.isdigit():
                try:
                    await bot.send_message(
                        int(referral_code),
                        f"🎁 Сиз йўналтирган мижознинг иши якунланди.\nБонус: <b>{BONUS_AMOUNT:,}</b> сўм"
                    )
                except Exception as e:
                    logger.warning("Referral notify failed: %s", e)

            await notify_admins(
                "🎁 Bonus trigger\n"
                f"Lead ID: {lead_id}\n"
                f"Referral: {referral_code}\n"
                f"Amount: {BONUS_AMOUNT}"
            )

        await callback.answer("Лид якунланди")
        return

    await callback.answer("OK")


@router.message(F.text)
async def text_fallback(message: Message) -> None:
    await message.answer("Менюдан фойдаланинг:", reply_markup=build_main_menu())


dp.include_router(router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting app...")
    try:
        title = await asyncio.to_thread(db.healthcheck)
        logger.info("Google Sheets connected: %s", title)
    except Exception as e:
        logger.exception("Google Sheets connection failed: %s", e)
        raise

    try:
        await bot.set_webhook(
            url=WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
        )
        logger.info("Webhook set: %s", WEBHOOK_URL)
    except Exception as e:
        logger.exception("Webhook setup failed: %s", e)
        raise

    yield

    logger.info("Stopping app...")
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception as e:
        logger.warning("Webhook delete failed: %s", e)

    await bot.session.close()


app = FastAPI(title="Golden Key Bot", lifespan=lifespan)


@app.get("/")
async def root() -> Dict[str, Any]:
    return {"ok": True, "service": "golden-key-bot", "env": ENVIRONMENT}


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request) -> JSONResponse:
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    telegram_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if telegram_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret token")

    try:
        data = await request.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.exception("Webhook processing failed: %s", e)
        raise HTTPException(status_code=500, detail="Webhook error")