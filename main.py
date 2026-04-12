import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import gspread
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from google.oauth2.service_account import Credentials

from config import settings, validate_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("gk_bot")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

PURPOSE_OPTIONS = [
    "сотиш",
    "ижара",
    "ипотека",
    "сотиб олиш учун",
    "ижарага олиш учун",
]


class LeadForm(StatesGroup):
    full_name = State()
    phone = State()
    purpose = State()
    notes = State()


class SearchForm(StatesGroup):
    keyword = State()


@dataclass
class UserRow:
    tg_id: int
    full_name: str
    username: str
    phone: str
    role: str
    status: str
    ref_by: str
    joined_at: str


class SheetDB:
    def __init__(self):
        self.gc = None

        self.main_sh = None
        self.props_sh = None

        self.users_ws = None
        self.leads_ws = None
        self.settings_ws = None
        self.objects_ws = None

    async def connect(self):
        await asyncio.to_thread(self._connect_sync)

    def _connect_sync(self):
        creds = Credentials.from_service_account_info(
            settings.service_account_info,
            scopes=SCOPES,
        )
        self.gc = gspread.authorize(creds)

        # 1-шит: Users / Leads / Settings
        self.main_sh = self.gc.open_by_url(settings.spreadsheet_url)

        # 2-шит: Properties
        self.props_sh = self.gc.open_by_url(settings.properties_spreadsheet_url)

        self.users_ws = self._get_or_create_ws(
            self.main_sh,
            "Users",
            [
                "tg_id",
                "full_name",
                "username",
                "phone",
                "role",
                "status",
                "ref_by",
                "joined_at",
            ],
        )

        self.leads_ws = self._get_or_create_ws(
            self.main_sh,
            "Leads",
            [
                "lead_id",
                "created_at",
                "client_tg_id",
                "client_name",
                "client_phone",
                "purpose",
                "notes",
                "status",
                "assigned_agent_id",
                "assigned_agent_name",
                "assigned_message_ids",
                "ref_by",
                "completed_at",
            ],
        )

        self.settings_ws = self._get_or_create_ws(
            self.main_sh,
            "Settings",
            ["key", "value"],
        )

        self.objects_ws = self._get_or_create_ws(
            self.props_sh,
            "Properties",
            [
                "ID",
                "created_at",
                "lead_id",
                "created_by_agent_id",
                "created_by_agent_name",
                "owner_name",
                "Phone",
                "property_type",
                "Purpose",
                "street_raw",
                "street_normalized",
                "Address",
                "district",
                "Rooms",
                "Floor",
                "total_floors",
                "Area",
                "Price",
                "currency",
                "ownership",
                "renovation",
                "Mortgage",
                "InitialPayment",
                "Landmark",
                "description",
                "photo_1",
                "photo_2",
                "photo_3",
                "photo_4",
                "photo_5",
                "photo_6",
                "photo_7",
                "photo_8",
                "photo_9",
                "photo_10",
                "ready_post",
                "post_status",
                "telegram_message_id",
                "Status",
                "SENT",
                "LocationUrl",
            ],
        )

    def _get_or_create_ws(self, spreadsheet, title: str, headers: List[str]):
        try:
            ws = spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=max(len(headers), 20))
            ws.append_row(headers)
            return ws

        existing = ws.row_values(1)
        if not existing:
            ws.append_row(headers)

        return ws

    async def get_user(self, tg_id: int) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_user_sync, tg_id)

    def _get_user_sync(self, tg_id: int) -> Optional[Dict[str, Any]]:
        records = self.users_ws.get_all_records()
        for row in records:
            if str(row.get("tg_id", "")) == str(tg_id):
                return row
        return None

    async def upsert_user(
        self,
        tg_id: int,
        full_name: str,
        username: str = "",
        phone: str = "",
        role: str = "client",
        status: str = "active",
        ref_by: str = "",
    ):
        await asyncio.to_thread(
            self._upsert_user_sync,
            tg_id,
            full_name,
            username,
            phone,
            role,
            status,
            ref_by,
        )

    def _upsert_user_sync(
        self,
        tg_id: int,
        full_name: str,
        username: str,
        phone: str,
        role: str,
        status: str,
        ref_by: str,
    ):
        all_values = self.users_ws.get_all_values()
        joined_at = now_str()
        new_row = [str(tg_id), full_name, username, phone, role, status, ref_by, joined_at]

        for idx, row in enumerate(all_values[1:], start=2):
            if str(row[0]) == str(tg_id):
                current = row + [""] * (8 - len(row))
                current[1] = full_name or current[1]
                current[2] = username or current[2]
                current[3] = phone or current[3]
                current[4] = role or current[4]
                current[5] = status or current[5]
                if ref_by and not current[6]:
                    current[6] = ref_by
                self.users_ws.update(f"A{idx}:H{idx}", [current[:8]])
                return

        self.users_ws.append_row(new_row)

    async def update_user_fields(self, tg_id: int, **fields):
        await asyncio.to_thread(self._update_user_fields_sync, tg_id, fields)

    def _update_user_fields_sync(self, tg_id: int, fields: Dict[str, Any]):
        headers = self.users_ws.row_values(1)
        all_values = self.users_ws.get_all_values()

        for idx, row in enumerate(all_values[1:], start=2):
            if str(row[0]) == str(tg_id):
                row = row + [""] * (len(headers) - len(row))
                for key, value in fields.items():
                    if key in headers:
                        row[headers.index(key)] = str(value)
                end_col = chr(64 + len(headers))
                self.users_ws.update(f"A{idx}:{end_col}{idx}", [row[:len(headers)]])
                return

    async def list_agents(self, active_only: bool = True) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._list_agents_sync, active_only)

    def _list_agents_sync(self, active_only: bool) -> List[Dict[str, Any]]:
        records = self.users_ws.get_all_records()
        out = []

        for row in records:
            if row.get("role") == "agent":
                if active_only and row.get("status") != "active":
                    continue
                out.append(row)

        return out

    async def search_objects(self, keyword: str) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._search_objects_sync, keyword)

    def _search_objects_sync(self, keyword: str) -> List[Dict[str, Any]]:
        k = keyword.lower().strip()
        records = self.objects_ws.get_all_records()
        results = []

        for row in records:
            hay = " ".join(str(v) for v in row.values()).lower()

            status = str(row.get("Status", "")).strip().lower()
            if status not in ("pending", "active", "ready", ""):
                continue

            if k in hay:
                results.append(row)

        return results[:20]

    async def create_lead(self, data: Dict[str, Any]) -> str:
        return await asyncio.to_thread(self._create_lead_sync, data)

    def _create_lead_sync(self, data: Dict[str, Any]) -> str:
        lead_id = self._next_lead_id()
        row = [
            lead_id,
            now_str(),
            str(data.get("client_tg_id", "")),
            data.get("client_name", ""),
            data.get("client_phone", ""),
            data.get("purpose", ""),
            data.get("notes", ""),
            "new",
            "",
            "",
            "",
            data.get("ref_by", ""),
            "",
        ]
        self.leads_ws.append_row(row)
        return lead_id

    def _next_lead_id(self) -> str:
        records = self.leads_ws.get_all_records()
        max_num = 0

        for row in records:
            lid = str(row.get("lead_id", ""))
            m = re.match(r"LD-(\d+)", lid)
            if m:
                max_num = max(max_num, int(m.group(1)))

        return f"LD-{max_num + 1:03d}"

    async def get_lead(self, lead_id: str) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_lead_sync, lead_id)

    def _get_lead_sync(self, lead_id: str) -> Optional[Dict[str, Any]]:
        for row in self.leads_ws.get_all_records():
            if row.get("lead_id") == lead_id:
                return row
        return None

    async def update_lead(self, lead_id: str, **fields):
        await asyncio.to_thread(self._update_lead_sync, lead_id, fields)

    def _update_lead_sync(self, lead_id: str, fields: Dict[str, Any]):
        headers = self.leads_ws.row_values(1)
        all_values = self.leads_ws.get_all_values()

        for idx, row in enumerate(all_values[1:], start=2):
            if row and row[0] == lead_id:
                row = row + [""] * (len(headers) - len(row))
                for key, value in fields.items():
                    if key in headers:
                        row[headers.index(key)] = str(value)
                end_col = chr(64 + len(headers))
                self.leads_ws.update(f"A{idx}:{end_col}{idx}", [row[:len(headers)]])
                return

    async def stats(self) -> Dict[str, int]:
        return await asyncio.to_thread(self._stats_sync)

    def _stats_sync(self):
        users = self.users_ws.get_all_records()
        leads = self.leads_ws.get_all_records()
        objects = self.objects_ws.get_all_records()

        return {
            "users": len(users),
            "agents": sum(1 for x in users if x.get("role") == "agent" and x.get("status") == "active"),
            "pending_agents": sum(1 for x in users if x.get("role") == "agent" and x.get("status") == "pending"),
            "leads": len(leads),
            "new_leads": sum(1 for x in leads if x.get("status") == "new"),
            "taken_leads": sum(1 for x in leads if x.get("status") == "taken"),
            "done_leads": sum(1 for x in leads if x.get("status") == "done"),
            "objects": len(objects),
        }


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📞 Телефон юбориш", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def main_menu(role: str) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📝 Заявка қолдириш"), KeyboardButton(text="🔎 Объект қидириш")],
        [KeyboardButton(text=f"📞 Алоқа: {settings.contact_phone}")],
    ]

    if role in ("agent", "admin"):
        rows.insert(1, [KeyboardButton(text="🏠 Объект қўшиш"), KeyboardButton(text="🔗 Махсус агент линк")])

    if role == "client":
        rows.append([KeyboardButton(text="🧑‍💼 Агент бўлиш")])

    if role == "admin":
        rows.append([KeyboardButton(text="📊 Админ статистика")])

    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def purpose_kb() -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=x)] for x in PURPOSE_OPTIONS]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def build_lead_text(lead_id: str, name: str, phone: str, purpose: str, notes: str, ref_by: str = "") -> str:
    text = (
        f"🆕 <b>Янги лид</b>\n\n"
        f"🆔 ID: <b>{lead_id}</b>\n"
        f"👤 Исм: {name}\n"
        f"📞 Телефон: {phone}\n"
        f"🎯 Мақсад: {purpose}\n"
        f"📝 Изоҳ: {notes or '-'}\n"
    )
    if ref_by:
        text += f"🤝 Махсус агент ID: {ref_by}\n"
    return text


def lead_action_kb(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Олдим", callback_data=f"lead_take:{lead_id}")],
            [InlineKeyboardButton(text="❌ Рад этдим", callback_data=f"lead_reject:{lead_id}")],
            [InlineKeyboardButton(text="🏁 Бажарилди", callback_data=f"lead_done:{lead_id}")],
        ]
    )


def admin_approve_kb(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Тасдиқлаш", callback_data=f"agent_approve:{tg_id}")],
            [InlineKeyboardButton(text="❌ Бекор қилиш", callback_data=f"agent_reject:{tg_id}")],
        ]
    )


bot = Bot(
    token=settings.bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())
db = SheetDB()


async def safe_send(chat_id: int, text: str, **kwargs):
    try:
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except Exception as e:
        logger.warning("send failed to %s: %s", chat_id, e)
        return None


async def notify_admins(text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    for admin_id in settings.admins:
        await safe_send(admin_id, text, reply_markup=reply_markup)


async def notify_agents_about_lead(lead_id: str):
    lead = await db.get_lead(lead_id)
    if not lead:
        return

    agents = await db.list_agents(active_only=True)
    msg_ids = []
    text = build_lead_text(
        lead_id,
        lead.get("client_name", ""),
        lead.get("client_phone", ""),
        lead.get("purpose", ""),
        lead.get("notes", ""),
        lead.get("ref_by", ""),
    )

    for agent in agents:
        tg_id_raw = str(agent.get("tg_id", "")).strip()
        if not tg_id_raw.isdigit():
            continue

        tg_id = int(tg_id_raw)
        sent = await safe_send(tg_id, text, reply_markup=lead_action_kb(lead_id))
        if sent:
            msg_ids.append(f"{tg_id}:{sent.message_id}")

    if msg_ids:
        await db.update_lead(lead_id, assigned_message_ids="|".join(msg_ids))


async def close_other_messages(lead: Dict[str, Any], except_agent_id: Optional[int] = None):
    raw = lead.get("assigned_message_ids", "")
    if not raw:
        return

    for item in str(raw).split("|"):
        try:
            chat_id_str, message_id_str = item.split(":")
            chat_id = int(chat_id_str)
            message_id = int(message_id_str)

            if except_agent_id and chat_id == except_agent_id:
                continue

            try:
                await bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=None,
                )
            except TelegramBadRequest:
                pass
        except Exception:
            continue


@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject):
    ref_by = ""
    if command.args and command.args.startswith("ref_"):
        ref_by = command.args.replace("ref_", "", 1)

    existing = await db.get_user(message.from_user.id)
    role = "admin" if message.from_user.id in settings.admins else (existing.get("role") if existing else "client")
    status = existing.get("status") if existing else "active"

    await db.upsert_user(
        tg_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username or "",
        role=role,
        status=status,
        ref_by=ref_by,
    )

    text = (
        f"Ассалому алайкум, <b>{message.from_user.full_name}</b>!\n\n"
        f"{settings.company_name} ботга хуш келибсиз.\n"
        f"Қуйидаги менюлардан бирини танланг."
    )

    if ref_by:
        text += "\n\n🤝 Сиз махсус агент ҳаволаси орқали кирдингиз."

    await message.answer(text, reply_markup=main_menu(role))


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in settings.admins:
        return await message.answer("Бу бўлим фақат админ учун.")

    s = await db.stats()
    await message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"👥 Фойдаланувчилар: {s['users']}\n"
        f"🧑‍💼 Актив агентлар: {s['agents']}\n"
        f"⏳ Кутилаётган агентлар: {s['pending_agents']}\n"
        f"📥 Лидлар: {s['leads']}\n"
        f"🆕 Янги лидлар: {s['new_leads']}\n"
        f"✅ Олинган лидлар: {s['taken_leads']}\n"
        f"🏁 Якунланган лидлар: {s['done_leads']}\n"
        f"🏠 Объектлар: {s['objects']}"
    )


@dp.message(Command("ref"))
async def cmd_ref(message: Message):
    user = await db.get_user(message.from_user.id)
    if not user or user.get("role") not in ("agent", "admin"):
        return await message.answer("Бу функция агент ва админ учун.")

    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{message.from_user.id}"
    await message.answer(
        "🔗 <b>Махсус агент линк</b>\n\n"
        f"Ушбу линкни мижозга юборинг:\n{link}\n\n"
        "Мижоз шу линк орқали кирса, lead сизга referral сифатида боғланади."
    )


@dp.message(F.text == "📊 Админ статистика")
async def admin_stats_button(message: Message):
    await cmd_admin(message)


@dp.message(F.text == "🧑‍💼 Агент бўлиш")
async def request_agent_role(message: Message):
    user = await db.get_user(message.from_user.id)

    if user and user.get("role") == "agent" and user.get("status") == "active":
        return await message.answer("Сиз аллақачон актив агентсиз.")

    await db.upsert_user(
        tg_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username or "",
        role="agent",
        status="pending",
        ref_by=user.get("ref_by", "") if user else "",
    )

    await message.answer("Сўров юборилди. Админ тасдиғидан кейин агент бўласиз.")
    await notify_admins(
        "🧑‍💼 <b>Янги агент сўрови</b>\n\n"
        f"👤 {message.from_user.full_name}\n"
        f"🆔 <code>{message.from_user.id}</code>\n"
        f"🔗 @{message.from_user.username or '-'}",
        reply_markup=admin_approve_kb(message.from_user.id),
    )


@dp.callback_query(F.data.startswith("agent_approve:"))
async def approve_agent(call: CallbackQuery):
    if call.from_user.id not in settings.admins:
        return await call.answer("Фақат админ", show_alert=True)

    tg_id = int(call.data.split(":", 1)[1])
    user = await db.get_user(tg_id)

    if not user:
        return await call.answer("Фойдаланувчи топилмади", show_alert=True)

    await db.update_user_fields(tg_id, role="agent", status="active")
    await safe_send(
        tg_id,
        "🎉 Табриклаймиз! Сиз агент сифатида тасдиқландингиз.",
        reply_markup=main_menu("agent"),
    )
    await call.message.edit_reply_markup(reply_markup=None)
    await call.answer("Тасдиқланди")


@dp.callback_query(F.data.startswith("agent_reject:"))
async def reject_agent(call: CallbackQuery):
    if call.from_user.id not in settings.admins:
        return await call.answer("Фақат админ", show_alert=True)

    tg_id = int(call.data.split(":", 1)[1])
    await db.update_user_fields(tg_id, role="client", status="active")
    await safe_send(
        tg_id,
        "Сизнинг агент сўровингиз ҳозирча тасдиқланмади.",
        reply_markup=main_menu("client"),
    )
    await call.message.edit_reply_markup(reply_markup=None)
    await call.answer("Бекор қилинди")


@dp.message(F.text == "📝 Заявка қолдириш")
async def start_lead(message: Message, state: FSMContext):
    await state.set_state(LeadForm.full_name)
    await message.answer("Исмингизни киритинг:", reply_markup=ReplyKeyboardRemove())


@dp.message(LeadForm.full_name)
async def lead_name(message: Message, state: FSMContext):
    await state.update_data(full_name=message.text.strip())
    await state.set_state(LeadForm.phone)
    await message.answer("Телефон рақамингизни юборинг ёки ёзинг:", reply_markup=phone_kb())


@dp.message(LeadForm.phone, F.contact)
async def lead_phone_contact(message: Message, state: FSMContext):
    await state.update_data(phone=message.contact.phone_number)
    await state.set_state(LeadForm.purpose)
    await message.answer("Мақсадни танланг:", reply_markup=purpose_kb())


@dp.message(LeadForm.phone)
async def lead_phone_text(message: Message, state: FSMContext):
    phone = message.text.strip()
    await state.update_data(phone=phone)
    await state.set_state(LeadForm.purpose)
    await message.answer("Мақсадни танланг:", reply_markup=purpose_kb())


@dp.message(LeadForm.purpose)
async def lead_purpose(message: Message, state: FSMContext):
    await state.update_data(purpose=message.text.strip())
    await state.set_state(LeadForm.notes)
    await message.answer("Қўшимча изоҳ ёзинг:", reply_markup=ReplyKeyboardRemove())


@dp.message(LeadForm.notes)
async def lead_notes(message: Message, state: FSMContext):
    data = await state.get_data()
    notes = message.text.strip()
    user = await db.get_user(message.from_user.id)

    await db.upsert_user(
        tg_id=message.from_user.id,
        full_name=data.get("full_name") or message.from_user.full_name,
        username=message.from_user.username or "",
        phone=data.get("phone", ""),
        role="admin" if message.from_user.id in settings.admins else (user.get("role") if user else "client"),
        status=user.get("status", "active") if user else "active",
        ref_by=user.get("ref_by", "") if user else "",
    )

    lead_id = await db.create_lead(
        {
            "client_tg_id": message.from_user.id,
            "client_name": data.get("full_name"),
            "client_phone": data.get("phone"),
            "purpose": data.get("purpose"),
            "notes": notes,
            "ref_by": user.get("ref_by", "") if user else "",
        }
    )

    await state.clear()
    await message.answer(
        f"✅ Заявкангиз қабул қилинди. ID: <b>{lead_id}</b>\nТез орада агент сиз билан боғланади.",
        reply_markup=main_menu(user.get("role") if user else "client"),
    )

    await notify_agents_about_lead(lead_id)
    await notify_admins(f"📥 Янги лид яратилди: <b>{lead_id}</b>")


@dp.callback_query(F.data.startswith("lead_take:"))
async def take_lead(call: CallbackQuery):
    lead_id = call.data.split(":", 1)[1]
    lead = await db.get_lead(lead_id)

    if not lead:
        return await call.answer("Лид топилмади", show_alert=True)

    if lead.get("status") == "done":
        return await call.answer("Бу лид якунланган", show_alert=True)

    if lead.get("assigned_agent_id") and str(lead.get("assigned_agent_id")) != str(call.from_user.id):
        return await call.answer("Бу лидни бошқа агент олган", show_alert=True)

    await db.update_lead(
        lead_id,
        status="taken",
        assigned_agent_id=call.from_user.id,
        assigned_agent_name=call.from_user.full_name,
    )
    await close_other_messages(lead, except_agent_id=call.from_user.id)

    try:
        await call.message.edit_reply_markup(reply_markup=lead_action_kb(lead_id))
    except TelegramBadRequest:
        pass

    await call.answer("Лид сизга бириктирилди")

    client_tg_id = str(lead.get("client_tg_id") or "").strip()
    if client_tg_id.isdigit():
        await safe_send(
            int(client_tg_id),
            f"✅ Сизнинг заявкангиз агент <b>{call.from_user.full_name}</b> га бириктирилди. Яқин орада сиз билан боғланишади.",
        )


@dp.callback_query(F.data.startswith("lead_reject:"))
async def reject_lead(call: CallbackQuery):
    lead_id = call.data.split(":", 1)[1]
    lead = await db.get_lead(lead_id)

    if not lead:
        return await call.answer("Лид топилмади", show_alert=True)

    if str(lead.get("assigned_agent_id") or "") not in ("", str(call.from_user.id)):
        return await call.answer("Бу лид сизга тегишли эмас", show_alert=True)

    await db.update_lead(
        lead_id,
        status="new",
        assigned_agent_id="",
        assigned_agent_name="",
    )
    await call.answer("Лид қайта очилди")
    await notify_agents_about_lead(lead_id)


@dp.callback_query(F.data.startswith("lead_done:"))
async def finish_lead(call: CallbackQuery):
    lead_id = call.data.split(":", 1)[1]
    lead = await db.get_lead(lead_id)

    if not lead:
        return await call.answer("Лид топилмади", show_alert=True)

    if str(lead.get("assigned_agent_id") or "") != str(call.from_user.id):
        return await call.answer("Фақат лидни олган агент якунлай олади", show_alert=True)

    await db.update_lead(lead_id, status="done", completed_at=now_str())
    await close_other_messages(lead)
    await call.answer("Лид якунланди")

    client_tg_id = str(lead.get("client_tg_id") or "").strip()
    if client_tg_id.isdigit():
        await safe_send(int(client_tg_id), "🏁 Сизнинг мурожаатингиз якунланди. Раҳмат!")

    ref_by = str(lead.get("ref_by") or "").strip()
    if ref_by.isdigit():
        await safe_send(
            int(ref_by),
            "🎁 Сиз юборган мижознинг иши якунланди. Бонусингизни офисдан олиб кетишингиз мумкин.",
        )


@dp.message(F.text == "🏠 Объект қўшиш")
async def start_object(message: Message):
    user = await db.get_user(message.from_user.id)
    role = "admin" if message.from_user.id in settings.admins else (user.get("role") if user else "client")

    if role not in ("agent", "admin"):
        return await message.answer("Бу функция фақат агент ва админ учун.")

    text = (
        "🏠 <b>Объект қўшиш</b>\n\n"
        "Қуйидаги AppSheet линк орқали объект маълумотларини киритинг:\n\n"
        f"{settings.appsheet_url}\n\n"
        "✅ Объект сақлангач, у базага тушади ва автомат постга юборилади."
    )
    await message.answer(text, disable_web_page_preview=True)


@dp.message(F.text == "🔎 Объект қидириш")
async def start_search(message: Message, state: FSMContext):
    await state.set_state(SearchForm.keyword)
    await message.answer(
        "Қидирув учун калит сўз киритинг (масалан: 3 хона, ипотека, GK-001, манзил):",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(SearchForm.keyword)
async def do_search(message: Message, state: FSMContext):
    keyword = message.text.strip()
    results = await db.search_objects(keyword)
    await state.clear()

    user = await db.get_user(message.from_user.id)
    role = "admin" if message.from_user.id in settings.admins else (user.get("role") if user else "client")

    if not results:
        return await message.answer("Ҳеч нарса топилмади.", reply_markup=main_menu(role))

    is_full = role in ("agent", "admin")

    for row in results[:10]:
        id_ = row.get("ID", "")
        address = row.get("Address", "")
        floor = row.get("Floor", "")
        rooms = row.get("Rooms", "")
        ownership = row.get("ownership", "")
        area = row.get("Area", "")
        purpose = row.get("Purpose", "")
        price = row.get("Price", "")
        photo_1 = row.get("photo_1", "")
        landmark = row.get("Landmark", "")
        mortgage = row.get("Mortgage", "")
        initial_payment = row.get("InitialPayment", "")
        property_type = row.get("property_type", "")
        description = row.get("description", "")
        status = row.get("Status", "")

        if is_full:
            text = (
                f"🏠 <b>{id_}</b>\n"
                f"📍 Манзил: {address}\n"
                f"🏢 Қават: {floor}\n"
                f"🛏 Хоналар: {rooms}\n"
                f"📜 Мулкчилик: {ownership}\n"
                f"📐 Майдон: {area}\n"
                f"🎯 Мақсад: {purpose}\n"
                f"💵 Нарх: {price}\n"
                f"🖼 Фото: {photo_1}\n"
                f"📌 Мўлжал: {landmark}\n"
                f"🏦 Ипотека: {mortgage}\n"
                f"💰 Бош тўлов: {initial_payment}\n"
                f"🏷 Тури: {property_type}\n"
                f"📝 Тавсиф: {description}\n"
                f"📌 Статус: {status}"
            )
        else:
            text = (
                f"🏠 <b>{id_}</b>\n"
                f"📍 Манзил: {address}\n"
                f"🏢 Қават: {floor}\n"
                f"🛏 Хоналар: {rooms}\n"
                f"📜 Мулкчилик: {ownership}\n"
                f"📐 Майдон: {area}\n"
                f"🎯 Мақсад: {purpose}\n"
                f"💵 Нарх: {price}\n"
                f"🖼 Фото: {photo_1}\n"
                f"📌 Мўлжал: {landmark}\n"
                f"🏦 Ипотека: {mortgage}\n"
                f"💰 Бош тўлов: {initial_payment}"
            )

        await message.answer(text)

    await message.answer("Қидирув якунланди.", reply_markup=main_menu(role))


@dp.message(F.text == "🔗 Махсус агент линк")
async def menu_ref(message: Message):
    await cmd_ref(message)


@dp.message()
async def fallback(message: Message):
    user = await db.get_user(message.from_user.id)
    role = "admin" if message.from_user.id in settings.admins else (user.get("role") if user else "client")
    await message.answer(
        "Менюдан бирини танланг ёки /start ни босинг.",
        reply_markup=main_menu(role),
    )


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "ok": True,
            "service": "gk-railway-bot",
            "time": now_str(),
        }
    )


async def start_http_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()

    logger.info("Healthcheck server started on :%s", port)


async def main():
    validate_settings()
    await start_http_server()
    await db.connect()
    logger.info("Bot polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())