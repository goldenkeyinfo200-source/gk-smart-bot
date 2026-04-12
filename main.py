import asyncio
import difflib
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


class ObjectInterestForm(StatesGroup):
    phone = State()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_text(text: str) -> str:
    if text is None:
        return ""

    t = str(text).lower().strip()

    custom_fixes = {
        "хукандий": "ҳўқандий",
        "хуканди": "ҳўқанди",
        "хуканд": "ҳўқанд",
        "куканд": "қўқон",
        "коканд": "қўқон",
        "кўкон": "қўқон",
        "қуқон": "қўқон",
        "қукон": "қўқон",
        "qoqon": "қўқон",
        "qoqan": "қўқон",
        "kokand": "қўқон",
        "kokon": "қўқон",
        "kukand": "қўқон",
        "fargona": "фарғона",
        "fergana": "фарғона",
        "andijon": "андижон",
        "andijan": "андижон",
        "namangan": "наманган",
        "toshkent": "тошкент",
        "tashkent": "тошкент",
        "ipoteka": "ипотека",
        "kvartira": "квартира",
        "xona": "хона",
        "xonali": "хонали",
        "xonadon": "хонадон",
        "uy": "уй",
        "hovli": "ҳовли",
        "dom": "уй",
        "moljal": "мўлжал",
        "mo'ljal": "мўлжал",
        "mo‘ljal": "мўлжал",
        "manzil": "манзил",
        "arenda": "ижара",
        "sotuv": "сотиш",
        "sotiladi": "сотиш",
        "ijara": "ижара",
        "ijaraga": "ижара",
    }

    replacements = {
        "o‘": "ў",
        "o'": "ў",
        "g‘": "ғ",
        "g'": "ғ",
        "sh": "ш",
        "ch": "ч",
        "yo": "ё",
        "yu": "ю",
        "ya": "я",
        "q": "қ",
    }

    for old, new in custom_fixes.items():
        t = t.replace(old, new)

    for old, new in replacements.items():
        t = t.replace(old, new)

    t = re.sub(r"[^a-zA-Zа-яА-ЯёЁқҚғҒҳҲўЎ0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def similarity(a: str, b: str) -> float:
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)
    if not a_norm or not b_norm:
        return 0.0
    return difflib.SequenceMatcher(None, a_norm, b_norm).ratio()


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

        self.main_sh = self.gc.open_by_url(settings.spreadsheet_url)
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
                "object_id",
                "lead_type",
                "contract_signed_at",
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
                "house_number",
                "apartment_number",
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
            if str(row.get("tg_id", "")).strip() == str(tg_id):
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
            if str(row[0]).strip() == str(tg_id):
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
            if str(row[0]).strip() == str(tg_id):
                row = row + [""] * (len(headers) - len(row))
                for key, value in fields.items():
                    if key in headers:
                        row[headers.index(key)] = str(value)
                end_col = chr(64 + len(headers))
                self.users_ws.update(f"A{idx}:{end_col}{idx}", [row[:len(headers)]])
                return

    async def list_agents_and_admins(self, active_only: bool = True) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._list_agents_and_admins_sync, active_only)

    def _list_agents_and_admins_sync(self, active_only: bool) -> List[Dict[str, Any]]:
        records = self.users_ws.get_all_records()
        out = []

        for row in records:
            role = row.get("role")
            status = row.get("status")
            if role in ("agent", "admin"):
                if active_only and status != "active":
                    continue
                out.append(row)

        for admin_id in settings.admins:
            found = any(str(x.get("tg_id", "")).strip() == str(admin_id) for x in out)
            if not found:
                out.append(
                    {
                        "tg_id": str(admin_id),
                        "full_name": f"Admin {admin_id}",
                        "username": "",
                        "phone": "",
                        "role": "admin",
                        "status": "active",
                    }
                )
        return out

    async def search_objects(self, keyword: str) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._search_objects_sync, keyword)

    def _search_objects_sync(self, keyword: str) -> List[Dict[str, Any]]:
        k = normalize_text(keyword)
        records = self.objects_ws.get_all_records()
        results = []

        for row in records:
            status = str(row.get("Status", "")).strip().lower()
            if status not in ("pending", "active", "ready", ""):
                continue

            searchable_fields = [
                row.get("ID", ""),
                row.get("Address", ""),
                row.get("district", ""),
                row.get("Landmark", ""),
                row.get("description", ""),
                row.get("property_type", ""),
                row.get("Purpose", ""),
                row.get("ownership", ""),
                row.get("Mortgage", ""),
                row.get("Rooms", ""),
                row.get("Floor", ""),
                row.get("Area", ""),
                row.get("Price", ""),
                row.get("street_raw", ""),
                row.get("street_normalized", ""),
                row.get("house_number", ""),
                row.get("apartment_number", ""),
                row.get("created_by_agent_name", ""),
            ]

            combined = " ".join(str(x) for x in searchable_fields if x is not None)
            combined_norm = normalize_text(combined)

            score = 0.0

            if k and k in combined_norm:
                score = max(score, 1.0)

            for field in searchable_fields:
                sim = similarity(k, str(field or ""))
                if sim > score:
                    score = sim

            tokens = [x for x in k.split() if x]
            if tokens:
                token_hits = sum(1 for token in tokens if token in combined_norm)
                token_score = token_hits / len(tokens)
                if token_score > score:
                    score = token_score

            if score >= 0.45:
                row["_score"] = round(score, 3)
                results.append(row)

        results.sort(key=lambda x: x.get("_score", 0), reverse=True)
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
            data.get("object_id", ""),
            data.get("lead_type", ""),
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

    async def get_property(self, object_id: str) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_property_sync, object_id)

    def _get_property_sync(self, object_id: str) -> Optional[Dict[str, Any]]:
        for row in self.objects_ws.get_all_records():
            if str(row.get("ID", "")).strip() == str(object_id).strip():
                return row
        return None

    async def update_property(self, object_id: str, **fields):
        await asyncio.to_thread(self._update_property_sync, object_id, fields)

    def _update_property_sync(self, object_id: str, fields: Dict[str, Any]):
        headers = self.objects_ws.row_values(1)
        all_values = self.objects_ws.get_all_values()

        for idx, row in enumerate(all_values[1:], start=2):
            row = row + [""] * (len(headers) - len(row))
            if str(row[0]).strip() == str(object_id).strip():
                for key, value in fields.items():
                    if key in headers:
                        row[headers.index(key)] = str(value)
                end_col = chr(64 + len(headers))
                self.objects_ws.update(f"A{idx}:{end_col}{idx}", [row[:len(headers)]])
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


def build_lead_text(
    lead_id: str,
    name: str,
    phone: str,
    purpose: str,
    notes: str,
    object_id: str = "",
    ref_by: str = "",
) -> str:
    text = (
        f"🆕 <b>Янги лид</b>\n\n"
        f"🆔 Lead ID: <b>{lead_id}</b>\n"
    )
    if object_id:
        text += f"🏠 Object ID: <b>{object_id}</b>\n"
    text += (
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
            [InlineKeyboardButton(text="📄 Шартнома тузилди", callback_data=f"lead_contract:{lead_id}")],
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


async def notify_agents_and_admins_about_lead(lead_id: str):
    lead = await db.get_lead(lead_id)
    if not lead:
        return

    recipients = await db.list_agents_and_admins(active_only=True)
    msg_ids = []
    text = build_lead_text(
        lead_id=lead_id,
        name=lead.get("client_name", ""),
        phone=lead.get("client_phone", ""),
        purpose=lead.get("purpose", ""),
        notes=lead.get("notes", ""),
        object_id=lead.get("object_id", ""),
        ref_by=lead.get("ref_by", ""),
    )

    for person in recipients:
        tg_id_raw = str(person.get("tg_id", "")).strip()
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
async def cmd_start(message: Message, command: CommandObject, state: FSMContext):
    ref_by = ""
    object_id = ""

    if command.args:
        if command.args.startswith("ref_"):
            ref_by = command.args.replace("ref_", "", 1)
        elif command.args.startswith("obj_"):
            object_id = command.args.replace("obj_", "", 1)

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

    if object_id:
        prop = await db.get_property(object_id)
        if not prop:
            return await message.answer("Ушбу объект топилмади ёки фаол эмас.")

        await state.set_state(ObjectInterestForm.phone)
        await state.update_data(object_id=object_id)
        await message.answer(
            f"🏠 Сиз <b>{object_id}</b> объектга қизиқиш билдирдингиз.\n\n"
            "Телефон рақамингизни юборинг:",
            reply_markup=phone_kb(),
        )
        return

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
            "object_id": "",
            "lead_type": "manual",
        }
    )

    await state.clear()
    await message.answer(
        f"✅ Заявкангиз қабул қилинди. ID: <b>{lead_id}</b>\nТез орада агент ёки админ сиз билан боғланади.",
        reply_markup=main_menu(user.get("role") if user else "client"),
    )

    await notify_agents_and_admins_about_lead(lead_id)
    await notify_admins(f"📥 Янги лид яратилди: <b>{lead_id}</b>")


@dp.message(ObjectInterestForm.phone, F.contact)
async def object_interest_phone_contact(message: Message, state: FSMContext):
    data = await state.get_data()
    object_id = data.get("object_id", "")
    phone = message.contact.phone_number

    await db.upsert_user(
        tg_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username or "",
        phone=phone,
        role="client",
        status="active",
    )

    lead_id = await db.create_lead(
        {
            "client_tg_id": message.from_user.id,
            "client_name": message.from_user.full_name,
            "client_phone": phone,
            "purpose": f"Объектга қизиқиш: {object_id}",
            "notes": f"Пост орқали қизиқди. Object ID: {object_id}",
            "ref_by": "",
            "object_id": object_id,
            "lead_type": "object_interest",
        }
    )

    await state.clear()
    await message.answer(
        f"✅ Сўров юборилди.\nОбъект: <b>{object_id}</b>\nЛид ID: <b>{lead_id}</b>\nТез орада агент ёки админ боғланади.",
        reply_markup=main_menu("client"),
    )
    await message.answer(
        "Қўшимча равишда, сизга шу объектга ўхшаш бошқа вариантлар ҳам таклиф қилинади. "
        "Менюдаги 🔎 Объект қидириш орқали яна вариантлар кўришингиз мумкин."
    )

    await notify_agents_and_admins_about_lead(lead_id)


@dp.message(ObjectInterestForm.phone)
async def object_interest_phone_text(message: Message, state: FSMContext):
    data = await state.get_data()
    object_id = data.get("object_id", "")
    phone = message.text.strip()

    await db.upsert_user(
        tg_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username or "",
        phone=phone,
        role="client",
        status="active",
    )

    lead_id = await db.create_lead(
        {
            "client_tg_id": message.from_user.id,
            "client_name": message.from_user.full_name,
            "client_phone": phone,
            "purpose": f"Объектга қизиқиш: {object_id}",
            "notes": f"Пост орқали қизиқди. Object ID: {object_id}",
            "ref_by": "",
            "object_id": object_id,
            "lead_type": "object_interest",
        }
    )

    await state.clear()
    await message.answer(
        f"✅ Сўров юборилди.\nОбъект: <b>{object_id}</b>\nЛид ID: <b>{lead_id}</b>\nТез орада агент ёки админ боғланади.",
        reply_markup=main_menu("client"),
    )
    await message.answer(
        "Қўшимча равишда, сизга шу объектга ўхшаш бошқа вариантлар ҳам таклиф қилинади. "
        "Менюдаги 🔎 Объект қидириш орқали яна вариантлар кўришингиз мумкин."
    )

    await notify_agents_and_admins_about_lead(lead_id)


@dp.callback_query(F.data.startswith("lead_take:"))
async def take_lead(call: CallbackQuery):
    lead_id = call.data.split(":", 1)[1]
    lead = await db.get_lead(lead_id)

    if not lead:
        return await call.answer("Лид топилмади", show_alert=True)

    if lead.get("status") in ("done", "contract_signed"):
        return await call.answer("Бу лид якунланган", show_alert=True)

    if lead.get("assigned_agent_id") and str(lead.get("assigned_agent_id")) != str(call.from_user.id):
        return await call.answer("Бу лидни бошқа ходим олган", show_alert=True)

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
            f"✅ Сизнинг сўровингиз <b>{call.from_user.full_name}</b> га бириктирилди. Яқин орада сиз билан боғланишади.",
        )


@dp.callback_query(F.data.startswith("lead_reject:"))
async def reject_lead(call: CallbackQuery):
    lead_id = call.data.split(":", 1)[1]
    lead = await db.get_lead(lead_id)

    if not lead:
        return await call.answer("Лид топилмади", show_alert=True)

    assigned_id = str(lead.get("assigned_agent_id") or "").strip()
    if assigned_id not in ("", str(call.from_user.id)):
        return await call.answer("Бу лид сизга тегишли эмас", show_alert=True)

    await db.update_lead(
        lead_id,
        status="new",
        assigned_agent_id="",
        assigned_agent_name="",
        assigned_message_ids="",
    )
    await call.answer("Лид қайта очилди ва барчага қайта юборилади")
    await notify_agents_and_admins_about_lead(lead_id)


@dp.callback_query(F.data.startswith("lead_done:"))
async def finish_lead(call: CallbackQuery):
    lead_id = call.data.split(":", 1)[1]
    lead = await db.get_lead(lead_id)

    if not lead:
        return await call.answer("Лид топилмади", show_alert=True)

    if str(lead.get("assigned_agent_id") or "").strip() != str(call.from_user.id):
        return await call.answer("Фақат лидни олган ходим якунлай олади", show_alert=True)

    await db.update_lead(lead_id, status="done", completed_at=now_str())
    await close_other_messages(lead)
    await call.answer("Лид якунланди")

    client_tg_id = str(lead.get("client_tg_id") or "").strip()
    if client_tg_id.isdigit():
        await safe_send(
            int(client_tg_id),
            "🏁 Сизнинг мурожаатингиз якунланди. Раҳмат. Қўшимча вариантлар керак бўлса бот орқали ёзиб қолдиринг.",
        )

    ref_by = str(lead.get("ref_by") or "").strip()
    if ref_by.isdigit():
        await safe_send(
            int(ref_by),
            "🎁 Сиз юборган мижознинг иши якунланди. Бонусингизни офисдан олиб кетишингиз мумкин.",
        )


@dp.callback_query(F.data.startswith("lead_contract:"))
async def sign_contract(call: CallbackQuery):
    lead_id = call.data.split(":", 1)[1]
    lead = await db.get_lead(lead_id)

    if not lead:
        return await call.answer("Лид топилмади", show_alert=True)

    if str(lead.get("assigned_agent_id") or "").strip() != str(call.from_user.id):
        return await call.answer("Фақат лидни олган ходим шартнома тугмасини босиши мумкин", show_alert=True)

    object_id = str(lead.get("object_id") or "").strip()
    if not object_id:
        return await call.answer("Бу лид объект билан боғланмаган", show_alert=True)

    await db.update_lead(
        lead_id,
        status="contract_signed",
        completed_at=now_str(),
        contract_signed_at=now_str(),
    )
    await db.update_property(
        object_id,
        Status="sold",
        post_status="archived",
    )
    await close_other_messages(lead)
    await call.answer("Шартнома тузилди, объект архивга ўтказилди")

    client_tg_id = str(lead.get("client_tg_id") or "").strip()
    if client_tg_id.isdigit():
        await safe_send(
            int(client_tg_id),
            f"📄 Табриклаймиз! <b>{object_id}</b> объект бўйича шартнома расмийлаштирилди.",
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
        return await message.answer(
            "Ҳеч нарса топилмади.\nБошқача ёзиб кўринг: манзил, туман, хона сони, ипотека, ID.",
            reply_markup=main_menu(role),
        )

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
        score = row.get("_score", "")

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
                f"📌 Статус: {status}\n"
                f"🤖 Мослик: {score}"
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