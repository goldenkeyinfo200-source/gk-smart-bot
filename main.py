import os
import json
import uuid
import logging
from io import StringIO
from datetime import datetime

import gspread
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").replace("@", "").strip()
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
ADMIN_IDS = [
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
]

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")
if not BOT_USERNAME:
    raise ValueError("BOT_USERNAME topilmadi")
if not SPREADSHEET_URL:
    raise ValueError("SPREADSHEET_URL topilmadi")
if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("GOOGLE_CREDENTIALS_JSON topilmadi")

# =========================
# BOT
# =========================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# =========================
# GOOGLE SHEETS
# =========================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gc = gspread.authorize(creds)
sh = gc.open_by_url(SPREADSHEET_URL)

ws_agents = sh.worksheet("Agents")
ws_special_agents = sh.worksheet("SpecialAgents")
ws_clients = sh.worksheet("Clients")
ws_leads = sh.worksheet("Leads")

# =========================
# FSM
# =========================
class ClientFlow(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_service = State()
    waiting_note = State()

class SpecialAgentFlow(StatesGroup):
    waiting_name = State()
    waiting_phone = State()

# =========================
# HELPERS
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"

def safe_str(v) -> str:
    return "" if v is None else str(v)

def safe_json_loads(value):
    if not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []

def normalize_phone(phone: str) -> str:
    phone = phone.strip().replace(" ", "").replace("-", "")
    if phone.startswith("+"):
        return phone
    if phone.startswith("998"):
        return f"+{phone}"
    return phone

def parse_start_ref(text: str):
    parts = text.strip().split()
    if len(parts) > 1 and parts[1].startswith("ref_"):
        return parts[1].replace("ref_", "", 1)
    return None

def build_ref_link(agent_code: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref_{agent_code}"

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Заявка қолдириш")],
            [KeyboardButton(text="🤝 Махсус агент бўлиш")],
            [KeyboardButton(text="☎️ Алоқа")],
        ],
        resize_keyboard=True
    )

def phone_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Рақамни юбориш", request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def services_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Уй сотиб олиш"), KeyboardButton(text="🏠 Уй сотиш")],
            [KeyboardButton(text="🔑 Ижарага олиш"), KeyboardButton(text="🔑 Ижарага бериш")],
            [KeyboardButton(text="💳 Ипотека"), KeyboardButton(text="📄 Кадастр")],
            [KeyboardButton(text="🔙 Бекор қилиш")]
        ],
        resize_keyboard=True
    )

def lead_kb(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Олдим", callback_data=f"lead:take:{lead_id}"),
                InlineKeyboardButton(text="❌ Рад этилди", callback_data=f"lead:reject:{lead_id}"),
                InlineKeyboardButton(text="🏁 Бажарилди", callback_data=f"lead:done:{lead_id}"),
            ]
        ]
    )

def lead_owner_kb(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="❌ Рад этилди", callback_data=f"lead:reject:{lead_id}"),
                InlineKeyboardButton(text="🏁 Бажарилди", callback_data=f"lead:done:{lead_id}"),
            ]
        ]
    )

# =========================
# SHEETS READ/WRITE
# =========================
def get_all_records(ws):
    return ws.get_all_records()

def append_row_safe(ws, row: list):
    ws.append_row(row, value_input_option="USER_ENTERED")

def find_row_by_value(ws, col_name: str, value: str):
    headers = ws.row_values(1)
    if col_name not in headers:
        return None
    col_index = headers.index(col_name) + 1
    col_values = ws.col_values(col_index)
    for i, v in enumerate(col_values[1:], start=2):
        if safe_str(v).strip() == safe_str(value).strip():
            return i
    return None

def row_to_dict(ws, row_number: int):
    headers = ws.row_values(1)
    row_values = ws.row_values(row_number)
    data = {}
    for i, h in enumerate(headers):
        data[h] = row_values[i] if i < len(row_values) else ""
    return data

def update_fields_by_row(ws, row_number: int, fields: dict):
    headers = ws.row_values(1)
    for key, value in fields.items():
        if key in headers:
            col = headers.index(key) + 1
            ws.update_cell(row_number, col, value)

def update_by_id(ws, id_col: str, id_value: str, fields: dict):
    row = find_row_by_value(ws, id_col, id_value)
    if not row:
        return False
    update_fields_by_row(ws, row, fields)
    return True

def get_by_id(ws, id_col: str, id_value: str):
    row = find_row_by_value(ws, id_col, id_value)
    if not row:
        return None
    return row_to_dict(ws, row)

# =========================
# AGENTS
# =========================
def get_active_agents():
    records = get_all_records(ws_agents)
    result = []
    for r in records:
        if safe_str(r.get("is_active")).lower() in ("1", "true", "yes", "ha"):
            tg = safe_str(r.get("telegram_id")).strip()
            if tg.isdigit():
                result.append({
                    "telegram_id": int(tg),
                    "full_name": safe_str(r.get("full_name")),
                    "phone": safe_str(r.get("phone")),
                    "agent_id": safe_str(r.get("agent_id")),
                })
    return result

def get_special_agent_by_code(agent_code: str):
    records = get_all_records(ws_special_agents)
    for r in records:
        if safe_str(r.get("agent_code")).strip() == safe_str(agent_code).strip():
            return r
    return None

def save_special_agent(full_name: str, phone: str, telegram_id: int, username: str):
    agent_code = uuid.uuid4().hex[:8].upper()
    ref_link = build_ref_link(agent_code)
    append_row_safe(ws_special_agents, [
        agent_code,
        full_name,
        phone,
        str(telegram_id),
        username,
        ref_link,
        now_str(),
        "TRUE",
    ])
    return {
        "agent_code": agent_code,
        "ref_link": ref_link,
        "full_name": full_name,
        "phone": phone,
        "telegram_id": str(telegram_id),
        "username": username,
    }

# =========================
# CLIENTS
# =========================
def get_client_by_telegram_id(telegram_id: int):
    records = get_all_records(ws_clients)
    for r in records:
        if safe_str(r.get("telegram_id")).strip() == str(telegram_id):
            return r
    return None

def create_or_update_client(full_name: str, phone: str, telegram_id: int, username: str, ref_agent=None):
    existing_row = find_row_by_value(ws_clients, "telegram_id", str(telegram_id))

    existing = get_client_by_telegram_id(telegram_id)
    if existing:
        client_id = safe_str(existing.get("client_id"))
        fields = {
            "full_name": full_name,
            "phone": phone,
            "username": username,
        }
        if ref_agent:
            fields["ref_agent_code"] = safe_str(ref_agent.get("agent_code"))
            fields["ref_agent_name"] = safe_str(ref_agent.get("full_name"))
            fields["ref_agent_telegram_id"] = safe_str(ref_agent.get("telegram_id"))
        update_fields_by_row(ws_clients, existing_row, fields)
        return client_id

    client_id = make_id("client")
    append_row_safe(ws_clients, [
        client_id,
        full_name,
        phone,
        str(telegram_id),
        username,
        safe_str(ref_agent.get("agent_code")) if ref_agent else "",
        safe_str(ref_agent.get("full_name")) if ref_agent else "",
        safe_str(ref_agent.get("telegram_id")) if ref_agent else "",
        now_str(),
    ])
    return client_id

# =========================
# LEADS
# =========================
def create_lead(
    client_id: str,
    client_name: str,
    client_phone: str,
    client_telegram_id: int,
    username: str,
    service_type: str,
    note: str,
    ref_agent=None,
):
    lead_id = make_id("lead")
    append_row_safe(ws_leads, [
        lead_id,
        client_id,
        client_name,
        client_phone,
        str(client_telegram_id),
        username,
        service_type,
        note,
        "new",
        "", "", "", "", "", "", "", "", "",
        safe_str(ref_agent.get("agent_code")) if ref_agent else "",
        safe_str(ref_agent.get("full_name")) if ref_agent else "",
        safe_str(ref_agent.get("telegram_id")) if ref_agent else "",
        now_str(),
        "[]",
    ])
    return lead_id

def get_lead(lead_id: str):
    return get_by_id(ws_leads, "lead_id", lead_id)

def update_lead(lead_id: str, fields: dict):
    return update_by_id(ws_leads, "lead_id", lead_id, fields)

# =========================
# NOTIFY AGENTS
# =========================
async def notify_agents_about_lead(lead_id: str):
    lead = get_lead(lead_id)
    if not lead:
        return

    agents = get_active_agents()
    if not agents:
        logger.warning("Faol agentlar topilmadi")
        return

    text = (
        f"🆕 <b>Янги лид</b>\n\n"
        f"🆔 ID: <code>{lead_id}</code>\n"
        f"👤 Мижоз: {safe_str(lead.get('client_name'))}\n"
        f"📞 Телефон: {safe_str(lead.get('client_phone'))}\n"
        f"🧾 Хизмат: {safe_str(lead.get('service_type'))}\n"
        f"📝 Изоҳ: {safe_str(lead.get('note')) or '-'}\n"
        f"🕒 Вақт: {safe_str(lead.get('created_at'))}\n"
    )

    if safe_str(lead.get("ref_agent_code")):
        text += (
            f"\n🤝 Махсус агент: {safe_str(lead.get('ref_agent_name'))}"
            f"\n🔖 Код: {safe_str(lead.get('ref_agent_code'))}\n"
        )

    refs = []
    for agent in agents:
        try:
            sent = await bot.send_message(
                chat_id=agent["telegram_id"],
                text=text,
                reply_markup=lead_kb(lead_id)
            )
            refs.append({
                "chat_id": agent["telegram_id"],
                "message_id": sent.message_id
            })
        except Exception as e:
            logger.exception(f"Agentga yuborishda xato: {e}")

    update_lead(lead_id, {
        "agent_message_refs": json.dumps(refs, ensure_ascii=False)
    })

# =========================
# COMMANDS
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    await state.clear()

    ref_code = parse_start_ref(message.text or "")
    if ref_code:
        special_agent = get_special_agent_by_code(ref_code)
        if special_agent:
            await state.update_data(ref_agent_code=ref_code)
            await message.answer(
                f"👋 Хуш келибсиз.\n\n"
                f"Сиз махсус агент орқали кирдингиз:\n"
                f"🤝 {safe_str(special_agent.get('full_name'))}\n\n"
                f"Энди маълумотларингизни киритинг.",
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await message.answer(
                "👋 Хуш келибсиз.\nРеферал код топилмади, оддий тартибда давом этамиз.",
                reply_markup=ReplyKeyboardRemove()
            )
    else:
        await message.answer(
            "👋 Хуш келибсиз.\nКеракли бўлимни танланг.",
            reply_markup=main_menu()
        )

@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Сизда рухсат йўқ.")
        return

    leads = get_all_records(ws_leads)
    total = len(leads)
    new_count = sum(1 for x in leads if safe_str(x.get("status")) == "new")
    progress_count = sum(1 for x in leads if safe_str(x.get("status")) == "in_progress")
    rejected_count = sum(1 for x in leads if safe_str(x.get("status")) == "rejected")
    done_count = sum(1 for x in leads if safe_str(x.get("status")) == "done")

    await message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"Жами лид: {total}\n"
        f"🆕 Янги: {new_count}\n"
        f"🔄 Жараёнда: {progress_count}\n"
        f"❌ Рад этилган: {rejected_count}\n"
        f"✅ Якунланган: {done_count}"
    )

# =========================
# MAIN MENU
# =========================
@dp.message(F.text == "☎️ Алоқа")
async def contact_cmd(message: Message):
    await message.answer(
        "☎️ Алоқа учун:\n+998 99 999 79 73",
        reply_markup=main_menu()
    )

@dp.message(F.text == "🤝 Махсус агент бўлиш")
async def special_agent_start(message: Message, state: FSMContext):
    await state.set_state(SpecialAgentFlow.waiting_name)
    await message.answer(
        "Махсус агент сифатида рўйхатдан ўтиш учун исмингизни киритинг:",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(SpecialAgentFlow.waiting_name)
async def special_agent_name(message: Message, state: FSMContext):
    full_name = message.text.strip()
    await state.update_data(full_name=full_name)
    await state.set_state(SpecialAgentFlow.waiting_phone)
    await message.answer("Телефон рақамингизни киритинг ёки юборинг:", reply_markup=phone_kb())

@dp.message(SpecialAgentFlow.waiting_phone, F.contact)
async def special_agent_phone_contact(message: Message, state: FSMContext):
    data = await state.get_data()
    full_name = safe_str(data.get("full_name"))
    phone = normalize_phone(message.contact.phone_number)

    saved = save_special_agent(
        full_name=full_name,
        phone=phone,
        telegram_id=message.from_user.id,
        username=safe_str(message.from_user.username),
    )
    await state.clear()

    await message.answer(
        f"✅ Сиз махсус агент сифатида рўйхатдан ўтдингиз.\n\n"
        f"👤 Исм: {saved['full_name']}\n"
        f"📞 Телефон: {saved['phone']}\n"
        f"🆔 Код: {saved['agent_code']}\n\n"
        f"🔗 Сизнинг махсус линкингиз:\n{saved['ref_link']}\n\n"
        f"Шу линк орқали кирган мижоз автоматик сизга боғланади.",
        reply_markup=main_menu()
    )

@dp.message(SpecialAgentFlow.waiting_phone)
async def special_agent_phone_text(message: Message, state: FSMContext):
    data = await state.get_data()
    full_name = safe_str(data.get("full_name"))
    phone = normalize_phone(message.text)

    saved = save_special_agent(
        full_name=full_name,
        phone=phone,
        telegram_id=message.from_user.id,
        username=safe_str(message.from_user.username),
    )
    await state.clear()

    await message.answer(
        f"✅ Сиз махсус агент сифатида рўйхатдан ўтдингиз.\n\n"
        f"👤 Исм: {saved['full_name']}\n"
        f"📞 Телефон: {saved['phone']}\n"
        f"🆔 Код: {saved['agent_code']}\n\n"
        f"🔗 Сизнинг махсус линкингиз:\n{saved['ref_link']}\n\n"
        f"Шу линк орқали кирган мижоз автоматик сизга боғланади.",
        reply_markup=main_menu()
    )

@dp.message(F.text == "📝 Заявка қолдириш")
async def client_start(message: Message, state: FSMContext):
    await state.set_state(ClientFlow.waiting_name)
    await message.answer(
        "Исмингизни киритинг:",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(ClientFlow.waiting_name)
async def client_name(message: Message, state: FSMContext):
    await state.update_data(full_name=message.text.strip())
    await state.set_state(ClientFlow.waiting_phone)
    await message.answer(
        "Телефон рақамингизни юборинг:",
        reply_markup=phone_kb()
    )

@dp.message(ClientFlow.waiting_phone, F.contact)
async def client_phone_contact(message: Message, state: FSMContext):
    await state.update_data(phone=normalize_phone(message.contact.phone_number))
    await state.set_state(ClientFlow.waiting_service)
    await message.answer(
        "Керакли хизмат турини танланг:",
        reply_markup=services_kb()
    )

@dp.message(ClientFlow.waiting_phone)
async def client_phone_text(message: Message, state: FSMContext):
    await state.update_data(phone=normalize_phone(message.text))
    await state.set_state(ClientFlow.waiting_service)
    await message.answer(
        "Керакли хизмат турини танланг:",
        reply_markup=services_kb()
    )

@dp.message(ClientFlow.waiting_service, F.text == "🔙 Бекор қилиш")
async def client_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Бекор қилинди.", reply_markup=main_menu())

@dp.message(ClientFlow.waiting_service)
async def client_service(message: Message, state: FSMContext):
    await state.update_data(service_type=message.text.strip())
    await state.set_state(ClientFlow.waiting_note)
    await message.answer(
        "Қўшимча изоҳ ёзинг. Агар изоҳ бўлмаса '-' юборинг:",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(ClientFlow.waiting_note)
async def client_note(message: Message, state: FSMContext):
    data = await state.get_data()

    full_name = safe_str(data.get("full_name"))
    phone = safe_str(data.get("phone"))
    service_type = safe_str(data.get("service_type"))
    note = safe_str(message.text).strip()
    username = safe_str(message.from_user.username)
    telegram_id = message.from_user.id

    ref_agent = None
    ref_code = data.get("ref_agent_code")
    if ref_code:
        ref_agent = get_special_agent_by_code(ref_code)

    client_id = create_or_update_client(
        full_name=full_name,
        phone=phone,
        telegram_id=telegram_id,
        username=username,
        ref_agent=ref_agent,
    )

    lead_id = create_lead(
        client_id=client_id,
        client_name=full_name,
        client_phone=phone,
        client_telegram_id=telegram_id,
        username=username,
        service_type=service_type,
        note=note,
        ref_agent=ref_agent,
    )

    await notify_agents_about_lead(lead_id)
    await state.clear()

    text = (
        f"✅ Заявкангиз қабул қилинди.\n\n"
        f"🆔 ID: <code>{lead_id}</code>\n"
        f"👤 Исм: {full_name}\n"
        f"📞 Телефон: {phone}\n"
        f"🧾 Хизмат: {service_type}\n"
        f"📝 Изоҳ: {note}\n"
    )
    if ref_agent:
        text += f"\n🤝 Сиз махсус агент орқали рўйхатдан ўтдингиз: {safe_str(ref_agent.get('full_name'))}\n"

    text += "\nТез орада сиз билан боғланишади."

    await message.answer(text, reply_markup=main_menu())

# =========================
# CALLBACKS
# =========================
@dp.callback_query(F.data.startswith("lead:take:"))
async def lead_take(callback: CallbackQuery):
    try:
        _, action, lead_id = callback.data.split(":")
        lead = get_lead(lead_id)

        if not lead:
            await callback.answer("Лид топилмади", show_alert=True)
            return

        status = safe_str(lead.get("status")).strip()
        agent_name = callback.from_user.full_name
        agent_id = str(callback.from_user.id)

        if status in ("in_progress", "done"):
            owner = safe_str(lead.get("taken_by_name")) or "бошқа агент"
            await callback.answer(f"Бу лид аллақачон {owner} га бириктирилган", show_alert=True)
            return

        update_lead(lead_id, {
            "status": "in_progress",
            "taken_by": agent_id,
            "taken_by_name": agent_name,
            "taken_at": now_str(),
        })

        base_text = callback.message.text or ""
        new_text = f"{base_text}\n\n✅ Бириктирилди: {agent_name}"

        try:
            await callback.message.edit_text(new_text, reply_markup=lead_owner_kb(lead_id))
        except Exception:
            pass

        refs = safe_json_loads(lead.get("agent_message_refs"))
        for ref in refs:
            try:
                chat_id = int(ref.get("chat_id"))
                message_id = int(ref.get("message_id"))

                if (
                    chat_id == callback.message.chat.id
                    and message_id == callback.message.message_id
                ):
                    continue

                other_text = (
                    f"{callback.message.text}\n\n"
                    f"⛔ Бу лид {agent_name} томонидан олинди."
                )
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=other_text,
                        reply_markup=None
                    )
                except Exception:
                    try:
                        await bot.edit_message_reply_markup(
                            chat_id=chat_id,
                            message_id=message_id,
                            reply_markup=None
                        )
                    except Exception:
                        pass
            except Exception:
                continue

        await callback.answer("Лид сизга бириктирилди")
    except Exception as e:
        logger.exception(e)
        await callback.answer("Хатолик юз берди", show_alert=True)

@dp.callback_query(F.data.startswith("lead:reject:"))
async def lead_reject(callback: CallbackQuery):
    try:
        _, action, lead_id = callback.data.split(":")
        lead = get_lead(lead_id)

        if not lead:
            await callback.answer("Лид топилмади", show_alert=True)
            return

        status = safe_str(lead.get("status")).strip()
        taken_by = safe_str(lead.get("taken_by")).strip()
        current_user = str(callback.from_user.id)

        if status != "in_progress":
            await callback.answer("Бу лид ҳозир жараёнда эмас", show_alert=True)
            return

        if taken_by != current_user:
            await callback.answer("Фақат лидни олган агент рад этиши мумкин", show_alert=True)
            return

        update_lead(lead_id, {
            "status": "rejected",
            "rejected_by": current_user,
            "rejected_by_name": callback.from_user.full_name,
            "rejected_at": now_str(),
        })

        text = f"{callback.message.text}\n\n❌ Рад этилди: {callback.from_user.full_name}"
        try:
            await callback.message.edit_text(text, reply_markup=None)
        except Exception:
            pass

        await callback.answer("Лид рад этилди")
    except Exception as e:
        logger.exception(e)
        await callback.answer("Хатолик юз берди", show_alert=True)

@dp.callback_query(F.data.startswith("lead:done:"))
async def lead_done(callback: CallbackQuery):
    try:
        _, action, lead_id = callback.data.split(":")
        lead = get_lead(lead_id)

        if not lead:
            await callback.answer("Лид топилмади", show_alert=True)
            return

        status = safe_str(lead.get("status")).strip()
        taken_by = safe_str(lead.get("taken_by")).strip()
        current_user = str(callback.from_user.id)

        if status != "in_progress":
            await callback.answer("Бу лидни якунлаб бўлмайди", show_alert=True)
            return

        if taken_by != current_user:
            await callback.answer("Фақат лидни олган агент бажарилди қилиши мумкин", show_alert=True)
            return

        update_lead(lead_id, {
            "status": "done",
            "done_by": current_user,
            "done_by_name": callback.from_user.full_name,
            "done_at": now_str(),
        })

        text = f"{callback.message.text}\n\n🏁 Бажарилди: {callback.from_user.full_name}"
        try:
            await callback.message.edit_text(text, reply_markup=None)
        except Exception:
            pass

        ref_tg = safe_str(lead.get("ref_agent_telegram_id")).strip()
        client_name = safe_str(lead.get("client_name")) or "Мижоз"

        if ref_tg.isdigit():
            try:
                await bot.send_message(
                    int(ref_tg),
                    f"🎉 Сиз юборган мижознинг иши якунланди.\n\n"
                    f"👤 Мижоз: {client_name}\n"
                    f"🆔 Lead ID: <code>{lead_id}</code>\n"
                    f"✅ Статус: Бажарилди\n\n"
                    f"Бонусингизни офисдан олиб кетишингиз мумкин."
                )
            except Exception as e:
                logger.exception(f"Special agentga yuborishda xato: {e}")

        client_tg = safe_str(lead.get("client_telegram_id")).strip()
        if client_tg.isdigit():
            try:
                await bot.send_message(
                    int(client_tg),
                    f"✅ Сизнинг мурожаатингиз бўйича иш якунланди.\n"
                    f"🆔 ID: <code>{lead_id}</code>\n\n"
                    f"Ташаккур."
                )
            except Exception:
                pass

        await callback.answer("Иш бажарилди деб белгиланди")
    except Exception as e:
        logger.exception(e)
        await callback.answer("Хатолик юз берди", show_alert=True)

# =========================
# FALLBACK
# =========================
@dp.message()
async def fallback_handler(message: Message):
    await message.answer(
        "Керакли бўлимни менюдан танланг.",
        reply_markup=main_menu()
    )

# =========================
# RUN
# =========================
async def main():
    logger.info("Bot ishga tushdi")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())