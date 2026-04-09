import os
import json
import time
import uuid
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify
import telebot
from telebot import types
import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# CONFIG
# =========================================================
APP_VERSION = "GK_CRM_v3.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "").strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
CONTACT_PHONE = os.getenv("CONTACT_PHONE", "+998 99 999 79 73").strip()
BOT_NAME = os.getenv("BOT_NAME", "Golden Key Smart Bot").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip()

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")
if not SPREADSHEET_URL:
    raise ValueError("SPREADSHEET_URL topilmadi")
if not GOOGLE_CREDS_JSON:
    raise ValueError("GOOGLE_CREDS_JSON topilmadi")

ADMIN_IDS = set()
if ADMIN_IDS_RAW:
    for x in ADMIN_IDS_RAW.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))

# =========================================================
# FLASK / HEALTHCHECK
# =========================================================
web_app = Flask(__name__)

@web_app.route("/")
def home():
    return f"{BOT_NAME} {APP_VERSION} running", 200

@web_app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": BOT_NAME,
        "version": APP_VERSION,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }), 200

def run_web():
    port = int(os.getenv("PORT", "8080"))
    logger.info(f"Web server started on port {port}")
    web_app.run(host="0.0.0.0", port=port)

# =========================================================
# TELEGRAM BOT
# =========================================================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# =========================================================
# GOOGLE SHEETS
# =========================================================
def get_gspread_client():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

def get_spreadsheet():
    client = get_gspread_client()
    return client.open_by_url(SPREADSHEET_URL)

def get_or_create_sheet(title, headers):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=4000, cols=max(35, len(headers) + 5))

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(headers)
    return ws

def leads_ws():
    return get_or_create_sheet("Leads", [
        "lead_id", "created_at", "client_chat_id", "telegram_id", "username",
        "full_name", "phone", "direction", "comment", "status",
        "agent_id", "agent_name", "agent_username", "agent_phone",
        "taken_at", "completed_at", "rejected_by", "source", "last_notified_at"
    ])

def agents_ws():
    return get_or_create_sheet("Agents", [
        "agent_id", "full_name", "username", "phone", "is_active", "role"
    ])

def special_leads_ws():
    return get_or_create_sheet("SpecialLeads", [
        "special_id", "created_at", "ref_agent_id", "special_agent_name",
        "special_agent_phone", "special_agent_telegram_id", "special_agent_username",
        "client_name", "client_phone", "comment", "status"
    ])

def lead_messages_ws():
    return get_or_create_sheet("LeadMessages", [
        "lead_id", "agent_id", "chat_id", "message_id", "status"
    ])

def init_sheets():
    leads_ws()
    agents_ws()
    special_leads_ws()
    lead_messages_ws()
    logger.info("Sheets initialized")

def get_all_records_safe(ws):
    try:
        return ws.get_all_records()
    except Exception as e:
        logger.exception(f"get_all_records error: {e}")
        return []

def find_row_by_value(ws, id_col_name, id_value):
    values = ws.get_all_values()
    if not values:
        return None, None

    headers = values[0]
    if id_col_name not in headers:
        return None, headers

    idx_col = headers.index(id_col_name)
    for row_num, row in enumerate(values[1:], start=2):
        val = row[idx_col].strip() if len(row) > idx_col else ""
        if str(val) == str(id_value):
            return row_num, headers
    return None, headers

def find_col_index(headers, col_name):
    try:
        return headers.index(col_name) + 1
    except ValueError:
        return None

def update_row_fields(ws, id_col_name, id_value, updates: dict):
    row_num, headers = find_row_by_value(ws, id_col_name, id_value)
    if not row_num:
        return False

    for key, value in updates.items():
        col = find_col_index(headers, key)
        if col:
            ws.update_cell(row_num, col, value)
    return True

def get_lead_by_id(lead_id):
    for r in get_all_records_safe(leads_ws()):
        if str(r.get("lead_id", "")).strip() == str(lead_id).strip():
            return r
    return None

# =========================================================
# HELPERS
# =========================================================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def new_lead_id():
    return f"LID-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

def new_special_id():
    return f"SP-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

def safe_username(user):
    return user.username if user and user.username else ""

def parse_bool_text(v):
    return str(v).strip().lower() in ("1", "true", "yes", "ha", "active")

def parse_rejected_by(text):
    raw = str(text or "").strip()
    if not raw:
        return set()
    return set(x.strip() for x in raw.split(",") if x.strip())

def rejected_by_to_text(values_set):
    return ",".join(sorted(values_set)) if values_set else ""

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def get_active_agents():
    rows = get_all_records_safe(agents_ws())
    result = []
    for r in rows:
        agent_id = str(r.get("agent_id", "")).strip()
        if not agent_id.isdigit():
            continue

        role = str(r.get("role", "")).strip().lower()
        if role not in ("agent", "admin"):
            continue

        if not parse_bool_text(r.get("is_active", "")):
            continue

        result.append({
            "agent_id": int(agent_id),
            "full_name": str(r.get("full_name", "")).strip(),
            "username": str(r.get("username", "")).strip(),
            "phone": str(r.get("phone", "")).strip(),
            "role": role
        })
    return result

def get_agent_info_by_id(agent_id):
    for a in get_active_agents():
        if int(a["agent_id"]) == int(agent_id):
            return a
    return None

def notify_admins(text):
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text)
        except Exception as e:
            logger.exception(f"Admin notify error {admin_id}: {e}")

def register_admin_in_agents(user):
    if not is_admin(user.id):
        return

    ws = agents_ws()
    row_num, _ = find_row_by_value(ws, "agent_id", str(user.id))
    row = [
        str(user.id),
        f"{user.first_name or ''} {user.last_name or ''}".strip(),
        safe_username(user),
        "",
        "true",
        "admin"
    ]

    if row_num:
        ws.update(f"A{row_num}:F{row_num}", [row])
    else:
        ws.append_row(row)

# =========================================================
# STORAGE
# =========================================================
def save_regular_lead(data: dict):
    lead_id = new_lead_id()
    row = [
        lead_id,
        now_str(),
        str(data.get("client_chat_id", "")),
        str(data.get("telegram_id", "")),
        str(data.get("username", "")),
        str(data.get("full_name", "")),
        str(data.get("phone", "")),
        str(data.get("direction", "")),
        str(data.get("comment", "")),
        "NEW",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "telegram_bot",
        now_str()
    ]
    leads_ws().append_row(row)
    return lead_id

def save_special_lead(data: dict):
    special_id = new_special_id()
    row = [
        special_id,
        now_str(),
        str(data.get("ref_agent_id", "")),
        str(data.get("special_agent_name", "")),
        str(data.get("special_agent_phone", "")),
        str(data.get("special_agent_telegram_id", "")),
        str(data.get("special_agent_username", "")),
        str(data.get("client_name", "")),
        str(data.get("client_phone", "")),
        str(data.get("comment", "")),
        "NEW"
    ]
    special_leads_ws().append_row(row)
    return special_id

def save_lead_message_log(lead_id, agent_id, chat_id, message_id, status="SENT"):
    lead_messages_ws().append_row([
        str(lead_id),
        str(agent_id),
        str(chat_id),
        str(message_id),
        str(status)
    ])

def get_lead_message_logs(lead_id):
    rows = get_all_records_safe(lead_messages_ws())
    result = []
    for r in rows:
        if str(r.get("lead_id", "")).strip() == str(lead_id).strip():
            result.append(r)
    return result

# =========================================================
# STATE
# =========================================================
user_state = {}

def default_user_state():
    return {
        "flow": None,

        "step": None,
        "direction": "",
        "full_name": "",
        "phone": "",
        "comment": "",

        "sa_step": None,
        "sa_name": "",
        "sa_phone": "",
        "sa_client_name": "",
        "sa_client_phone": "",
        "sa_comment": "",
        "ref_agent_id": ""
    }

def ensure_user(uid):
    if uid not in user_state:
        user_state[uid] = default_user_state()

def reset_all(uid):
    user_state[uid] = default_user_state()

# =========================================================
# UI
# =========================================================
def main_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("🏠 Уй сотиш", "🏡 Уй олиш")
    kb.row("🔑 Ижарага бериш", "📄 Ижарага олиш")
    kb.row("💳 Ипотека", "🌟 Махсус агент")
    kb.row("☎️ Алоқа")
    return kb

def phone_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(types.KeyboardButton("📱 Телефон юбориш", request_contact=True))
    kb.row("⬅️ Орқага")
    return kb

def back_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("⬅️ Орқага")
    return kb

def agent_inline_kb(lead_id):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("✅ Олдим", callback_data=f"take|{lead_id}"),
        types.InlineKeyboardButton("❌ Рад этилди", callback_data=f"reject|{lead_id}")
    )
    kb.add(
        types.InlineKeyboardButton("🏁 Бажарилди", callback_data=f"done|{lead_id}")
    )
    return kb

def owner_inline_kb(lead_id):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("❌ Рад этилди", callback_data=f"reject|{lead_id}"),
        types.InlineKeyboardButton("🏁 Бажарилди", callback_data=f"done|{lead_id}")
    )
    return kb

def lead_text(lead_id, payload, reassigned=False):
    title = "🔁 <b>Қайта юборилган лид</b>" if reassigned else "📥 <b>Янги лид</b>"
    return (
        f"{title}\n\n"
        f"🆔 <b>{lead_id}</b>\n"
        f"👤 Исм: {payload.get('full_name', '-')}\n"
        f"📞 Телефон: {payload.get('phone', '-')}\n"
        f"📌 Йўналиш: {payload.get('direction', '-')}\n"
        f"💬 Изоҳ: {payload.get('comment', '-')}\n"
        f"🆔 Telegram ID: {payload.get('telegram_id', '-')}\n"
        f"🔗 Username: @{payload.get('username') or '-'}"
    )

def notify_agents_about_lead(lead_id, payload, exclude_ids=None, reassigned=False):
    exclude_ids = set(str(x) for x in (exclude_ids or set()))
    sent = 0

    for agent in get_active_agents():
        if str(agent["agent_id"]) in exclude_ids:
            continue

        try:
            msg = bot.send_message(
                agent["agent_id"],
                lead_text(lead_id, payload, reassigned=reassigned),
                reply_markup=agent_inline_kb(lead_id)
            )
            save_lead_message_log(
                lead_id=lead_id,
                agent_id=agent["agent_id"],
                chat_id=agent["agent_id"],
                message_id=msg.message_id,
                status="SENT"
            )
            sent += 1
        except Exception as e:
            logger.exception(f"Agent notify error {agent['agent_id']}: {e}")

    return sent

def set_buttons_for_all_agents(lead_id, owner_agent_id=None, done=False):
    logs = get_lead_message_logs(lead_id)

    for row in logs:
        try:
            chat_id = int(str(row.get("chat_id", "")).strip())
            message_id = int(str(row.get("message_id", "")).strip())
            agent_id = str(row.get("agent_id", "")).strip()

            if done:
                reply_markup = None
            else:
                if owner_agent_id is not None and str(owner_agent_id) == agent_id:
                    reply_markup = owner_inline_kb(lead_id)
                else:
                    reply_markup = None

            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning(f"edit_message_reply_markup failed for {lead_id}: {e}")

# =========================================================
# STATS
# =========================================================
def compute_stats():
    records = get_all_records_safe(leads_ws())
    today = datetime.now().strftime("%Y-%m-%d")
    month = datetime.now().strftime("%Y-%m")

    total = len(records)
    new_count = 0
    taken_count = 0
    done_count = 0
    redirected_count = 0
    daily_new = 0
    monthly_new = 0
    top_agents = {}

    for r in records:
        created_at = str(r.get("created_at", ""))
        status = str(r.get("status", "")).strip().upper()
        agent_name = str(r.get("agent_name", "")).strip() or "Номаълум"

        if created_at.startswith(today):
            daily_new += 1
        if created_at.startswith(month):
            monthly_new += 1

        if status == "NEW":
            new_count += 1
        elif status == "TAKEN":
            taken_count += 1
        elif status == "DONE":
            done_count += 1
            top_agents[agent_name] = top_agents.get(agent_name, 0) + 1

        if str(r.get("rejected_by", "")).strip():
            redirected_count += 1

    top_agent_name = "-"
    top_agent_count = 0
    for k, v in top_agents.items():
        if v > top_agent_count:
            top_agent_name = k
            top_agent_count = v

    return {
        "total": total,
        "new_count": new_count,
        "taken_count": taken_count,
        "done_count": done_count,
        "redirected_count": redirected_count,
        "daily_new": daily_new,
        "monthly_new": monthly_new,
        "top_agent_name": top_agent_name,
        "top_agent_count": top_agent_count
    }

# =========================================================
# COMMANDS
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    reset_all(message.from_user.id)
    register_admin_in_agents(message.from_user)

    parts = message.text.split(maxsplit=1)
    ref_code = parts[1].strip() if len(parts) > 1 else ""

    if ref_code.startswith("ref_"):
        ref_agent_id = ref_code.replace("ref_", "").strip()

        ensure_user(message.from_user.id)
        user_state[message.from_user.id]["flow"] = "special"
        user_state[message.from_user.id]["sa_step"] = "sa_client_name"
        user_state[message.from_user.id]["ref_agent_id"] = ref_agent_id

        bot.send_message(
            message.chat.id,
            "Сиз махсус агент орқали кирдингиз.\n\nИсмингизни киритинг:",
            reply_markup=back_keyboard()
        )
        return

    bot.send_message(
        message.chat.id,
        f"Assalomu alaykum, <b>{message.from_user.first_name}</b>!\n\n"
        f"Версия: <b>{APP_VERSION}</b>\n"
        "Керакли хизматни танланг:",
        reply_markup=main_menu()
    )

@bot.message_handler(commands=["menu"])
def cmd_menu(message):
    bot.send_message(message.chat.id, "Асосий меню:", reply_markup=main_menu())

@bot.message_handler(commands=["help"])
def cmd_help(message):
    bot.send_message(
        message.chat.id,
        "/start - бошлаш\n/menu - меню\n/version - версия\n/myref - махсус агент линк\n/admin - статистика",
        reply_markup=main_menu()
    )

@bot.message_handler(commands=["version"])
def cmd_version(message):
    bot.send_message(message.chat.id, f"Ишлаяптган версия: <b>{APP_VERSION}</b>")

@bot.message_handler(commands=["myref"])
def cmd_myref(message):
    if not BOT_USERNAME:
        bot.send_message(message.chat.id, "BOT_USERNAME variable qo'yilmagan.")
        return

    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{message.from_user.id}"
    bot.send_message(
        message.chat.id,
        "Сизнинг махсус агент линкингиз:\n\n"
        f"{ref_link}\n\n"
        "Шу линк орқали кирган мижоз автоматик равишда сизга боғланади."
    )

@bot.message_handler(commands=["admin"])
def cmd_admin(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Сизда рухсат йўқ.")
        return

    st = compute_stats()
    text = (
        "📊 <b>ADMIN STATISTIKA</b>\n\n"
        f"📥 Жами лид: <b>{st['total']}</b>\n"
        f"🆕 Янги: <b>{st['new_count']}</b>\n"
        f"✅ Олинган: <b>{st['taken_count']}</b>\n"
        f"🏁 Якунланган: <b>{st['done_count']}</b>\n"
        f"🔁 Қайта йўналтирилган: <b>{st['redirected_count']}</b>\n\n"
        f"📅 Бугунги лидлар: <b>{st['daily_new']}</b>\n"
        f"🗓 Шу ой лидлари: <b>{st['monthly_new']}</b>\n\n"
        f"🥇 Энг яхши агент: <b>{st['top_agent_name']}</b>\n"
        f"🏆 Якунлаган лидлар: <b>{st['top_agent_count']}</b>\n\n"
        f"🧩 Версия: <b>{APP_VERSION}</b>"
    )
    bot.send_message(message.chat.id, text)

# =========================================================
# MENU
# =========================================================
SERVICE_BUTTONS = [
    "🏠 Уй сотиш",
    "🏡 Уй олиш",
    "🔑 Ижарага бериш",
    "📄 Ижарага олиш",
    "💳 Ипотека"
]

@bot.message_handler(func=lambda m: m.text in SERVICE_BUTTONS)
def choose_direction(message):
    uid = message.from_user.id
    ensure_user(uid)
    reset_all(uid)

    user_state[uid]["flow"] = "regular"
    user_state[uid]["direction"] = message.text
    user_state[uid]["step"] = "full_name"

    bot.send_message(message.chat.id, "Исмингизни киритинг:", reply_markup=back_keyboard())

@bot.message_handler(func=lambda m: m.text == "🌟 Махсус агент")
def choose_special_agent(message):
    uid = message.from_user.id
    ensure_user(uid)
    reset_all(uid)

    user_state[uid]["flow"] = "special"
    user_state[uid]["sa_step"] = "sa_name"

    bot.send_message(
        message.chat.id,
        "🌟 <b>Махсус агент</b>\n\nАввал исмингизни киритинг:",
        reply_markup=back_keyboard()
    )

@bot.message_handler(func=lambda m: m.text == "☎️ Алоқа")
def show_contact(message):
    bot.send_message(
        message.chat.id,
        f"☎️ Боғланиш учун:\n<b>{CONTACT_PHONE}</b>",
        reply_markup=main_menu()
    )

@bot.message_handler(func=lambda m: m.text == "⬅️ Орқага")
def go_back(message):
    reset_all(message.from_user.id)
    bot.send_message(message.chat.id, "Асосий менюга қайтдингиз.", reply_markup=main_menu())

# =========================================================
# CONTACTS
# =========================================================
@bot.message_handler(content_types=["contact"])
def handle_contact(message):
    uid = message.from_user.id
    ensure_user(uid)
    phone = message.contact.phone_number or ""

    if user_state[uid]["flow"] == "regular" and user_state[uid]["step"] == "phone":
        user_state[uid]["phone"] = phone
        user_state[uid]["step"] = "comment"
        bot.send_message(message.chat.id, "Қисқача изоҳ ёзинг:", reply_markup=back_keyboard())
        return

    if user_state[uid]["flow"] == "special":
        if user_state[uid]["sa_step"] == "sa_phone":
            user_state[uid]["sa_phone"] = phone
            user_state[uid]["sa_step"] = "sa_client_name"
            bot.send_message(message.chat.id, "Мижоз исмини киритинг:", reply_markup=back_keyboard())
            return

        if user_state[uid]["sa_step"] == "sa_client_phone":
            user_state[uid]["sa_client_phone"] = phone
            user_state[uid]["sa_step"] = "sa_comment"
            bot.send_message(message.chat.id, "Изоҳ ёзинг:", reply_markup=back_keyboard())
            return

# =========================================================
# CALLBACKS
# =========================================================
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    try:
        parts = (call.data or "").split("|")
        if len(parts) != 2:
            bot.answer_callback_query(call.id, "Хато callback")
            return

        action, lead_id = parts[0], parts[1].strip()
        user = call.from_user
        agent_id = str(user.id)
        agent_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        agent_username = safe_username(user)

        lead = get_lead_by_id(lead_id)
        if not lead:
            bot.answer_callback_query(call.id, "Лид топилмади", show_alert=True)
            return

        status = str(lead.get("status", "")).strip().upper()
        current_agent_id = str(lead.get("agent_id", "")).strip()
        client_chat_id = str(lead.get("client_chat_id", "")).strip()
        rejected_by = parse_rejected_by(lead.get("rejected_by", ""))

        payload = {
            "client_chat_id": lead.get("client_chat_id", ""),
            "telegram_id": lead.get("telegram_id", ""),
            "username": lead.get("username", ""),
            "full_name": lead.get("full_name", ""),
            "phone": lead.get("phone", ""),
            "direction": lead.get("direction", ""),
            "comment": lead.get("comment", ""),
        }

        if action == "take":
            if status == "DONE":
                bot.answer_callback_query(call.id, "Бу лид якунланган")
                return

            if status == "TAKEN":
                if current_agent_id == agent_id:
                    bot.answer_callback_query(call.id, "Сиз аллақачон олгансиз")
                else:
                    bot.answer_callback_query(call.id, "Бу лидни бошқа агент олган")
                return

            agent_info = get_agent_info_by_id(agent_id)
            agent_phone = agent_info["phone"] if agent_info else ""

            ok = update_row_fields(leads_ws(), "lead_id", lead_id, {
                "status": "TAKEN",
                "agent_id": agent_id,
                "agent_name": agent_name,
                "agent_username": agent_username,
                "agent_phone": agent_phone,
                "taken_at": now_str()
            })

            if not ok:
                bot.answer_callback_query(call.id, "Лидни янгилашда хато")
                return

            set_buttons_for_all_agents(lead_id, owner_agent_id=agent_id, done=False)

            if client_chat_id.isdigit():
                text = (
                    "👨‍💼 Сизнинг мурожаатингизни агент қабул қилди.\n\n"
                    f"👤 Агент: <b>{agent_name}</b>\n"
                )
                if agent_phone:
                    text += f"📞 Телефон: <b>{agent_phone}</b>\n"
                if agent_username:
                    text += f"🔗 Username: @{agent_username}\n"
                text += "\nТез орада сиз билан боғланади."

                try:
                    bot.send_message(int(client_chat_id), text)
                except Exception as e:
                    logger.exception(f"client take notify error: {e}")

            bot.answer_callback_query(call.id, "Лид сизга бириктирилди ✅")
            return

        if action == "reject":
            if status == "DONE":
                bot.answer_callback_query(call.id, "Бу лид якунланган")
                return

            if status != "TAKEN":
                bot.answer_callback_query(call.id, "Аввал лидни олиш керак")
                return

            if current_agent_id != agent_id:
                bot.answer_callback_query(call.id, "Бу лид сизга тегишли эмас")
                return

            rejected_by.add(agent_id)

            ok = update_row_fields(leads_ws(), "lead_id", lead_id, {
                "status": "NEW",
                "agent_id": "",
                "agent_name": "",
                "agent_username": "",
                "agent_phone": "",
                "taken_at": "",
                "rejected_by": rejected_by_to_text(rejected_by),
                "last_notified_at": now_str()
            })

            if not ok:
                bot.answer_callback_query(call.id, "Лидни янгилашда хато")
                return

            set_buttons_for_all_agents(lead_id, owner_agent_id=None, done=True)

            sent = notify_agents_about_lead(
                lead_id,
                payload,
                exclude_ids=rejected_by,
                reassigned=True
            )

            if client_chat_id.isdigit():
                try:
                    bot.send_message(
                        int(client_chat_id),
                        "ℹ️ Сизнинг мурожаатингиз бошқа агентга ўтказилди. Янги агент сиз билан боғланади."
                    )
                except Exception as e:
                    logger.exception(f"client reject notify error: {e}")

            if sent > 0:
                bot.answer_callback_query(call.id, "Лид бошқа агентларга юборилди")
            else:
                bot.answer_callback_query(call.id, "Бошқа актив агент топилмади")
                notify_admins(f"⚠️ {lead_id} учун бошқа актив агент топилмади")
            return

        if action == "done":
            if status != "TAKEN":
                bot.answer_callback_query(call.id, "Аввал лидни олиш керак")
                return

            if current_agent_id != agent_id:
                bot.answer_callback_query(call.id, "Бу лид сизга тегишли эмас")
                return

            ok = update_row_fields(leads_ws(), "lead_id", lead_id, {
                "status": "DONE",
                "completed_at": now_str()
            })

            if not ok:
                bot.answer_callback_query(call.id, "Лидни янгилашда хато")
                return

            set_buttons_for_all_agents(lead_id, owner_agent_id=None, done=True)

            if client_chat_id.isdigit():
                try:
                    bot.send_message(
                        int(client_chat_id),
                        "✅ Сизнинг мурожаатингиз бўйича иш якунланди. Рахмат!"
                    )
                except Exception as e:
                    logger.exception(f"client done notify error: {e}")

            bot.answer_callback_query(call.id, "Лид якунланди 🏁")
            return

        bot.answer_callback_query(call.id, "Номаълум амал")

    except Exception as e:
        logger.exception(f"callback_handler error: {e}")
        try:
            bot.answer_callback_query(call.id, "Хатолик юз берди")
        except Exception:
            pass

# =========================================================
# TEXT FLOW
# =========================================================
@bot.message_handler(content_types=["text"])
def handle_text(message):
    uid = message.from_user.id
    ensure_user(uid)
    text = (message.text or "").strip()

    if text in SERVICE_BUTTONS or text in ["🌟 Махсус агент", "☎️ Алоқа", "⬅️ Орқага"]:
        return

    flow = user_state[uid]["flow"]

    if flow == "regular":
        step = user_state[uid]["step"]

        if step == "full_name":
            user_state[uid]["full_name"] = text
            user_state[uid]["step"] = "phone"
            bot.send_message(message.chat.id, "Телефон рақамингизни юборинг:", reply_markup=phone_keyboard())
            return

        if step == "phone":
            user_state[uid]["phone"] = text
            user_state[uid]["step"] = "comment"
            bot.send_message(message.chat.id, "Қисқача изоҳ ёзинг:", reply_markup=back_keyboard())
            return

        if step == "comment":
            user_state[uid]["comment"] = text
            payload = {
                "client_chat_id": message.chat.id,
                "telegram_id": uid,
                "username": safe_username(message.from_user),
                "full_name": user_state[uid]["full_name"],
                "phone": user_state[uid]["phone"],
                "direction": user_state[uid]["direction"],
                "comment": user_state[uid]["comment"],
            }

            lead_id = save_regular_lead(payload)
            sent = notify_agents_about_lead(lead_id, payload)

            bot.send_message(
                message.chat.id,
                (
                    "✅ Сўровингиз қабул қилинди!\n\n"
                    f"🆔 ID: <b>{lead_id}</b>\n"
                    f"👨‍💼 Хабар юборилган агентлар: <b>{sent}</b>\n"
                    "Тез орада сиз билан боғланишади."
                ),
                reply_markup=main_menu()
            )

            notify_admins(
                f"📢 Янги лид тушди\n"
                f"🆔 {lead_id}\n"
                f"👤 {payload['full_name']}\n"
                f"📞 {payload['phone']}\n"
                f"📌 {payload['direction']}\n"
                f"🧩 Версия: {APP_VERSION}"
            )

            reset_all(uid)
            return

    if flow == "special":
        sa_step = user_state[uid]["sa_step"]

        if sa_step == "sa_name":
            user_state[uid]["sa_name"] = text
            user_state[uid]["sa_step"] = "sa_phone"
            bot.send_message(message.chat.id, "Телефон рақамингизни юборинг:", reply_markup=phone_keyboard())
            return

        if sa_step == "sa_phone":
            user_state[uid]["sa_phone"] = text
            user_state[uid]["sa_step"] = "sa_client_name"
            bot.send_message(message.chat.id, "Мижоз исмини киритинг:", reply_markup=back_keyboard())
            return

        if sa_step == "sa_client_name":
            user_state[uid]["sa_client_name"] = text
            user_state[uid]["sa_step"] = "sa_client_phone"
            bot.send_message(message.chat.id, "Мижоз телефон рақамини юборинг:", reply_markup=phone_keyboard())
            return

        if sa_step == "sa_client_phone":
            user_state[uid]["sa_client_phone"] = text
            user_state[uid]["sa_step"] = "sa_comment"
            bot.send_message(message.chat.id, "Изоҳ ёзинг:", reply_markup=back_keyboard())
            return

        if sa_step == "sa_comment":
            user_state[uid]["sa_comment"] = text

            payload = {
                "ref_agent_id": user_state[uid]["ref_agent_id"],
                "special_agent_name": user_state[uid]["sa_name"],
                "special_agent_phone": user_state[uid]["sa_phone"],
                "special_agent_telegram_id": uid,
                "special_agent_username": safe_username(message.from_user),
                "client_name": user_state[uid]["sa_client_name"],
                "client_phone": user_state[uid]["sa_client_phone"],
                "comment": user_state[uid]["sa_comment"]
            }

            sid = save_special_lead(payload)

            bot.send_message(
                message.chat.id,
                f"✅ Махсус агент заявкаси қабул қилинди!\n\n🆔 ID: <b>{sid}</b>",
                reply_markup=main_menu()
            )

            notify_admins(
                "🌟 МАХСУС АГЕНТ ЗАЯВКАСИ\n\n"
                f"🆔 ID: {sid}\n"
                f"🧷 Ref Agent ID: {payload['ref_agent_id']}\n"
                f"👤 Махсус агент: {payload['special_agent_name']}\n"
                f"📞 Унинг рақами: {payload['special_agent_phone']}\n"
                f"👤 Мижоз: {payload['client_name']}\n"
                f"📞 Мижоз рақами: {payload['client_phone']}\n"
                f"💬 Изоҳ: {payload['comment']}\n"
                f"🔗 Username: @{payload['special_agent_username'] or '-'}\n"
                f"🧩 Версия: {APP_VERSION}"
            )

            reset_all(uid)
            return

    bot.send_message(message.chat.id, "Керакли бўлимни менюдан танланг:", reply_markup=main_menu())

# =========================================================
# RUN
# =========================================================
def run_bot():
    init_sheets()

    try:
        bot.remove_webhook()
    except Exception as e:
        logger.warning(f"remove_webhook error: {e}")

    while True:
        try:
            logger.info(f"Polling started... version={APP_VERSION}")
            bot.infinity_polling(timeout=60, long_polling_timeout=60, skip_pending=True)
        except Exception as e:
            logger.exception(f"Polling error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    run_bot()