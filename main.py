import os
import json
import time
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify
import telebot
from telebot import types
import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# =========================================================
# ENV
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", "").strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()  # example: 12345,67890
CONTACT_PHONE = os.getenv("CONTACT_PHONE", "+998 99 999 79 73").strip()
BOT_NAME = os.getenv("BOT_NAME", "Golden Key Bot").strip()

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
# FLASK HEALTHCHECK
# =========================================================
web_app = Flask(__name__)

@web_app.route("/")
def home():
    return f"{BOT_NAME} is running", 200

@web_app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": BOT_NAME,
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
        ws = sh.add_worksheet(title=title, rows=1000, cols=max(20, len(headers) + 5))
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(headers)
    return ws

def init_sheets():
    leads_headers = [
        "lead_id", "created_at", "client_chat_id", "telegram_id", "username",
        "full_name", "phone", "direction", "comment", "status",
        "agent_id", "agent_name", "agent_username", "taken_at",
        "completed_at", "rejected_by", "source"
    ]
    agents_headers = [
        "agent_id", "full_name", "username", "phone", "is_active", "role"
    ]
    get_or_create_sheet("Leads", leads_headers)
    get_or_create_sheet("Agents", agents_headers)
    logger.info("Sheets initialized")

def leads_ws():
    return get_or_create_sheet("Leads", [
        "lead_id", "created_at", "client_chat_id", "telegram_id", "username",
        "full_name", "phone", "direction", "comment", "status",
        "agent_id", "agent_name", "agent_username", "taken_at",
        "completed_at", "rejected_by", "source"
    ])

def agents_ws():
    return get_or_create_sheet("Agents", [
        "agent_id", "full_name", "username", "phone", "is_active", "role"
    ])

# =========================================================
# MEMORY STATE
# =========================================================
user_state = {}

def ensure_user(uid):
    if uid not in user_state:
        user_state[uid] = {
            "step": None,
            "direction": "",
            "full_name": "",
            "phone": "",
            "comment": ""
        }

def reset_user(uid):
    user_state[uid] = {
        "step": None,
        "direction": "",
        "full_name": "",
        "phone": "",
        "comment": ""
    }

# =========================================================
# KEYBOARDS
# =========================================================
def main_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("🏠 Уй сотиш", "🏡 Уй олиш")
    kb.row("🔑 Ижарага бериш", "📄 Ижарага олиш")
    kb.row("💳 Ипотека", "☎️ Алоқа")
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
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Олдим", callback_data=f"take|{lead_id}"),
        types.InlineKeyboardButton("❌ Рад этилди", callback_data=f"reject|{lead_id}")
    )
    kb.row(
        types.InlineKeyboardButton("🏁 Бажарилди", callback_data=f"done|{lead_id}")
    )
    return kb

# =========================================================
# HELPERS
# =========================================================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def safe_username(user):
    return user.username if user and user.username else ""

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def new_lead_id():
    return f"LID-{int(time.time())}"

def find_col_index(headers, col_name):
    try:
        return headers.index(col_name) + 1
    except ValueError:
        return None

def get_all_records_safe(ws):
    try:
        return ws.get_all_records()
    except Exception as e:
        logger.exception(f"get_all_records error: {e}")
        return []

def find_lead_row_by_id(lead_id):
    ws = leads_ws()
    values = ws.get_all_values()
    if not values:
        return None, None
    headers = values[0]
    lead_col = headers.index("lead_id")
    for idx, row in enumerate(values[1:], start=2):
        if len(row) > lead_col and str(row[lead_col]).strip() == str(lead_id).strip():
            return idx, headers
    return None, headers

def update_lead_fields(lead_id, updates: dict):
    ws = leads_ws()
    row_num, headers = find_lead_row_by_id(lead_id)
    if not row_num:
        return False

    for key, value in updates.items():
        col = find_col_index(headers, key)
        if col:
            ws.update_cell(row_num, col, value)
    return True

def get_lead_by_id(lead_id):
    ws = leads_ws()
    records = get_all_records_safe(ws)
    for rec in records:
        if str(rec.get("lead_id", "")).strip() == str(lead_id).strip():
            return rec
    return None

def save_lead(data: dict):
    ws = leads_ws()
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
        "telegram_bot"
    ]
    ws.append_row(row)
    return lead_id

def get_active_agents():
    ws = agents_ws()
    records = get_all_records_safe(ws)
    result = []
    for r in records:
        agent_id = str(r.get("agent_id", "")).strip()
        is_active = str(r.get("is_active", "")).strip().lower()
        if agent_id.isdigit() and is_active in ("1", "true", "yes", "ha", "active"):
            result.append({
                "agent_id": int(agent_id),
                "full_name": str(r.get("full_name", "")).strip(),
                "username": str(r.get("username", "")).strip(),
                "phone": str(r.get("phone", "")).strip(),
                "role": str(r.get("role", "")).strip(),
            })
    return result

def notify_agents_about_lead(lead_id, payload):
    agents = get_active_agents()
    if not agents:
        logger.warning("Active agents topilmadi")
        return 0

    text = (
        "📥 <b>Янги лид</b>\n\n"
        f"🆔 <b>{lead_id}</b>\n"
        f"👤 Исм: {payload.get('full_name', '-')}\n"
        f"📞 Телефон: {payload.get('phone', '-')}\n"
        f"📌 Йўналиш: {payload.get('direction', '-')}\n"
        f"💬 Изоҳ: {payload.get('comment', '-')}\n"
        f"🆔 Telegram ID: {payload.get('telegram_id', '-')}\n"
        f"🔗 Username: @{payload.get('username', '-') if payload.get('username') else '-'}"
    )

    sent = 0
    for agent in agents:
        try:
            bot.send_message(
                agent["agent_id"],
                text,
                reply_markup=agent_inline_kb(lead_id)
            )
            sent += 1
        except Exception as e:
            logger.exception(f"Agentga yuborishda xatolik {agent['agent_id']}: {e}")
    return sent

def notify_admins(text):
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text)
        except Exception as e:
            logger.exception(f"Admin notify error {admin_id}: {e}")

def register_agent_if_admin(user):
    if not is_admin(user.id):
        return
    ws = agents_ws()
    records = get_all_records_safe(ws)

    for i, rec in enumerate(records, start=2):
        if str(rec.get("agent_id", "")).strip() == str(user.id):
            # update existing
            ws.update(f"A{i}:F{i}", [[
                str(user.id),
                f"{user.first_name or ''} {user.last_name or ''}".strip(),
                safe_username(user),
                "",
                "true",
                "admin"
            ]])
            return

    ws.append_row([
        str(user.id),
        f"{user.first_name or ''} {user.last_name or ''}".strip(),
        safe_username(user),
        "",
        "true",
        "admin"
    ])

# =========================================================
# STATS
# =========================================================
def compute_stats():
    records = get_all_records_safe(leads_ws())
    today = datetime.now().strftime("%Y-%m-%d")
    month = datetime.now().strftime("%Y-%m")

    daily_new = 0
    monthly_new = 0
    completed_by_agent = {}

    for r in records:
        created_at = str(r.get("created_at", ""))
        completed_at = str(r.get("completed_at", ""))
        status = str(r.get("status", "")).upper()
        agent_name = str(r.get("agent_name", "")).strip() or "Номаълум"

        if created_at.startswith(today):
            daily_new += 1
        if created_at.startswith(month):
            monthly_new += 1
        if status == "DONE":
            completed_by_agent[agent_name] = completed_by_agent.get(agent_name, 0) + 1

    top_agent_name = "-"
    top_agent_count = 0
    for k, v in completed_by_agent.items():
        if v > top_agent_count:
            top_agent_name = k
            top_agent_count = v

    total = len(records)
    new_count = sum(1 for r in records if str(r.get("status", "")).upper() == "NEW")
    taken_count = sum(1 for r in records if str(r.get("status", "")).upper() == "TAKEN")
    done_count = sum(1 for r in records if str(r.get("status", "")).upper() == "DONE")
    rejected_count = sum(1 for r in records if str(r.get("status", "")).upper() == "REJECTED")

    return {
        "total": total,
        "daily_new": daily_new,
        "monthly_new": monthly_new,
        "new_count": new_count,
        "taken_count": taken_count,
        "done_count": done_count,
        "rejected_count": rejected_count,
        "top_agent_name": top_agent_name,
        "top_agent_count": top_agent_count
    }

# =========================================================
# COMMANDS
# =========================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    ensure_user(message.from_user.id)
    reset_user(message.from_user.id)
    register_agent_if_admin(message.from_user)

    text = (
        f"Assalomu alaykum, <b>{message.from_user.first_name}</b>!\n\n"
        "Kerakli xizmatni tanlang:"
    )
    bot.send_message(message.chat.id, text, reply_markup=main_menu())

@bot.message_handler(commands=["menu"])
def cmd_menu(message):
    bot.send_message(message.chat.id, "Асосий меню:", reply_markup=main_menu())

@bot.message_handler(commands=["help"])
def cmd_help(message):
    bot.send_message(
        message.chat.id,
        "/start - бошлаш\n/menu - меню\n/admin - статистика",
        reply_markup=main_menu()
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
        f"❌ Рад этилган: <b>{st['rejected_count']}</b>\n\n"
        f"📅 Бугунги лидлар: <b>{st['daily_new']}</b>\n"
        f"🗓 Шу ой лидлари: <b>{st['monthly_new']}</b>\n\n"
        f"🥇 Энг яхши агент: <b>{st['top_agent_name']}</b>\n"
        f"🏆 Якунлаган лидлар: <b>{st['top_agent_count']}</b>"
    )
    bot.send_message(message.chat.id, text)

# =========================================================
# MENU FLOW
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
    user_state[uid]["direction"] = message.text
    user_state[uid]["step"] = "full_name"
    bot.send_message(message.chat.id, "Исмингизни киритинг:", reply_markup=back_keyboard())

@bot.message_handler(func=lambda m: m.text == "☎️ Алоқа")
def show_contact(message):
    bot.send_message(
        message.chat.id,
        f"☎️ Боғланиш учун:\n<b>{CONTACT_PHONE}</b>",
        reply_markup=main_menu()
    )

@bot.message_handler(func=lambda m: m.text == "⬅️ Орқага")
def go_back(message):
    reset_user(message.from_user.id)
    bot.send_message(message.chat.id, "Асосий менюга қайтдингиз.", reply_markup=main_menu())

# =========================================================
# CONTACT HANDLER
# =========================================================
@bot.message_handler(content_types=["contact"])
def handle_contact(message):
    uid = message.from_user.id
    ensure_user(uid)

    if user_state[uid]["step"] != "phone":
        return

    phone = message.contact.phone_number or ""
    user_state[uid]["phone"] = phone
    user_state[uid]["step"] = "comment"

    bot.send_message(
        message.chat.id,
        "Қисқача изоҳ ёзинг.\nМасалан: туман, бюджет, хона сони ва ҳ.к.",
        reply_markup=back_keyboard()
    )

# =========================================================
# TEXT HANDLER
# =========================================================
@bot.message_handler(content_types=["text"])
def handle_text(message):
    uid = message.from_user.id
    ensure_user(uid)

    text = (message.text or "").strip()

    if text in SERVICE_BUTTONS or text in ["☎️ Алоқа", "⬅️ Орқага"]:
        return

    step = user_state[uid]["step"]

    if step == "full_name":
        user_state[uid]["full_name"] = text
        user_state[uid]["step"] = "phone"
        bot.send_message(
            message.chat.id,
            "Телефон рақамингизни юборинг:",
            reply_markup=phone_keyboard()
        )
        return

    if step == "phone":
        user_state[uid]["phone"] = text
        user_state[uid]["step"] = "comment"
        bot.send_message(
            message.chat.id,
            "Қисқача изоҳ ёзинг.\nМасалан: туман, бюджет, хона сони ва ҳ.к.",
            reply_markup=back_keyboard()
        )
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

        lead_id = save_lead(payload)
        notify_count = notify_agents_about_lead(lead_id, payload)

        bot.send_message(
            message.chat.id,
            (
                "✅ Сўровингиз қабул қилинди!\n\n"
                f"🆔 ID: <b>{lead_id}</b>\n"
                f"👨‍💼 Хабар юборилган агентлар: <b>{notify_count}</b>\n"
                "Тез орада сиз билан боғланишади."
            ),
            reply_markup=main_menu()
        )

        notify_admins(
            f"📢 Янги лид тушди\n"
            f"🆔 {lead_id}\n"
            f"👤 {payload['full_name']}\n"
            f"📞 {payload['phone']}\n"
            f"📌 {payload['direction']}"
        )

        reset_user(uid)
        return

    bot.send_message(
        message.chat.id,
        "Керакли бўлимни менюдан танланг:",
        reply_markup=main_menu()
    )

# =========================================================
# CALLBACKS FOR AGENTS
# =========================================================
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    try:
        data = (call.data or "").split("|")
        action = data[0]
        lead_id = data[1] if len(data) > 1 else ""

        user = call.from_user
        user_id = user.id
        agent_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        agent_username = safe_username(user)

        lead = get_lead_by_id(lead_id)
        if not lead:
            bot.answer_callback_query(call.id, "Lead topilmadi")
            return

        current_status = str(lead.get("status", "")).upper()

        if action == "take":
            if current_status == "DONE":
                bot.answer_callback_query(call.id, "Бу лид аллақачон якунланган")
                return

            if current_status == "TAKEN":
                current_agent_id = str(lead.get("agent_id", "")).strip()
                if current_agent_id == str(user_id):
                    bot.answer_callback_query(call.id, "Сиз аллақачон олгансиз")
                else:
                    bot.answer_callback_query(call.id, "Бу лидни бошқа агент олган")
                return

            update_lead_fields(lead_id, {
                "status": "TAKEN",
                "agent_id": str(user_id),
                "agent_name": agent_name,
                "agent_username": agent_username,
                "taken_at": now_str(),
                "rejected_by": ""
            })

            try:
                bot.edit_message_reply_markup(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=agent_inline_kb(lead_id)
                )
            except Exception:
                pass

            bot.answer_callback_query(call.id, "Лид сизга бириктирилди ✅")

            try:
                client_chat_id = str(lead.get("client_chat_id", "")).strip()
                if client_chat_id.isdigit():
                    bot.send_message(
                        int(client_chat_id),
                        "👨‍💼 Менежер сизнинг сўровингизни олди. Тез орада боғланади."
                    )
            except Exception as e:
                logger.exception(f"Client notify on take error: {e}")

            return

        if action == "reject":
            rejected_by_old = str(lead.get("rejected_by", "")).strip()
            new_rejected_by = rejected_by_old + ("," if rejected_by_old else "") + str(user_id)

            if current_status == "DONE":
                bot.answer_callback_query(call.id, "Бу лид якунланган")
                return

            if current_status == "TAKEN":
                current_agent_id = str(lead.get("agent_id", "")).strip()
                if current_agent_id == str(user_id):
                    update_lead_fields(lead_id, {
                        "status": "NEW",
                        "agent_id": "",
                        "agent_name": "",
                        "agent_username": "",
                        "taken_at": "",
                        "rejected_by": new_rejected_by
                    })
                    bot.answer_callback_query(call.id, "Лиддан воз кечдингиз")
                else:
                    bot.answer_callback_query(call.id, "Бу лид бошқа агентда")
                return

            update_lead_fields(lead_id, {
                "rejected_by": new_rejected_by
            })
            bot.answer_callback_query(call.id, "Рад этилди")
            return

        if action == "done":
            if current_status != "TAKEN":
                bot.answer_callback_query(call.id, "Аввал лидни олиш керак")
                return

            current_agent_id = str(lead.get("agent_id", "")).strip()
            if current_agent_id != str(user_id):
                bot.answer_callback_query(call.id, "Бу лид сизга тегишли эмас")
                return

            update_lead_fields(lead_id, {
                "status": "DONE",
                "completed_at": now_str()
            })

            bot.answer_callback_query(call.id, "Лид якунланди 🏁")

            try:
                client_chat_id = str(lead.get("client_chat_id", "")).strip()
                if client_chat_id.isdigit():
                    bot.send_message(
                        int(client_chat_id),
                        "✅ Сизнинг мурожаатингиз бўйича иш якунланди. Рахмат!"
                    )
            except Exception as e:
                logger.exception(f"Client notify on done error: {e}")

            try:
                bot.edit_message_reply_markup(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=None
                )
            except Exception:
                pass

            return

        bot.answer_callback_query(call.id, "Номаълум амал")
    except Exception as e:
        logger.exception(f"callback_handler error: {e}")
        try:
            bot.answer_callback_query(call.id, "Хатолик юз берди")
        except Exception:
            pass

# =========================================================
# POLLING LOOP
# =========================================================
def run_bot():
    init_sheets()

    try:
        bot.remove_webhook()
        logger.info("Webhook removed")
    except Exception as e:
        logger.warning(f"remove_webhook error: {e}")

    while True:
        try:
            logger.info("Polling started...")
            bot.infinity_polling(
                timeout=60,
                long_polling_timeout=60,
                skip_pending=True
            )
        except Exception as e:
            logger.exception(f"Polling error: {e}")
            time.sleep(5)

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    run_bot()