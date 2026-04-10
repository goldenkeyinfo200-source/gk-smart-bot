import logging
from datetime import datetime

import gspread
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from aiogram.enums import ParseMode
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# CONFIG
# =========================
BOT_TOKEN = "YOUR_BOT_TOKEN"
SPREADSHEET_URL = "YOUR_GOOGLE_SHEET_URL"

ADMIN_IDS = [123456789]  # админ ID

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# =========================
# GOOGLE SHEETS
# =========================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

sheet = client.open_by_url(SPREADSHEET_URL)
db_sheet = sheet.worksheet("Base")
agents_sheet = sheet.worksheet("Agents")

# =========================
# HELPER FUNCTIONS
# =========================

def generate_id():
    data = db_sheet.get_all_values()
    count = len(data)
    return f"GK-{str(count).zfill(3)}"


def get_agents():
    data = agents_sheet.get_all_values()
    return [row[0] for row in data[1:] if row]


def save_base(data):
    db_sheet.append_row(data)


# =========================
# MENUS
# =========================

def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📥 База қўшиш")],
        ],
        resize_keyboard=True
    )


# =========================
# STATES
# =========================
user_state = {}


# =========================
# START
# =========================

@dp.message(F.text == "/start")
async def start(msg: types.Message):
    await msg.answer("Хуш келибсиз!", reply_markup=main_menu())


# =========================
# ADD BASE FLOW
# =========================

@dp.message(F.text == "📥 База қўшиш")
async def add_base(msg: types.Message):
    user_state[msg.from_user.id] = {}
    await msg.answer("Манзилни киритинг:")


@dp.message()
async def handle_base(msg: types.Message):
    uid = msg.from_user.id

    if uid not in user_state:
        return

    state = user_state[uid]

    if "address" not in state:
        state["address"] = msg.text
        await msg.answer("Қават:")
        return

    if "floor" not in state:
        state["floor"] = msg.text
        await msg.answer("Хоналар сони:")
        return

    if "rooms" not in state:
        state["rooms"] = msg.text
        await msg.answer("Мулк шакли:")
        return

    if "type" not in state:
        state["type"] = msg.text
        await msg.answer("Майдони:")
        return

    if "area" not in state:
        state["area"] = msg.text
        await msg.answer("Мақсади (сотиш/ижара/ипотека):")
        return

    if "purpose" not in state:
        state["purpose"] = msg.text
        await msg.answer("Нархи:")
        return

    if "price" not in state:
        state["price"] = msg.text
        await msg.answer("Фото линк:")
        return

    if "photo" not in state:
        state["photo"] = msg.text
        await msg.answer("Мўлжал:")
        return

    if "target" not in state:
        state["target"] = msg.text
        await msg.answer("Ипотека (ха/йўқ):")
        return

    if "ipoteka" not in state:
        state["ipoteka"] = msg.text
        await msg.answer("Бош тўлов:")
        return

    if "initial" not in state:
        state["initial"] = msg.text

        # SAVE
        base_id = generate_id()

        row = [
            base_id,
            state["address"],
            state["floor"],
            state["rooms"],
            state["type"],
            state["area"],
            state["purpose"],
            state["price"],
            state["photo"],
            state["target"],
            state["ipoteka"],
            state["initial"],
            "pending",
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            msg.from_user.id
        ]

        save_base(row)

        await msg.answer(f"✅ База сақланди!\nID: {base_id}")

        # SEND TO AGENTS
        text = (
            f"📥 Янги база\n\n"
            f"ID: {base_id}\n"
            f"Манзил: {state['address']}\n"
            f"Нархи: {state['price']}"
        )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Олдим", callback_data=f"take_{base_id}"),
                InlineKeyboardButton(text="❌ Рад", callback_data=f"reject_{base_id}")
            ]
        ])

        for agent in get_agents():
            try:
                await bot.send_message(agent, text, reply_markup=kb)
            except:
                pass

        del user_state[uid]


# =========================
# CALLBACKS
# =========================

@dp.callback_query(F.data.startswith("take_"))
async def take_lead(call: types.CallbackQuery):
    base_id = call.data.split("_")[1]

    data = db_sheet.get_all_values()

    for i, row in enumerate(data):
        if row[0] == base_id:
            db_sheet.update_cell(i + 1, 13, "taken")
            db_sheet.update_cell(i + 1, 16, call.from_user.id)

    await call.message.edit_text(f"✅ Сиз олдингиз: {base_id}")


@dp.callback_query(F.data.startswith("reject_"))
async def reject_lead(call: types.CallbackQuery):
    await call.message.edit_text("❌ Рад этилди")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))