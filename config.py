import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List


def _parse_admin_ids(raw: str) -> List[int]:
    ids: List[int] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if part.isdigit():
            ids.append(int(part))
    return ids


def _load_service_account_info() -> Dict[str, Any]:
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        return {}
    return json.loads(raw)


@dataclass
class Settings:
    bot_token: str
    spreadsheet_url: str
    service_account_info: Dict[str, Any]
    admins: List[int]
    company_name: str
    contact_phone: str


settings = Settings(
    bot_token=os.getenv("BOT_TOKEN", "").strip(),
    spreadsheet_url=os.getenv("SPREADSHEET_URL", "").strip(),
    service_account_info=_load_service_account_info(),
    admins=_parse_admin_ids(os.getenv("ADMIN_IDS", "")),
    company_name=os.getenv("COMPANY_NAME", "Golden Key"),
    contact_phone=os.getenv("CONTACT_PHONE", "+998 99 999 79 73"),
)


def validate_settings() -> None:
    missing = []
    if not settings.bot_token:
        missing.append("BOT_TOKEN")
    if not settings.spreadsheet_url:
        missing.append("SPREADSHEET_URL")
    if not settings.service_account_info:
        missing.append("GOOGLE_CREDENTIALS_JSON")
    if not settings.admins:
        missing.append("ADMIN_IDS")

    if missing:
        raise ValueError("Missing required environment variables: " + ", ".join(missing))
