import json
import os
from dataclasses import dataclass, field
from typing import List


def _split_csv(value: str) -> List[int]:
    if not value:
        return []
    return [int(x.strip()) for x in value.split(',') if x.strip()]


@dataclass
class Settings:
    bot_token: str = os.getenv("BOT_TOKEN", "")
    spreadsheet_url: str = os.getenv("SPREADSHEET_URL", "")
    admins: List[int] = field(default_factory=lambda: _split_csv(os.getenv("ADMIN_IDS", "")))
    public_base_url: str = os.getenv("PUBLIC_BASE_URL", "")
    health_port: int = int(os.getenv("PORT", "8080"))
    timezone: str = os.getenv("TIMEZONE", "Asia/Tashkent")
    company_name: str = os.getenv("COMPANY_NAME", "Golden Key Ipoteka")
    contact_phone: str = os.getenv("CONTACT_PHONE", "+998999997973")

    @property
    def service_account_info(self) -> dict:
        raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not raw:
            return {}
        return json.loads(raw)


settings = Settings()


def validate_settings() -> None:
    missing = []
    if not settings.bot_token:
        missing.append("BOT_TOKEN")
    if not settings.spreadsheet_url:
        missing.append("SPREADSHEET_URL")
    if not settings.service_account_info:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
