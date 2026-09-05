# version_check.py
from typing import Literal

from fastapi import APIRouter, Query
from auth import RequireAPIKey
from models import VersionCheckModel

MIN_SUPPORTED_VERSION = "1.1"
LATEST_VERSION = "1.1"
STORE_URL = "https://apps.apple.com/app/biblegarden/id123456789"

LAMPADA_MIN_SUPPORTED_VERSION = "1.0.0"
LAMPADA_LATEST_VERSION = "1.0.0"
# До публикации страницы App Store уведомления выключены.
LAMPADA_UPDATES_ENABLED = False
LAMPADA_STORE_URL = "https://apps.apple.com/app/id6806024678"

MESSAGES = {
    "soft": {
        "en": "A new version of Bible Garden is available. Please update for the best experience.",
        "ru": "Доступна новая версия Bible Garden. Пожалуйста, обновите приложение.",
        "uk": "Доступна нова версія Bible Garden. Будь ласка, оновіть додаток."
    },
    "hard": {
        "en": "This version of Bible Garden is no longer supported. Please update to continue using the app.",
        "ru": "Эта версия Bible Garden больше не поддерживается. Пожалуйста, обновите приложение для продолжения работы.",
        "uk": "Ця версія Bible Garden більше не підтримується. Будь ласка, оновіть додаток для продовження роботи."
    }
}


def parse_version(version: str) -> tuple[int, ...]:
    """Parses a semver string into a tuple of integers for comparison"""
    parts = tuple(int(x) for x in version.split("."))
    return parts + (0,) * (3 - len(parts))


router = APIRouter()


@router.get('/version-check', response_model=VersionCheckModel, operation_id="versionCheck", tags=["Version"])
def version_check(
    app_version: str = Query(..., pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$", max_length=32, description="Current app version, e.g. 1.2 or 1.2.0"),
    api_key: str = RequireAPIKey,
    app: Literal["bible-garden", "lampada"] = Query(default="bible-garden"),
):
    """Check whether the app version is up to date"""
    minimum = LAMPADA_MIN_SUPPORTED_VERSION if app == "lampada" else MIN_SUPPORTED_VERSION
    latest = LAMPADA_LATEST_VERSION if app == "lampada" else LATEST_VERSION
    store_url = LAMPADA_STORE_URL if app == "lampada" else STORE_URL
    v = parse_version(app_version)

    if not store_url or (app == "lampada" and not LAMPADA_UPDATES_ENABLED):
        update_type = "none"
    elif v < parse_version(minimum):
        update_type = "hard"
    elif v < parse_version(latest):
        update_type = "soft"
    else:
        update_type = "none"

    message = MESSAGES.get(update_type)
    if message and app == "lampada":
        message = {language: text.replace("Bible Garden", "Lampada") for language, text in message.items()}
    return {
        "app": app,
        "update_type": update_type,
        "latest_version": latest,
        "store_url": store_url,
        "message": message
    }
