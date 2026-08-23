import os


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _require(name: str) -> str:
    value = os.getenv(name, "")
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = _get_int("DB_PORT", 3306)
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "cep_public")

# Path to MP3 files storage (inside container)
MP3_FILES_PATH = os.getenv("MP3_FILES_PATH", "audio")

# Base URL for audio files
AUDIO_BASE_URL = os.getenv("AUDIO_BASE_URL", "http://localhost:8000")

# API Authorization settings (required)
API_KEY = _require("API_KEY")

# Admin API connection settings (for import)
ADMIN_API_URL = os.getenv("ADMIN_API_URL", "http://admin-api:8000")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

# Gemini API for the Lampada prayer companion. Optional at startup so the
# rest of Bible API remains available when AI is not configured.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.7-flash")
GEMINI_TRANSCRIPTION_MODEL = os.getenv(
    "GEMINI_TRANSCRIPTION_MODEL",
    "gemini-3.5-flash-lite",
)
GEMINI_REQUESTS_PER_MINUTE = max(1, _get_int("GEMINI_REQUESTS_PER_MINUTE", 10))
GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE = min(
    GEMINI_REQUESTS_PER_MINUTE,
    max(1, _get_int("GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE", 3)),
)
LAMPADA_SYSTEM_PROMPT = os.getenv("LAMPADA_SYSTEM_PROMPT", "").strip()
LAMPADA_CLIENT_HMAC_KEY = os.getenv("LAMPADA_CLIENT_HMAC_KEY", "").strip()
TRUSTED_PROXY_IPS = frozenset(
    value.strip()
    for value in os.getenv("TRUSTED_PROXY_IPS", "").split(",")
    if value.strip()
)
