"""Test environment defaults.

`app/config.py` fails fast on a missing required variable, so the values must
exist before any test module imports it. conftest is imported by pytest first,
which makes this the single place that keeps the suite runnable regardless of
what the container's `.env` happens to contain. `setdefault` is used on
purpose: a real value already present in the environment still wins.
"""

import os

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("TWINKLER_SYSTEM_PROMPT", "Серверная система")
os.environ.setdefault("TWINKLER_CLIENT_HMAC_KEY", "test-hmac-key")

# The database is never contacted by the unit tests, but config requires these
# to be named rather than defaulted to "localhost/root/cep_public".
os.environ.setdefault("DB_HOST", "test-db-host")
os.environ.setdefault("DB_USER", "test-db-user")
os.environ.setdefault("DB_PASSWORD", "test-db-password")
os.environ.setdefault("DB_NAME", "cep_public_test")

# Model variables are required whenever GEMINI_API_KEY is set. Tests never
# call the provider, but they import modules that read these at import time
# (and use them as default arguments), so pin the production values here
# instead of letting the suite depend on whether a key is present.
os.environ.setdefault("GEMINI_MODEL", "gemini-3.5-flash-lite")
os.environ.setdefault("GEMINI_TRANSCRIPTION_MODEL", "gemini-3.5-flash-lite")
os.environ.setdefault("EMBEDDING_MODEL", "gemini-embedding-001")
os.environ.setdefault("EMBEDDING_DIMENSIONS", "768")
os.environ.setdefault("RETRIEVAL_REWRITE_MODEL", "gemini-3.7-flash")
os.environ.setdefault("RETRIEVAL_RERANK_MODEL", "gemini-3.5-flash-lite")
