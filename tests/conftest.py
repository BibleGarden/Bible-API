"""Test environment defaults.

`app/config.py` fails fast on a missing required variable, so the values must
exist before any test module imports it. conftest is imported by pytest first,
which makes this the single hermetic AI configuration regardless of the
container's operational `.env`.
"""

import os

os.environ.setdefault("API_KEY", "test-api-key")
os.environ["AI_CLIENT_HMAC_KEY"] = "test-hmac-key"
os.environ["AI_ENABLED"] = "true"
# Operational container settings must not enable content-bearing diagnostics
# in the hermetic test configuration unless a test opts in explicitly.
os.environ["AI_QUESTION_LOG_PROVIDER_BODIES"] = "false"

# Removed shared credentials must not leak from the container's operational
# environment into the isolated unit-test configuration.
for removed_name in (
    "GEMINI_API_KEY",
    "AI_OPENAI_COMPAT_ENDPOINT",
    "AI_OPENAI_COMPAT_API_KEY",
):
    os.environ.pop(removed_name, None)

for unused_test_field in (
    "AI_QUESTION_ENDPOINT",
    "AI_SCRIPTURE_REWRITE_ENDPOINT",
    "AI_SCRIPTURE_RERANK_ENDPOINT",
    "AI_QUESTION_REASONING_EFFORT",
    "AI_SCRIPTURE_REWRITE_REASONING_EFFORT",
    "AI_SCRIPTURE_RERANK_REASONING_EFFORT",
    "AI_TRANSCRIBE_ENDPOINT",
    "AI_TRANSCRIBE_MODEL_PATH",
    "EMBEDDING_ENDPOINT",
    "EMBEDDING_MODEL_PATH",
    "AI_REASONING_EFFORT",
    "AI_TRANSCRIBE_REASONING_EFFORT",
    "EMBEDDING_REASONING_EFFORT",
):
    os.environ.pop(unused_test_field, None)

for provider_key in (
    "AI_QUESTION_API_KEY",
    "AI_SCRIPTURE_REWRITE_API_KEY",
    "AI_SCRIPTURE_RERANK_API_KEY",
    "AI_TRANSCRIBE_API_KEY",
    "EMBEDDING_API_KEY",
):
    if os.environ.get(provider_key) == "__BIBLE_API_UNSET__":
        os.environ.pop(provider_key)

# The database is never contacted by the unit tests, but config requires these
# to be named rather than defaulted to "localhost/root/cep_public".
os.environ.setdefault("DB_HOST", "test-db-host")
os.environ.setdefault("DB_USER", "test-db-user")
os.environ.setdefault("DB_PASSWORD", "test-db-password")
os.environ.setdefault("DB_NAME", "cep_public_test")

# Model variables are required whenever AI_ENABLED=true. Tests never call the
# real provider, but they import modules that read these at import time
# (and use them as default arguments), so pin the production values here
# instead of letting the suite depend on whether a key is present.
os.environ["AI_QUESTION_MODEL"] = "gemini-3.5-flash-lite"
os.environ["AI_TRANSCRIBE_MODEL"] = "gemini-3.5-flash-lite"
os.environ["EMBEDDING_MODEL"] = "gemini-embedding-001"
os.environ["EMBEDDING_DIMENSIONS"] = "768"
# Who computes the vectors (ADR 0010). Required in every environment, so the
# suite must name it. `gemini` for the same reason the models above are the
# production ones — it keeps every existing test on the client it was written
# against, and, more importantly, it keeps the suite from importing torch or
# loading 2.3 GB of bge-m3 weights: the local-client tests inject a stand-in
# model instead of ever touching the real one.
os.environ["EMBEDDING_PROVIDER"] = "gemini"
os.environ["AI_SCRIPTURE_REWRITE_MODEL"] = "gemini-3.7-flash"
os.environ["AI_SCRIPTURE_RERANK_MODEL"] = "gemini-3.5-flash-lite"

# Which transport serves each chat stage (ADR 0009/0019). Required when the
# AI surface is enabled, so the suite names all four explicitly.
# `gemini` is the right value here for the same reason the models above are
# the production ones: it keeps every existing test on the transport it was
# written against. The provider-switching tests build their own environments
# and never rely on these.
os.environ["AI_QUESTION_PROVIDER"] = "gemini"
os.environ["AI_SCRIPTURE_REWRITE_PROVIDER"] = "gemini"
os.environ["AI_SCRIPTURE_RERANK_PROVIDER"] = "gemini"
# Who transcribes (ADR 0012). `gemini` for the same reason as the three
# above, and for one more: it keeps the suite from importing faster-whisper
# or loading any Whisper weights — the transcription tests inject a stand-in
# model, and the one test that touches real weights is skipped unless
# AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST asks for it.
os.environ["AI_TRANSCRIBE_PROVIDER"] = "gemini"
os.environ["AI_QUESTION_API_KEY"] = "test-question-key"
os.environ["AI_SCRIPTURE_REWRITE_API_KEY"] = "test-rewrite-key"
os.environ["AI_SCRIPTURE_RERANK_API_KEY"] = "test-rerank-key"
os.environ["AI_TRANSCRIBE_API_KEY"] = "test-transcribe-key"
os.environ["EMBEDDING_API_KEY"] = "test-embedding-key"
