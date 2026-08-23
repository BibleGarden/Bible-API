# Bible API

Public read-only REST API for the [Bible Garden](https://github.com/Bible-Garden) app. Serves Bible texts, translations, and word-level audio alignments.

Built with FastAPI and MySQL.

## Setup

```bash
cp .env.example .env
# fill in DB credentials and API_KEY in .env

docker compose up -d --build
```

The API will be available at `http://localhost:9084/api`.

## Endpoints

- `GET /api/languages` — available languages
- `GET /api/translations` — available translations
- `GET /api/translations/{code}/books` — books in a translation
- `GET /api/excerpt_with_alignment` — text with word-level audio timing
- `GET /api/audio/{translation}/{voice}/{book}/{chapter}.mp3` — audio files
- `GET /api/about` — about page
- `GET /api/version-check` — app version check

All endpoints require `X-API-Key` header.

### Lampada AI

`POST /api/lampada/v1/complete` accepts `{ "user" }` and calls Gemini. The
system prompt and the Google AI Studio key are configured only on the server:

```dotenv
GEMINI_API_KEY=your-google-ai-studio-key
GEMINI_MODEL=gemini-3.7-flash
GEMINI_TRANSCRIPTION_MODEL=gemini-3.5-flash-lite
GEMINI_REQUESTS_PER_MINUTE=10
GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE=3
LAMPADA_SYSTEM_PROMPT=Your server-controlled companion instructions
LAMPADA_CLIENT_HMAC_KEY=generate-a-separate-random-secret
TRUSTED_PROXY_IPS=127.0.0.1
```

The Google key must never be included in the mobile application. Before a
production deployment, also set a hard quota for the key in Google AI Studio;
the server-side request limit is only an additional safeguard. The counters
are held in process memory, reset on restart, and are not shared across API
workers or replicas. Run a single worker or add an external distributed
limiter before scaling the service horizontally.

`TRUSTED_PROXY_IPS` must contain only direct reverse-proxy peers whose
`X-Forwarded-For` header is trusted. Leave it empty when the API is exposed
directly. `LAMPADA_CLIENT_HMAC_KEY` pseudonymizes client addresses and must be
different from both API keys.

`POST /api/lampada/v1/transcribe` accepts `multipart/form-data` with a required
M4A `file` and an optional BCP 47 `locale` (for example, `ru-RU`). The locale is
only a weak language hint: Gemini transcribes the recording verbatim in its
original language and does not translate it. Files larger than 14 MiB are
rejected. Recordings, filenames, and transcripts are not written to logs or
persistent storage.

## License

[GPLv3](LICENSE)
