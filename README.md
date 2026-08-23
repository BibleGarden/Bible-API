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
GEMINI_REQUESTS_PER_MINUTE=10
GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE=3
LAMPADA_SYSTEM_PROMPT=Your server-controlled companion instructions
```

The Google key must never be included in the mobile application. Before a
production deployment, also set a hard quota for the key in Google AI Studio;
the server-side request limit is only an additional safeguard. The limiter
uses MySQL so its global and per-client counters are shared by all API workers
and replicas. The database user must be allowed to create and update the
`lampada_rate_limit_events` table.

## License

[GPLv3](LICENSE)
