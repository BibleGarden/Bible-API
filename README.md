# Bible API

Public read-only REST API for the [Bible Garden](https://github.com/Bible-Garden) app. Serves Bible texts, translations, and word-level audio alignments.

Built with FastAPI and MySQL.

## Setup

```bash
cp .env.example .env
# fill in DB credentials and API_KEY in .env

docker compose up -d --build
```

The API will be available at `http://localhost:8084/api`.

## Endpoints

- `GET /api/languages` — available languages
- `GET /api/translations` — available translations
- `GET /api/translations/{code}/books` — books in a translation
- `GET /api/excerpt_with_alignment` — text with word-level audio timing
- `GET /api/audio/{translation}/{voice}/{book}/{chapter}.mp3` — audio files
- `GET /api/about` — about page
- `GET /api/version-check` — app version check

All endpoints require `X-API-Key` header.

## License

[GPLv3](LICENSE)
