@AGENTS.md

## Public site audio key

`BIBLE_GARDEN_SITE_API_KEY` is required in `.env`, must contain at least 32
characters and must differ from the other application keys. Compose reads it
through `env_file`; never reuse the released iOS key. It is intentionally
public in static page source and identifies `bible-garden-site` in statistics.
Only audio GET/HEAD accepts it (query `api_key` or `X-API-Key` header); all other
authenticated API routes return 403. Audio OPTIONS remains unauthenticated.

Browser playback uses `/api/audio/{translation}/{voice}/{book}/{chapter}.mp3?api_key=<site-key>`.
Audio responses allow any CORS origin and expose `Accept-Ranges`, `Content-Range`
and `Content-Length`. Single byte ranges, open-ended ranges and suffix ranges
return 206; unsatisfiable ranges return 416 with CORS headers.

Checked 2026-10-05 by reading `app/audio.py`, AI limiter call sites and
`Deploy/prod-nginx-default.conf`: MP3 routes have no rate limiter. Existing
limiters protect AI routes only. Recommend per-client request/connection limits
for `/api/audio/` in production nginx; the orchestrator decides the policy.

Deploy was read-only for task 123pfqn1yjg. Apply
`architect/deploy-env-checklist.patch` in the Deploy repository before release;
it adds the required variable without modifying Deploy from this worktree.
