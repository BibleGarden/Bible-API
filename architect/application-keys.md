# Application keys and request statistics

Bible-API accepts exactly three client identities: `bible-garden` uses
`BIBLE_GARDEN_API_KEY` (the released iOS key, unchanged), `lampada` uses the new
`LAMPADA_API_KEY`, and monitoring, operator checks and live evaluations use the
new `OPS_API_KEY`. All are required at startup; `API_KEY` is rejected. The two
new keys must each contain at least 32 characters. Key values are never logged
or stored with request statistics. Authentication hashes the request key and
each configured key with SHA-256, then compares all three fixed-length digests
with `hmac.compare_digest` before selecting an identity.

`X-API-Key` authenticates normal API requests. Audio GET/HEAD use a non-empty
`api_key` query parameter first, then the header. Invalid non-empty query keys
do not fall through to a valid header. Auth sets `request.state.application`;
request statistics write that identity, never a missing or inferred one.
`GET /api/import` and `POST /api/cache/clear` require the `ops` identity.
Bible Garden and Lampada keys receive the same 403 shape as an invalid key.
`GET /api/health` is a read-only readiness probe and accepts any valid client
key; it is excluded from statistics.
Only requests that passed authentication are eligible for statistics; 403/404/405,
audio OPTIONS and `/api/health` remain excluded. FastAPI's trailing-slash 307
redirect happens before the auth dependency, so that redirect is not counted;
the redirected request is counted after successful authentication. Validation
errors can also happen before authentication and are not counted without a
resolved identity. If a successful response lacks identity, the middleware logs
an error and leaves the client response unchanged. The middleware dispatches an
insert only with a resolved identity; tests assert authenticated requests are
recorded. No new `unknown` rows are written. There are no public API routes
eligible for request statistics.

Raw requests retain their 14-day limit. Daily endpoint and per-application
total rows are grouped by application; `application='all'` endpoint and daily
total rows count distinct clients across applications correctly. Historical rows
and requests from the old Bible-API during migration are `unknown` because their
original key cannot be reconstructed. The schema keeps `DEFAULT 'unknown'` for
that old writer; this writer supplies an explicit application. See the
cross-repository decision in `Architecture/decisions/0014-per-application-api-keys.md`.
