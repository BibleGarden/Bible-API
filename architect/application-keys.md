# Application keys and request statistics

Bible-API accepts exactly three client identities: `bible-garden` uses
`BIBLE_GARDEN_API_KEY` (the released iOS key, unchanged), `lampada` uses the new
`LAMPADA_API_KEY`, and monitoring, operator checks and live evaluations use the
new `OPS_API_KEY`. All are required at startup; `API_KEY` is rejected. The two
new keys must each contain at least 32 characters. Key values are never logged
or stored with request statistics.

`X-API-Key` authenticates normal API requests. Audio GET/HEAD use a non-empty
`api_key` query parameter first, then the header. Invalid non-empty query keys
do not fall through to a valid header. Auth sets `request.state.application`;
request statistics write that identity, never a missing or inferred one.
Only requests that passed authentication are eligible for statistics; 403/404/405,
audio OPTIONS and `/api/health` remain excluded. FastAPI's trailing-slash 307
redirect happens before the auth dependency, so that redirect is not counted;
the redirected request is counted after successful authentication. Validation
errors can also happen before authentication and are not counted without a
resolved identity. If a successful response lacks identity, the middleware logs
an error and leaves the client response unchanged. The insert function rejects
missing or invalid identities, and tests assert the authenticated request is
recorded. No new `unknown` rows are written. There are no public API routes
eligible for request statistics.

Raw requests retain their 14-day limit. Daily endpoint and per-application
total rows are grouped by application; `application='all'` endpoint and daily
total rows count distinct clients across applications correctly. Historical rows are
`unknown` because their original key cannot be reconstructed. See the
cross-repository decision in `Architecture/decisions/0014-per-application-api-keys.md`.
