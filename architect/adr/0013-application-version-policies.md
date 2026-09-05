# ADR 0013: Independent application version policies

Status: accepted (2026-09-05).
Ticket: ClickUp 86cbbt978 — delivered as GitHub PR #4; the Lampada client
change lives in the Lampada workspace. Follows ADR 0011, which introduced the
same `app` selector on `GET /api/about`. (Numbered 0012 in the pull request;
renumbered on merge because 0012 was taken by the transcription ADR.)

## Decision

The shared version-check endpoint accepts `app=bible-garden|lampada`, defaulting
to Bible Garden. Each application has independent minimum/latest versions,
messages and store links. The response includes `app` so new clients can reject
responses from servers that ignore the selector. Existing response fields and
authentication remain compatible with released Bible Garden clients.

Versions contain one to three numeric components and normalize missing parts
to zero. Malformed values and unknown applications return 422.

Lampada starts at 1.0.0 with updates explicitly disabled until publication.
Its App Store URL is https://apps.apple.com/app/id6806024678. After that page is
public, set `LAMPADA_UPDATES_ENABLED` to `True`. Set `LAMPADA_LATEST_VERSION` to
offer optional updates, and `LAMPADA_MIN_SUPPORTED_VERSION` to require an
update. Always keep minimum <= latest. Policy changes require an API deployment.

**These four names are code constants in `app/version_check.py`, not
environment variables** — the same shape as the Bible Garden trio beside them
(`MIN_SUPPORTED_VERSION`, `LATEST_VERSION`, `STORE_URL`). Forcing every user of
a published app to update is a release decision that belongs in a reviewed
commit, not in a deployment's `.env`; ADR 0008's rule about silent defaults
does not apply, because there is no configuration here to be missing.
