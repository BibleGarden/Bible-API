# ADR 0012: Independent application version policies

Status: Accepted

The shared version-check endpoint accepts `app=bible-garden|lampada`, defaulting
to Bible Garden. Each application has independent minimum/latest versions,
messages and store links. The response includes `app` so new clients can reject
responses from servers that ignore the selector. Existing response fields and
authentication remain compatible with released Bible Garden clients.

Versions contain one to three numeric components and normalize missing parts
to zero. Malformed values and unknown applications return 422.

Lampada starts at 1.0.0 with updates explicitly disabled until publication.
Its App Store URL is https://apps.apple.com/app/id6806024678. After that page is
public, set LAMPADA_UPDATES_ENABLED to True. Set LAMPADA_LATEST_VERSION to offer
optional updates, and LAMPADA_MIN_SUPPORTED_VERSION to require an update.
Always keep minimum <= latest. Policy changes require an API deployment.
