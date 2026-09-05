# ADR 0011: Application-specific About content

Status: Accepted

## Context

Bible Garden and Lampada share this API and its API key. The existing About
endpoint assumes a single application and returns the Bible Garden website.

## Decision

Add an optional `app` query parameter to `GET /api/about`, accepting
`bible-garden` (the default) and `lampada`. Omitted parameters preserve the
existing response for released Bible Garden clients. Unknown values return 422.
The response schema and API-key authentication remain unchanged.

Lampada shares Telegram and GitHub contacts, but has its own website URL,
localized website subtitles and project description. Build its response from a
deep copy so one application's request cannot alter another application's data.

## Consequences

Clients explicitly select their public content without requiring separate keys.
The selector is not an authorization boundary. Deploy the API before distributing
the updated Lampada client: older servers ignore the selector and still return
Bible Garden content. Lampada currently consumes contacts only and keeps its
screen description local.
