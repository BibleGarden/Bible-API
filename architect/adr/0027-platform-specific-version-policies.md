# ADR 0027: Platform-specific Lampada version policies

Status: accepted (2026-10-10).
Ticket: none — delivered as a GitHub pull request; the client contract is
Lampada ADR-0039 (Google Play release). Extends ADR 0013.

## Context

Lampada is being released on Google Play in addition to the App Store. The
version check returned one App Store URL and one pair of thresholds for every
Lampada build, so an Android build would have offered an App Store link, and
the two stores could not be published or gated independently.

## Decision

`GET /api/version-check` accepts `platform=ios|android`. Any other value
returns 422. A request without `platform` is answered for `ios`: Lampada and
Bible Garden builds released before this change send no platform and are iOS
builds. The response carries `platform`, the platform the answer was decided
for; Lampada clients that send `platform` accept only a response with the
same value, so a server without this change shows them no update notices
instead of an App Store link on Android.

Lampada's four policy constants in `app/version_check.py` keep their names
and meaning and become per-platform maps: `LAMPADA_MIN_SUPPORTED_VERSION`,
`LAMPADA_LATEST_VERSION`, `LAMPADA_UPDATES_ENABLED` and `LAMPADA_STORE_URL`,
each keyed by `ios` and `android`. Store URLs: iOS
https://apps.apple.com/app/id6806024678, Android
https://play.google.com/store/apps/details?id=app.lampada.

Activation, per platform `<p>`: after that store listing is public, set
`LAMPADA_UPDATES_ENABLED[<p>]` to `True`. Set `LAMPADA_LATEST_VERSION[<p>]` to
offer optional updates and `LAMPADA_MIN_SUPPORTED_VERSION[<p>]` to require an
update. Always keep minimum <= latest for each platform.

Bible Garden is released for iOS only: `app=bible-garden&platform=android`
returns 422 rather than an App Store answer labelled `android`, with the same
validation-error body as any other invalid query parameter
(`detail[0].loc == ["query", "platform"]`).

The constants remain code, not environment variables, for the reason given in
ADR 0013: forcing an update is a release decision for a reviewed commit.

## Consequences

- Deploy this change before the first Lampada Android release; until then
  Android builds show no update notices.
- iOS and Android versions are compared against their own thresholds, so a
  release published on one store first does not prompt users of the other.
- A global Lampada switch is gone: turning notices on is a per-store decision.
