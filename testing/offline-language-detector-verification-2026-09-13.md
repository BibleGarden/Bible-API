# Offline language detector verification — 86cbehk24

Date: 2026-09-13. All runtime checks used an isolated container with
`--network none`, a read-only source mount and placeholder test configuration.
The running `bible-api` container and production were not touched.

## Reproducible selection evidence

The detector comparison, frozen corpora, raw routing rows and report are in
`BibleGarden/AI-Evaluation@ec757fe`. The selected configuration is
`py3langid==0.4.0`, all 139 bundled languages, normalized probability at least
`0.9`, with no word overrides or alphabet fallback. Its routing result was:

- tier 1: 57/57 fixed replies in the expected language;
- tier 2: 25/25 with the approved pattern-language evidence on abstention;
- question scenarios and probes: 26 localized in their declared language,
  eight honestly unresolved through the universal prompt, three empty inputs
  through the documented English default, and zero localized disagreements;
- long Spanish, Polish and Portuguese inputs detected as `es`, `pl` and `pt`
  and routed through the existing universal prompt.

## Application request coverage

The FastAPI `TestClient` sends real `POST /api/ai/question` requests through
the application router while the provider seam is an in-process stub. Tests
cover:

- an ambiguous latest reply continuing through `language_source` to confident
  topic or earlier-person context;
- detected `es`/`pl`/`pt` selecting the universal prompt;
- a tier-1 abstention taking the matched pattern language;
- a tier-2 abstention taking the matched pattern language;
- known English and known unsupported Spanish conversation context retaining
  precedence over the tier-2 pattern;
- both provider transports receiving byte-identical localized or universal
  system prompts.

The detector tests additionally pin the inclusive `0.9` boundary, wordless
`None`, `zxx`/`und` handling, loud failure on invalid dependency output and
model initialization, singleton reuse and deterministic concurrent calls.

## Offline test commands

The dependency-only test image was derived from the existing local production
image without replacing or restarting any container:

```bash
docker build -t bible-api-86cbehk24-deps -f - . <<'EOF'
FROM bible-api-bible-api
RUN pip install --no-cache-dir py3langid==0.4.0
EOF
```

Exit `0`. The focused suite:

```bash
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v <worktree>:/src:ro -w /src bible-api-86cbehk24-deps \
  pytest -q -p no:cacheprovider \
  tests/test_language_detection.py tests/test_safety.py tests/test_twinkler_ai.py
```

Exit `0`: **420 passed**.

The final full suite used a 256 MiB temporary filesystem because py3langid
streams its bundled model through a temporary decompressed NPZ at startup:

```bash
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=256m \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v <worktree>:/src:ro -w /src bible-api-86cbehk24-deps \
  pytest -q -p no:cacheprovider
```

Exit `0`: **1,638 passed, 44 skipped**. The skipped tests are the repository's
existing opt-in real model checks. No provider or database connection was used.

## Footprint

Docker image sizes from `docker image inspect`:

| image | bytes |
| --- | ---: |
| existing `bible-api-bible-api` | 2,765,776,884 |
| dependency test image | 2,770,419,236 |
| increase | **4,642,352** |

The installed compressed model is 4,586,720 bytes. Loading expands it into a
68,312,620-byte temporary NPZ, which is deleted after initialization.

Application imports were measured in otherwise identical network-isolated
containers after loading `app/main.py` with test configuration:

| application import | VmRSS |
| --- | ---: |
| baseline | 78,824 KiB |
| with detector | 153,740 KiB |
| increase | **74,916 KiB** |

The selection benchmark measured detector initialization at approximately
384 ms and hot classification at 0.016 ms median / 0.030 ms p95. These are
single-process measurements; production loads one model per API process.
