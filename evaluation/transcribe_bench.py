#!/usr/bin/env python3
"""Speech-to-text benchmark: local faster-whisper vs Gemini (ClickUp 86cbegg3m).

The harness builds a small reference set out of the audio corpus and the
`cep_public` database (read-only), then measures the candidates on it:

    samples   cut the excerpts and write transcribe_samples.json
    local     run one faster-whisper configuration over the samples
    remote    run an OpenAI-compatible AUDIO server over the samples
    gemini    run the production Gemini transcription over the samples
    report    print the aggregate tables and the per-sample transcripts

Every excerpt is a run of WHOLE verses cut on the `voice_alignments`
boundaries, so the reference transcript is exactly the concatenation of those
verses' `translation_verses.text`. Excerpts are encoded as mono AAC in m4a,
which is what the mobile app uploads (`audio/mp4`), so the decode path being
measured is the production one.

Run each `local` configuration in its own process: the peak RSS reported per
run is `ru_maxrss` of the whole process, which would otherwise carry the peak
of a previously loaded model.

    /root/whisper-bench-venv/bin/python evaluation/transcribe_bench.py samples
    /root/whisper-bench-venv/bin/python evaluation/transcribe_bench.py local \
        --model small --beam 1
    /root/whisper-bench-venv/bin/python evaluation/transcribe_bench.py gemini
    /root/whisper-bench-venv/bin/python evaluation/transcribe_bench.py report

`remote` is the one that re-measures the production provider when the admins
change the model behind it. It does not re-implement the request: it drives
`app/transcription.RemoteTranscriber`, the very client the service uses, so
the bytes on the wire are the deployed ones by construction (down to the
multipart filename and the `language` hint derived from the sample's LOCALE,
the way the app derives it). It reads the key from the environment and never
prints it, never writes it to the artifact and never puts it in the URL:

    export AI_TRANSCRIBE_API_KEY="$(...)"   # never into a file
    python evaluation/transcribe_bench.py remote \
        --endpoint https://llm.ai2.ru/whisper/v1 \
        --model deepdml/faster-whisper-large-v3-turbo-ct2
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import resource
import statistics
import subprocess
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent / "bench_data"
AUDIO_OUT = BENCH_DIR / "transcribe_audio"
SAMPLES_PATH = BENCH_DIR / "transcribe_samples.json"
ENV_PATH = Path("/root/cep/Bible-API/.env")
MODELS_DIR = Path("/root/models/whisper")

# The transcription prompt of app/twinkler_ai.py, copied verbatim so the
# Gemini leg of the benchmark sends the same bytes production sends.
_TRANSCRIPTION_PROMPT = (
    "Transcribe the speech verbatim in its original language. Preserve "
    "code-switching. Do not translate, summarize, add, omit, explain, or "
    "rewrite anything. Add only natural punctuation. Return only the transcript."
)
LOCALES = {"ru": "ru-RU", "uk": "uk-UA", "en": "en-US"}

# Hand-picked starting points: three languages x five genres, all from
# different books. Only the first chapters of most epistles exist in the
# corpus, which is why the epistle picks are Romans 5 / 1 Corinthians 2 /
# Ephesians 1 rather than the usual Romans 8 / 1 Corinthians 13. The `target`
# is the minimum length the verse run is extended to, spread over 12-50 s so
# the time ratio is measured on short and long recordings alike (a voice
# message in the app is anywhere in that range).
# Book numbers follow the Slavonic order of the New Testament (45 James,
# 46 1 Peter, 49 2 John, 59 1 Thessalonians), not the Protestant one.
# `strip` removes a leading fragment of the verse text that the narrator does
# not read inside the excerpt: BSB keeps the section heading inside
# `translation_verses.text` (the matching `translation_titles` row exists but
# its `text` is empty, so it cannot be cut automatically), and the UBH psalm
# superscription is spoken by this narrator as part of the chapter
# announcement, before the first verse's alignment begins.
# (language, translation, voice, book, chapter, first verse, genre, target, strip)
PICKS = [
    ("ru", "syn", "prudovsky", 1, 1, 1, "narrative", 12, ""),
    ("ru", "syn", "prudovsky", 19, 22, 1, "psalm", 45, ""),
    ("ru", "syn", "prudovsky", 40, 5, 3, "gospel", 25, ""),
    ("ru", "syn", "prudovsky", 45, 5, 1, "epistle", 35, ""),
    ("ru", "syn", "prudovsky", 23, 40, 28, "prophecy", 20, ""),
    ("uk", "ubh", "kozlov_uk", 2, 20, 1, "law", 30, ""),
    ("uk", "ubh", "kozlov_uk", 19, 23, 1, "psalm", 50, "Псалом. Давида."),
    ("uk", "ubh", "kozlov_uk", 43, 3, 16, "gospel", 12, ""),
    ("uk", "ubh", "kozlov_uk", 46, 2, 9, "epistle", 25, ""),
    ("uk", "ubh", "kozlov_uk", 20, 3, 5, "wisdom", 35, ""),
    ("en", "bsb", "bsb_david", 8, 1, 16, "narrative", 20, ""),
    ("en", "bsb", "bsb_david", 19, 23, 1, "psalm", 40, "The Lord Is My Shepherd"),
    ("en", "bsb", "bsb_david", 42, 2, 8, "gospel", 30, ""),
    ("en", "bsb", "bsb_david", 59, 1, 2, "epistle", 45, ""),
    ("en", "bsb", "bsb_david", 49, 1, 3, "epistle", 12, ""),
]
MAX_SECONDS = 60.0


# --------------------------------------------------------------------------
# environment / database
# --------------------------------------------------------------------------
def read_env() -> dict[str, str]:
    """Read the production .env of Bible-API (never written to)."""
    env: dict[str, str] = {}
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def query(sql: str) -> list[dict]:
    """Run one read-only SELECT against cep_public and return its JSON rows.

    No MySQL driver is installed in the benchmark venv, so the query goes
    through the mysql client and comes back as a single JSON document.
    """
    env = read_env()
    wrapped = f"SELECT COALESCE(JSON_ARRAYAGG(r), JSON_ARRAY()) FROM ({sql}) AS r"
    # JSON_ARRAYAGG over a derived table needs the row as an object.
    result = subprocess.run(
        [
            "mysql", "-h", "127.0.0.1", "-P", "3306",
            "-u", env["DB_USER"], env["DB_NAME"], "-N", "--raw", "-e", wrapped,
        ],
        env={**os.environ, "MYSQL_PWD": env["DB_PASSWORD"]},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip() or "[]")


def query_objects(sql: str) -> list[dict]:
    """Run a SELECT that already produces a single JSON_OBJECT column."""
    env = read_env()
    result = subprocess.run(
        [
            "mysql", "-h", "127.0.0.1", "-P", "3306",
            "-u", env["DB_USER"], env["DB_NAME"], "-N", "--raw", "-e", sql,
        ],
        env={**os.environ, "MYSQL_PWD": env["DB_PASSWORD"]},
        capture_output=True,
        text=True,
        check=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


# --------------------------------------------------------------------------
# reference text normalisation
# --------------------------------------------------------------------------
_APOSTROPHES = "’‘ʼʻ`´'"
_PUNCT_CATEGORIES = {"P", "S"}


def clean_reference(text: str, strip_prefix: str = "") -> str:
    """Strip the editorial markup `translation_verses.text` may carry.

    The column is plain text (the markup lives in `html`), but the Synodal
    translation brackets the Septuagint additions — `[И стало так.]` — and the
    narrator reads them, so the brackets go and the words stay. The same for
    the two bracketed verses in UBH. `strip_prefix` removes a leading fragment
    that is present in the text but not spoken inside the excerpt (see PICKS).
    """
    if strip_prefix and text.startswith(strip_prefix):
        text = text[len(strip_prefix):]
    elif strip_prefix:
        raise SystemExit(f"prefix {strip_prefix!r} not found in {text[:80]!r}")
    text = text.replace("[", "").replace("]", "")
    return re.sub(r"\s+", " ", text).strip()


def normalise(text: str) -> str:
    """Lowercase, drop punctuation, unify ё/е and the apostrophe variants.

    Word error rate on ru/uk is dominated by inflection and by how the two
    sides spell the same sound, so the comparison is deliberately blind to
    case, punctuation, `ё` vs `е` and the four apostrophes Ukrainian is
    written with.
    """
    text = unicodedata.normalize("NFC", text).lower()
    text = text.replace("ё", "е").replace("ѐ", "е")
    for mark in _APOSTROPHES:
        text = text.replace(mark, "'")
    text = "".join(
        " " if unicodedata.category(ch)[0] in _PUNCT_CATEGORIES and ch != "'" else ch
        for ch in text
    )
    return re.sub(r"\s+", " ", text).strip()


def _levenshtein(a: list, b: list) -> int:
    """Plain Levenshtein distance over two token sequences."""
    if not a:
        return len(b)
    previous = list(range(len(b) + 1))
    for i, token_a in enumerate(a, 1):
        current = [i]
        for j, token_b in enumerate(b, 1):
            current.append(min(
                previous[j] + 1,             # deletion
                current[j - 1] + 1,          # insertion
                previous[j - 1] + (token_a != token_b),  # substitution
            ))
        previous = current
    return previous[-1]


def wer(reference: str, hypothesis: str) -> float:
    ref = normalise(reference).split()
    hyp = normalise(hypothesis).split()
    if not ref:
        return 0.0
    return _levenshtein(ref, hyp) / len(ref)


def cer(reference: str, hypothesis: str) -> float:
    ref = list(normalise(reference).replace(" ", ""))
    hyp = list(normalise(hypothesis).replace(" ", ""))
    if not ref:
        return 0.0
    return _levenshtein(ref, hyp) / len(ref)


# --------------------------------------------------------------------------
# sample construction
# --------------------------------------------------------------------------
def build_samples() -> None:
    env = read_env()
    audio_root = Path(env["AUDIO_DIR"])
    AUDIO_OUT.mkdir(parents=True, exist_ok=True)
    samples = []

    for (language, translation, voice, book, chapter, first_verse, genre,
         target, strip) in PICKS:
        rows = query_objects(
            "SELECT JSON_OBJECT('verse', a.verse_number, 'begin', a.begin, "
            "'end', a.end, 'text', v.text) "
            "FROM voice_alignments a "
            "JOIN voices vo ON vo.code = a.voice "
            "JOIN translations t ON t.code = vo.translation "
            "JOIN translation_verses v ON v.translation = t.code "
            "  AND v.book_number = a.book_number "
            "  AND v.chapter_number = a.chapter_number "
            "  AND v.verse_number = a.verse_number "
            f"WHERE vo.alias = '{voice}' AND t.alias = '{translation}' "
            f"AND a.book_number = {book} AND a.chapter_number = {chapter} "
            f"AND a.verse_number >= {first_verse} "
            "ORDER BY a.verse_number"
        )
        if not rows:
            raise SystemExit(f"no alignments for {translation}/{voice} {book}:{chapter}")

        # Extend verse by verse until the span is long enough, then stop
        # before it would exceed the cap.
        chosen: list[dict] = []
        begin = float(rows[0]["begin"])
        for row in rows:
            span = float(row["end"]) - begin
            if chosen and span > MAX_SECONDS:
                break
            chosen.append(row)
            if span >= target:
                break
        duration = float(chosen[-1]["end"]) - begin
        if not (10.0 <= duration <= MAX_SECONDS):
            raise SystemExit(
                f"cannot fit {translation} {book}:{chapter} into the window "
                f"({duration:.1f}s)"
            )

        source = (
            audio_root / translation / voice / "mp3" / f"{book:02d}" / f"{chapter:02d}.mp3"
        )
        if not source.exists():
            raise SystemExit(f"missing audio {source}")
        sample_id = f"{language}_{book:02d}{chapter:02d}_{chosen[0]['verse']}"
        excerpt = AUDIO_OUT / f"{sample_id}.m4a"
        subprocess.run(
            [
                "ffmpeg", "-y", "-loglevel", "error",
                "-ss", f"{begin:.3f}", "-t", f"{duration:.3f}", "-i", str(source),
                "-c:a", "aac", "-b:a", "64k", "-ac", "1", "-ar", "16000",
                "-movflags", "+faststart", str(excerpt),
            ],
            check=True,
        )
        measured = float(subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(excerpt)],
            capture_output=True, text=True, check=True,
        ).stdout.strip())

        samples.append({
            "id": sample_id,
            "language": language,
            "locale": LOCALES[language],
            "genre": genre,
            "translation": translation,
            "voice": voice,
            "book_number": book,
            "chapter_number": chapter,
            "verse_from": chosen[0]["verse"],
            "verse_to": chosen[-1]["verse"],
            "source_mp3": str(source),
            "offset_seconds": round(begin, 3),
            "span_seconds": round(duration, 3),
            "excerpt": str(excerpt.relative_to(BENCH_DIR.parent)),
            "excerpt_seconds": round(measured, 3),
            "stripped_prefix": strip,
            "reference": clean_reference(
                " ".join(row["text"] for row in chosen), strip
            ),
        })
        print(
            f"{sample_id:16s} {language} {genre:9s} "
            f"v{chosen[0]['verse']}-{chosen[-1]['verse']} "
            f"span={duration:5.1f}s file={measured:5.1f}s"
        )

    SAMPLES_PATH.write_text(
        json.dumps(samples, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"\n{len(samples)} samples -> {SAMPLES_PATH}")


def load_samples() -> list[dict]:
    return json.loads(SAMPLES_PATH.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# local models
# --------------------------------------------------------------------------
def run_local(model_name: str, beam_size: int, threads: int,
              only: list[str] | None, out_path: Path) -> None:
    from faster_whisper import WhisperModel

    samples = load_samples()
    if only:
        samples = [s for s in samples if s["id"] in only]

    load_started = time.monotonic()
    model = WhisperModel(
        str(MODELS_DIR / model_name),
        device="cpu",
        compute_type="int8",
        cpu_threads=threads,
    )
    load_seconds = time.monotonic() - load_started

    records = []
    for sample in samples:
        audio_path = BENCH_DIR.parent / sample["excerpt"]
        started = time.monotonic()
        segments, info = model.transcribe(
            str(audio_path),
            task="transcribe",
            language=sample["language"],
            vad_filter=True,
            beam_size=beam_size,
        )
        transcript = " ".join(segment.text.strip() for segment in segments).strip()
        transcript = re.sub(r"\s+", " ", transcript)
        elapsed = time.monotonic() - started
        duration = sample["excerpt_seconds"]
        records.append({
            "id": sample["id"],
            "language": sample["language"],
            "transcript": transcript,
            "seconds": round(elapsed, 3),
            "audio_seconds": duration,
            "ratio": round(elapsed / duration, 3),
            "wer": round(wer(sample["reference"], transcript), 4),
            "cer": round(cer(sample["reference"], transcript), 4),
        })
        print(
            f"{sample['id']:16s} {elapsed:6.1f}s / {duration:5.1f}s "
            f"= {elapsed / duration:4.2f}x  wer={records[-1]['wer']:.3f}",
            flush=True,
        )

    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    meta = {
        "kind": "run_meta",
        "engine": "faster-whisper",
        "model": model_name,
        "beam_size": beam_size,
        "cpu_threads": threads,
        "compute_type": "int8",
        "vad_filter": True,
        "load_seconds": round(load_seconds, 2),
        "peak_rss_mb": round(peak_rss_mb, 1),
        "samples": len(records),
    }
    with out_path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(meta, ensure_ascii=False) + "\n")
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"\npeak RSS {peak_rss_mb:.0f} MB (model load {load_seconds:.1f}s) -> {out_path}")


# --------------------------------------------------------------------------
# Gemini
# --------------------------------------------------------------------------
def transcription_prompt(locale: str) -> str:
    return (
        f"{_TRANSCRIPTION_PROMPT} The app locale is {locale}; use it only as a "
        "weak hint when the spoken language is ambiguous."
    )


def run_gemini(out_path: Path, pause: float) -> None:
    env = read_env()
    api_key = env["GEMINI_API_KEY"]
    model = env["AI_TRANSCRIBE_MODEL"]
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent"
    )
    records = []
    samples = load_samples()
    for index, sample in enumerate(samples):
        audio = (BENCH_DIR.parent / sample["excerpt"]).read_bytes()
        payload = {
            "contents": [{
                "role": "user",
                "parts": [
                    {"text": transcription_prompt(sample["locale"])},
                    {"inline_data": {
                        "mime_type": "audio/mp4",
                        "data": base64.b64encode(audio).decode("ascii"),
                    }},
                ],
            }],
            "generationConfig": {"maxOutputTokens": 4096, "temperature": 0},
        }
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"content-type": "application/json", "x-goog-api-key": api_key},
            method="POST",
        )
        started = time.monotonic()
        transcript, error = "", None
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                data = json.loads(response.read().decode("utf-8"))
            parts = data["candidates"][0]["content"]["parts"]
            transcript = "".join(
                part.get("text", "") for part in parts if isinstance(part, dict)
            ).strip()
        except urllib.error.HTTPError as exc:
            error = f"HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')[:300]}"
        except Exception as exc:  # noqa: BLE001 - benchmark records the failure
            error = f"{type(exc).__name__}: {exc}"
        elapsed = time.monotonic() - started
        record = {
            "id": sample["id"],
            "language": sample["language"],
            "transcript": re.sub(r"\s+", " ", transcript),
            "seconds": round(elapsed, 3),
            "audio_seconds": sample["excerpt_seconds"],
            "ratio": round(elapsed / sample["excerpt_seconds"], 3),
            "error": error,
        }
        if not error:
            record["wer"] = round(wer(sample["reference"], transcript), 4)
            record["cer"] = round(cer(sample["reference"], transcript), 4)
        records.append(record)
        print(
            f"{sample['id']:16s} {elapsed:6.1f}s "
            + (error or f"wer={record['wer']:.3f}"),
            flush=True,
        )
        if index + 1 < len(samples):
            time.sleep(pause)

    meta = {
        "kind": "run_meta",
        "engine": "gemini",
        "model": model,
        "samples": len(records),
        "failed": sum(1 for r in records if r["error"]),
    }
    with out_path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(meta, ensure_ascii=False) + "\n")
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"\n-> {out_path}")


# --------------------------------------------------------------------------
# the remote audio server (AI_TRANSCRIBE_PROVIDER=openai_compat)
# --------------------------------------------------------------------------
APP_DIR = Path(__file__).resolve().parent.parent / "app"

# What the mobile app declares when it uploads a voice message, and what
# `samples` encodes the excerpts as. `RemoteTranscriber` maps it to the
# multipart filename (`recording.m4a`), which is how a server built on the
# OpenAI shape picks its decoder.
UPLOAD_MIME = "audio/mp4"

# `app/config.py` fails fast on missing deployment variables (ADR 0008) and
# `transcription` imports it at module level. This tool builds its client from
# the command line and reads none of these values — they exist only so the
# import succeeds outside the container, exactly as `gen_rewrites.py` and
# `tests/conftest.py` do it. `setdefault`, so a real environment still wins.
_CONFIG_STUBS = (
    ("API_KEY", "transcribe-bench-unused"),
    ("DB_HOST", "transcribe-bench-unused"),
    ("DB_USER", "transcribe-bench-unused"),
    ("DB_PASSWORD", "transcribe-bench-unused"),
    ("DB_NAME", "transcribe-bench-unused"),
    ("EMBEDDING_PROVIDER", "gemini"),
    ("EMBEDDING_MODEL", "gemini-embedding-001"),
    ("EMBEDDING_DIMENSIONS", "768"),
    ("AI_QUESTION_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_REWRITE_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_RERANK_PROVIDER", "gemini"),
    # Deliberately `gemini` and not `openai_compat`: the stage configuration
    # of the machine this runs on must not decide what the benchmark measures
    # — the endpoint and the model come from the command line.
    ("AI_TRANSCRIBE_PROVIDER", "gemini"),
    ("AI_QUESTION_MODEL", "gemini-3.5-flash-lite"),
    ("AI_TRANSCRIBE_MODEL", "gemini-3.5-flash-lite"),
    ("AI_SCRIPTURE_REWRITE_MODEL", "gemini-3.7-flash"),
    ("AI_SCRIPTURE_RERANK_MODEL", "gemini-3.5-flash-lite"),
)


def build_remote_transcriber(endpoint: str, model: str, timeout: float,
                             attempts: int):
    """The PRODUCTION client (`RemoteTranscriber`), driven from a CLI.

    Not a copy of its request: the point of this subcommand is to measure what
    the deployed service will get, and a hand-written multipart body here
    could drift from the one `app/transcription.py` sends without anything
    failing. The key is read from `AI_TRANSCRIBE_API_KEY` and goes nowhere
    else — not into the artifact, not into a log line, not into the URL.
    """
    api_key = os.environ.get("AI_TRANSCRIBE_API_KEY", "").strip()
    for name, value in _CONFIG_STUBS:
        os.environ.setdefault(name, value)
    # The key is this TOOL's argument, not this machine's configuration, and
    # `config` would rightly refuse it beside the stubbed `gemini` stage
    # ("a key that only Gemini could spend, on a deployment with no Gemini
    # key"). So it is taken out of the environment before the import that
    # validates it and lives only in the local above.
    os.environ.pop("AI_TRANSCRIBE_API_KEY", None)
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
    from transcription import RemoteTranscriber

    return RemoteTranscriber(
        endpoint=endpoint,
        api_key=api_key,
        model=model,
        timeout=timeout,
        attempts=attempts,
    )


def remote_label(model: str) -> str:
    """`deepdml/faster-whisper-large-v3-turbo-ct2` -> `remote_faster-whisper-large-v3-turbo-ct2`.

    The label names the artifact (`transcribe_<label>.jsonl`) and the row of
    the report, so it must be a filename and must not collide with another
    model measured against the same endpoint.
    """
    short = re.sub(r"[^A-Za-z0-9._-]+", "-", model.rsplit("/", 1)[-1]).strip("-.")
    return f"remote_{short}" if short else "remote"


def run_remote(out_path: Path, endpoint: str, model: str, label: str,
               only: list[str] | None, timeout: float, attempts: int) -> None:
    """Measure an OpenAI-compatible AUDIO server over the same 15 samples.

    One `RemoteTranscriber.transcribe` per excerpt, in sequence: the wall
    clock therefore includes the network, the queue on the server and the
    retry ladder, which is exactly the number a person waits — and the reason
    the `ratio` column of this run is not comparable with a local one on
    anything but the total.
    """
    transcriber = build_remote_transcriber(endpoint, model, timeout, attempts)
    samples = load_samples()
    if only:
        samples = [s for s in samples if s["id"] in only]

    records = []
    for sample in samples:
        audio = (BENCH_DIR.parent / sample["excerpt"]).read_bytes()
        started = time.monotonic()
        transcript, error = "", None
        try:
            # The locale, not the language column: the app sends what the
            # phone is set to and the client reduces it to a Whisper language
            # itself. Passing `ru` here would measure a request production
            # never makes.
            transcript = asyncio.run(
                transcriber.transcribe(audio, UPLOAD_MIME, sample["locale"])
            )
        except Exception as exc:  # noqa: BLE001 - benchmark records the failure
            error = f"{type(exc).__name__}: {exc}"
        elapsed = time.monotonic() - started
        record = {
            "id": sample["id"],
            "language": sample["language"],
            "transcript": re.sub(r"\s+", " ", transcript),
            "seconds": round(elapsed, 3),
            "audio_seconds": sample["excerpt_seconds"],
            "ratio": round(elapsed / sample["excerpt_seconds"], 3),
            "error": error,
        }
        if not error:
            record["wer"] = round(wer(sample["reference"], transcript), 4)
            record["cer"] = round(cer(sample["reference"], transcript), 4)
        records.append(record)
        print(
            f"{sample['id']:16s} {elapsed:6.1f}s / {sample['excerpt_seconds']:5.1f}s "
            + (error or f"wer={record['wer']:.3f}"),
            flush=True,
        )

    meta = {
        "kind": "run_meta",
        "engine": "openai_compat",
        "model": model,
        "label": label,
        # The host only. Never the whole URL: `urlsplit().netloc` would carry
        # userinfo, and a key pasted into an endpoint is the one way it could
        # reach this file.
        "endpoint_host": urllib.parse.urlsplit(endpoint).hostname or "",
        "timeout_seconds": timeout,
        "attempts": attempts,
        "samples": len(records),
        "failed": sum(1 for r in records if r["error"]),
    }
    with out_path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(meta, ensure_ascii=False) + "\n")
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"\n-> {out_path}")


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------
def load_run(path: Path) -> tuple[dict, dict[str, dict]]:
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    return lines[0], {record["id"]: record for record in lines[1:]}


def run_paths() -> list[Path]:
    """Every run artifact, local first, then remote, then Gemini.

    Globbed rather than listed: a `remote` run of a model nobody has measured
    before appears in the report by existing, which is the point — the file
    name carries the label and the label carries the model.
    """
    return (
        sorted(BENCH_DIR.glob("transcribe_local_*.jsonl"))
        + sorted(BENCH_DIR.glob("transcribe_remote*.jsonl"))
        + sorted(BENCH_DIR.glob("transcribe_gemini.jsonl"))
    )


def report() -> None:
    samples = load_samples()
    runs = run_paths()
    # A remote label carries the model name, which is far longer than any
    # local one ("remote_faster-whisper-large-v3-turbo-ct2"), so the column is
    # sized from the runs actually present instead of a constant that the
    # first long name would silently break.
    width = max([28] + [len(path.stem.replace("transcribe_", "")) for path in runs])

    header = (
        f"{'run':{width}s} {'lang':5s} {'n':>2s} {'WER':>7s} {'CER':>7s} "
        f"{'ratio~':>7s} {'ratio^':>7s} {'RSS MB':>7s}"
    )
    print(header)
    print("-" * len(header))
    for path in runs:
        meta, records = load_run(path)
        label = path.stem.replace("transcribe_", "")
        for language in ("ru", "uk", "en", "all"):
            picked = [
                record for record in records.values()
                if (language == "all" or record["language"] == language)
                and not record.get("error")
            ]
            if not picked:
                continue
            ratios = [record["ratio"] for record in picked]
            print(
                f"{label:{width}s} {language:5s} {len(picked):2d} "
                f"{statistics.mean(r['wer'] for r in picked):7.3f} "
                f"{statistics.mean(r['cer'] for r in picked):7.3f} "
                f"{statistics.mean(ratios):7.2f} {max(ratios):7.2f} "
                f"{meta.get('peak_rss_mb', 0):7.1f}"
            )
        print()

    print("\n=== per-sample transcripts ===")
    loaded = [(path.stem.replace("transcribe_", ""), load_run(path)[1])
              for path in runs]
    for sample in samples:
        print(f"\n--- {sample['id']} ({sample['language']}, {sample['genre']}, "
              f"{sample['excerpt_seconds']:.1f}s) ---")
        print(f"{'REF':{width}s}: {sample['reference']}")
        for label, records in loaded:
            record = records.get(sample["id"])
            if record:
                print(f"{label:{width}s}: "
                      f"{record.get('error') or record['transcript']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("samples")

    local = sub.add_parser("local")
    local.add_argument("--model", required=True)
    local.add_argument("--beam", type=int, required=True)
    local.add_argument("--threads", type=int, default=os.cpu_count() or 8)
    local.add_argument("--only", nargs="*")
    local.add_argument("--out")

    gemini = sub.add_parser("gemini")
    gemini.add_argument("--pause", type=float, default=4.0)
    gemini.add_argument("--out", default=str(BENCH_DIR / "transcribe_gemini.jsonl"))

    remote = sub.add_parser("remote")
    remote.add_argument("--endpoint",
                        default=os.environ.get("AI_TRANSCRIBE_ENDPOINT", ""))
    remote.add_argument("--model",
                        default=os.environ.get("AI_TRANSCRIBE_MODEL", ""))
    # Names the artifact and the report row; defaults to the model's own short
    # name, so two models behind one endpoint cannot overwrite each other.
    remote.add_argument("--label")
    remote.add_argument("--only", nargs="*")
    # The production ceiling (`AI_TRANSCRIBE_TIMEOUT_SECONDS`) and the
    # production ladder (`RemoteTranscriber.attempts`), so a run that would
    # have failed in the service fails here too.
    remote.add_argument("--timeout", type=float, default=60.0)
    remote.add_argument("--attempts", type=int, default=2)
    remote.add_argument("--out")

    sub.add_parser("report")
    args = parser.parse_args()

    if args.command == "samples":
        build_samples()
    elif args.command == "local":
        default = BENCH_DIR / f"transcribe_local_{args.model}_beam{args.beam}.jsonl"
        run_local(args.model, args.beam, args.threads, args.only,
                  Path(args.out) if args.out else default)
    elif args.command == "gemini":
        run_gemini(Path(args.out), args.pause)
    elif args.command == "remote":
        if not args.endpoint or not args.model:
            raise SystemExit(
                "remote needs --endpoint and --model (or AI_TRANSCRIBE_ENDPOINT "
                "/ AI_TRANSCRIBE_MODEL in the environment); the key is read "
                "from AI_TRANSCRIBE_API_KEY and never printed"
            )
        label = args.label or remote_label(args.model)
        out_path = Path(args.out) if args.out else (
            BENCH_DIR / f"transcribe_{label}.jsonl"
        )
        run_remote(out_path, args.endpoint, args.model, label, args.only,
                   args.timeout, args.attempts)
    else:
        report()


if __name__ == "__main__":
    sys.exit(main())
