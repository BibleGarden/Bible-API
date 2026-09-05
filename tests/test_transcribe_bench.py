"""Tests for evaluation/transcribe_bench.py — the `remote` subcommand.

The benchmark is not part of the service, but its `remote` leg makes one
claim that a wrong measurement would hide: that it sends **the production
request**. It does that by driving `app/transcription.RemoteTranscriber`
rather than writing a multipart body of its own, so what is asserted here is
the wiring around it — the URL, the fields, the `language` hint derived from
the sample's LOCALE (not from its language column), the key taken from the
environment, and the artifact that comes out.

Nothing here touches the network: the client's httpx transport is an
`httpx.MockTransport`, exactly as `tests/test_transcription.py` drives the
same class. Nothing here touches the real samples either — they reference
audio files that are deliberately not in git — so the fixtures build a
two-sample corpus in a tmp directory.

The module is loaded by path because `evaluation/` is deliberately not on
`pythonpath` (pytest.ini exposes `app` only); importing it is side-effect
free, and the config stubs it needs are applied inside
`build_remote_transcriber`, not at import time.
"""

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

import httpx
import pytest

BENCH = Path(__file__).resolve().parents[1] / "evaluation" / "transcribe_bench.py"


def _load_bench():
    spec = importlib.util.spec_from_file_location("transcribe_bench", BENCH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def bench():
    return _load_bench()


@pytest.fixture
def corpus(bench, tmp_path, monkeypatch):
    """Two samples and their (fake) audio, in place of `bench_data`.

    `ru-RU` is a locale Whisper knows and `zz-ZZ` is not, which is what makes
    the pair a test of the weak-hint contract rather than of one request.
    """
    audio_dir = tmp_path / "bench_data" / "transcribe_audio"
    audio_dir.mkdir(parents=True)
    (audio_dir / "ru_0101_1.m4a").write_bytes(b"fake-m4a-ru")
    (audio_dir / "zz_0101_1.m4a").write_bytes(b"fake-m4a-zz")
    samples = [
        {
            "id": "ru_0101_1",
            "language": "ru",
            "locale": "ru-RU",
            "genre": "narrative",
            "excerpt": "bench_data/transcribe_audio/ru_0101_1.m4a",
            "excerpt_seconds": 10.0,
            "reference": "В начале сотворил Бог небо и землю",
        },
        {
            "id": "zz_0101_1",
            "language": "zz",
            "locale": "zz-ZZ",
            "genre": "narrative",
            "excerpt": "bench_data/transcribe_audio/zz_0101_1.m4a",
            "excerpt_seconds": 20.0,
            "reference": "unknown language reference",
        },
    ]
    samples_path = tmp_path / "bench_data" / "transcribe_samples.json"
    samples_path.write_text(json.dumps(samples, ensure_ascii=False),
                            encoding="utf-8")
    monkeypatch.setattr(bench, "BENCH_DIR", tmp_path / "bench_data")
    monkeypatch.setattr(bench, "SAMPLES_PATH", samples_path)
    return tmp_path / "bench_data"


def _mock_httpx(handler):
    """Patch httpx.AsyncClient so every client built runs on MockTransport."""
    real_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        return real_client(*args, transport=httpx.MockTransport(handler),
                           **kwargs)

    return mock.patch("httpx.AsyncClient", factory)


@pytest.mark.parametrize(
    "model,expected",
    [
        ("deepdml/faster-whisper-large-v3-turbo-ct2",
         "remote_faster-whisper-large-v3-turbo-ct2"),
        ("Systran/faster-whisper-large-v3", "remote_faster-whisper-large-v3"),
        ("whisper large:v3", "remote_whisper-large-v3"),
        ("", "remote"),
    ],
)
def test_the_label_is_a_filename_made_of_the_model_name(bench, model, expected):
    assert bench.remote_label(model) == expected


def test_the_request_is_the_production_one(bench, corpus, monkeypatch):
    """URL, fields, filename, MIME and the bearer key, on the wire."""
    monkeypatch.setenv("AI_TRANSCRIBE_API_KEY", "bench-secret-key")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"text": "В начале сотворил Бог небо и землю"})

    with _mock_httpx(handler):
        bench.run_remote(
            corpus / "transcribe_remote_x.jsonl",
            "https://whisper.example:8443/whisper/v1",
            "deepdml/faster-whisper-large-v3-turbo-ct2",
            "remote_x",
            ["ru_0101_1"],
            timeout=60.0,
            attempts=2,
        )

    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert str(request.url) == (
        "https://whisper.example:8443/whisper/v1/audio/transcriptions"
    )
    assert request.headers["authorization"] == "Bearer bench-secret-key"
    assert request.headers["content-type"].startswith("multipart/form-data;")
    body = request.content
    assert b'name="model"\r\n\r\ndeepdml/faster-whisper-large-v3-turbo-ct2' in body
    assert b'name="response_format"\r\n\r\njson' in body
    assert b'name="temperature"\r\n\r\n0' in body
    # From the LOCALE (`ru-RU`), the way the app derives it.
    assert b'name="language"\r\n\r\nru' in body
    assert b'filename="recording.m4a"' in body
    assert b"Content-Type: audio/mp4" in body
    assert b"fake-m4a-ru" in body


def test_an_unknown_locale_sends_no_language_field(bench, corpus, monkeypatch):
    """The locale is a weak hint: a language Whisper cannot name is dropped."""
    monkeypatch.setenv("AI_TRANSCRIBE_API_KEY", "bench-secret-key")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"text": "whatever the model heard"})

    with _mock_httpx(handler):
        bench.run_remote(
            corpus / "transcribe_remote_x.jsonl",
            "https://whisper.example:8443/whisper/v1",
            "some-model",
            "remote_x",
            ["zz_0101_1"],
            timeout=60.0,
            attempts=2,
        )

    assert b'name="language"' not in seen[0].content


def test_no_key_in_the_environment_sends_no_authorization(bench, corpus,
                                                          monkeypatch):
    monkeypatch.delenv("AI_TRANSCRIBE_API_KEY", raising=False)
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"text": "text"})

    with _mock_httpx(handler):
        bench.run_remote(
            corpus / "transcribe_remote_x.jsonl",
            "https://whisper.example:8443/whisper/v1",
            "some-model",
            "remote_x",
            ["ru_0101_1"],
            timeout=60.0,
            attempts=2,
        )

    assert "authorization" not in seen[0].headers


def test_the_artifact_has_the_shape_the_report_reads(bench, corpus, monkeypatch):
    """`run_meta` first, one record per sample, and no key anywhere in it."""
    monkeypatch.setenv("AI_TRANSCRIBE_API_KEY", "bench-secret-key")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": "В начале сотворил Бог небо"})

    out = corpus / "transcribe_remote_turbo.jsonl"
    with _mock_httpx(handler):
        bench.run_remote(
            out,
            "https://whisper.example:8443/whisper/v1",
            "deepdml/faster-whisper-large-v3-turbo-ct2",
            "remote_turbo",
            ["ru_0101_1"],
            timeout=60.0,
            attempts=2,
        )

    raw = out.read_text(encoding="utf-8")
    assert "bench-secret-key" not in raw
    meta, record = [json.loads(line) for line in raw.splitlines()]
    assert meta["kind"] == "run_meta"
    assert meta["engine"] == "openai_compat"
    assert meta["model"] == "deepdml/faster-whisper-large-v3-turbo-ct2"
    assert meta["label"] == "remote_turbo"
    # The host only — never the whole URL, which could carry userinfo.
    assert meta["endpoint_host"] == "whisper.example"
    assert meta["samples"] == 1 and meta["failed"] == 0
    assert record["id"] == "ru_0101_1"
    assert record["language"] == "ru"
    assert record["audio_seconds"] == 10.0
    assert record["error"] is None
    # Two words missing out of seven, and the timing is real wall clock.
    assert record["wer"] == pytest.approx(2 / 7, abs=0.001)
    # Wall clock of the call itself: against a MockTransport it rounds to 0.0.
    assert 0.0 <= record["seconds"] < 60.0
    assert record["ratio"] == pytest.approx(record["seconds"] / 10.0, abs=0.001)


def test_a_failing_server_is_recorded_not_raised(bench, corpus, monkeypatch):
    """A run must survive one dead sample: the report shows the rest."""
    monkeypatch.setenv("AI_TRANSCRIBE_API_KEY", "bench-secret-key")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "invalid key"})

    out = corpus / "transcribe_remote_turbo.jsonl"
    with _mock_httpx(handler):
        bench.run_remote(
            out, "https://whisper.example:8443/whisper/v1", "some-model",
            "remote_turbo", ["ru_0101_1"], timeout=60.0, attempts=2,
        )

    meta, record = [json.loads(line)
                    for line in out.read_text(encoding="utf-8").splitlines()]
    assert meta["failed"] == 1
    assert record["error"]
    # The failure is the client's own category, so nothing of the body — and
    # nothing of the key — reaches the artifact.
    assert "invalid key" not in record["error"]
    assert "wer" not in record


def test_the_report_picks_a_remote_run_up_by_itself(bench, corpus, capsys):
    """No listing to edit: the file's presence is what adds the row."""
    run = [
        {"kind": "run_meta", "engine": "openai_compat", "model": "m",
         "label": "remote_turbo", "samples": 1, "failed": 0},
        {"id": "ru_0101_1", "language": "ru", "transcript": "расшифровка",
         "seconds": 3.0, "audio_seconds": 10.0, "ratio": 0.3,
         "error": None, "wer": 0.1, "cer": 0.05},
    ]
    (corpus / "transcribe_remote_turbo.jsonl").write_text(
        "\n".join(json.dumps(line, ensure_ascii=False) for line in run) + "\n",
        encoding="utf-8",
    )
    assert bench.run_paths() == [corpus / "transcribe_remote_turbo.jsonl"]

    bench.report()
    out = capsys.readouterr().out
    # The aggregate row and the side-by-side column, both under the label.
    assert "remote_turbo" in out
    assert "0.100" in out
    assert "расшифровка" in out
    # A sample the run has no record for is simply absent from its column.
    assert "zz_0101_1" in out
