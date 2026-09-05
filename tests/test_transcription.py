"""Tests for app/transcription.py — the two non-Gemini providers (ADR 0012).

No weights and no faster-whisper are needed: `fake_faster_whisper` puts a
stand-in module in `sys.modules` (the shape `tests/test_embeddings.py` uses
for sentence-transformers), and the remote client is driven through
`httpx.MockTransport`. The one test that loads real weights is skipped unless
`AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST` asks for it.
"""

import asyncio
import io
import logging
import os
import sys
import threading
import time
import types
import wave

import httpx
import numpy as np
import pytest

import transcription
from test_gemini_retry import FakeClock
from transcription import (
    LocalTranscriber,
    RemoteTranscriber,
    TranscriptionUnavailable,
    bearer_headers,
    transcriptions_url,
    whisper_language,
)

SAMPLE_RATE = transcription.SAMPLE_RATE


def segment(text: str):
    """One faster-whisper `Segment`, as much of it as this code reads."""
    return types.SimpleNamespace(text=text)


class FakeWhisperModel:
    """A `WhisperModel` that records its call and answers fixed segments."""

    def __init__(self, segments=("Господи, помоги мне.",), languages=None,
                 detected="ru", delay=0.0, error=None):
        self.segments = [segment(text) for text in segments]
        self.supported_languages = list(
            languages if languages is not None else ["en", "ru", "uk", "de"]
        )
        self.detected = detected
        self.delay = delay
        self.error = error
        self.calls = []

    def transcribe(self, audio, **kwargs):
        self.calls.append((audio, kwargs))
        if self.error is not None:
            raise self.error
        if self.delay:
            time.sleep(self.delay)

        def generate():
            yield from self.segments

        return generate(), types.SimpleNamespace(language=self.detected)


def waveform(seconds: float) -> np.ndarray:
    return np.zeros(int(seconds * SAMPLE_RATE), dtype=np.float32)


@pytest.fixture
def fake_faster_whisper(monkeypatch):
    """A stand-in `faster_whisper` module: the loader and the decoder.

    `decode_audio` answers a 3-second waveform for any bytes, so the tests
    never depend on a real container being parsed.
    """
    built = []
    decoded = []

    def fake_decode(source, sampling_rate=SAMPLE_RATE):
        decoded.append((source.read() if hasattr(source, "read") else source,
                        sampling_rate))
        return waveform(3.0)

    # `module.error` / `module.model` are read through this holder rather
    # than as class attributes: a test sets them on the module INSTANCE, and
    # a class-attribute lookup would never see that.
    holder = {}

    def fake_whisper_model(path, **kwargs):
        built.append((path, kwargs))
        error = getattr(holder["module"], "error", None)
        if error is not None:
            raise error
        return getattr(holder["module"], "model", None) or FakeWhisperModel()

    class Module(types.ModuleType):
        model = None
        error = None
        WhisperModel = staticmethod(fake_whisper_model)
        decode_audio = staticmethod(fake_decode)

    module = Module("faster_whisper")
    holder["module"] = module
    monkeypatch.setitem(sys.modules, "faster_whisper", module)
    monkeypatch.setattr(transcription, "_model", None)
    yield module, built, decoded
    transcription._model = None


def test_the_transcription_timings_are_visible_under_uvicorn():
    """A timing line nothing handles is not a line.

    `Local transcription: … ratio=…` and `Remote transcription: …` are how a
    new machine is judged (ADR 0012), and uvicorn leaves the ROOT logger
    without handlers — so `app/main.py` must ask `ensure_visible_handler` for
    this module's logger exactly as it does for the two banners, or the
    documented grep finds nothing in `docker logs`.
    """
    import main

    root = logging.getLogger()
    saved = (root.handlers, transcription.logger.handlers,
             transcription.logger.level)
    try:
        root.handlers = []
        transcription.logger.handlers = []
        transcription.logger.setLevel(logging.NOTSET)
        main.ensure_visible_handler(logging.getLogger("transcription"))
        assert transcription.logger.handlers, "transcription logs are invisible"
        assert transcription.logger.isEnabledFor(logging.INFO)
    finally:
        root.handlers, transcription.logger.handlers, level = saved
        transcription.logger.setLevel(level)


# --- the locale hint -------------------------------------------------------


@pytest.mark.parametrize(
    "locale, expected",
    [
        ("ru-RU", "ru"),
        ("uk-UA", "uk"),
        ("en", "en"),
        ("EN-gb", "en"),
        ("zh-Hant-TW", "zh"),
        ("iw-IL", "he"),
        ("nb-NO", "no"),
        # Whisper knows Nynorsk under its own code, so it is not aliased.
        ("nn-NO", "nn"),
        (None, None),
        ("", None),
        # A language Whisper does not have: the locale is a WEAK hint, so it
        # becomes auto-detection rather than a refusal.
        ("xx-YY", None),
        ("zzz", None),
    ],
)
def test_locale_becomes_a_language_hint_or_nothing(locale, expected):
    assert whisper_language(locale) == expected


def test_a_model_that_knows_one_language_rejects_the_others():
    """`small.en` must not be asked for Russian just because the phone is."""
    assert whisper_language("ru-RU", frozenset({"en"})) is None
    assert whisper_language("en-US", frozenset({"en"})) == "en"


# --- loading the weights ---------------------------------------------------


def test_the_model_is_loaded_once_with_the_configured_quantisation(
    fake_faster_whisper,
):
    module, built, _decoded = fake_faster_whisper
    first = transcription.load_transcription_model(
        path="/models/whisper/small", compute_type="int8", cpu_threads=4
    )
    again = transcription.load_transcription_model(
        path="/models/whisper/small", compute_type="int8", cpu_threads=4
    )

    assert first is again  # the weights are loaded once per process, or never
    assert len(built) == 1
    path, kwargs = built[0]
    assert path == "/models/whisper/small"
    assert kwargs["device"] == "cpu"
    assert kwargs["compute_type"] == "int8"
    assert kwargs["cpu_threads"] == 4
    # Structural offline promise: never a hub id, never a download.
    assert kwargs["local_files_only"] is True


def test_loading_without_a_path_names_the_variable(monkeypatch):
    monkeypatch.setattr(transcription, "_model", None)
    with pytest.raises(TranscriptionUnavailable, match="AI_TRANSCRIBE_MODEL_PATH"):
        transcription.load_transcription_model(path="")


def test_a_broken_weights_directory_is_reported_by_type(fake_faster_whisper):
    module, _built, _decoded = fake_faster_whisper
    module.error = OSError("no such file or directory: model.bin")

    with pytest.raises(TranscriptionUnavailable) as exc_info:
        transcription.load_transcription_model(path="/models/whisper/gone")

    message = str(exc_info.value)
    assert "OSError" in message
    assert "/models/whisper/gone" in message
    assert "model.bin" not in message


# --- transcribing locally --------------------------------------------------


def test_segments_are_joined_verbatim_in_the_original_language(
    fake_faster_whisper,
):
    model = FakeWhisperModel(segments=(" Господи, ", " помоги мне. ", "  "))
    client = LocalTranscriber(model=model)

    text = client.transcribe(b"m4a-bytes", "audio/mp4", "ru-RU")

    assert text == "Господи, помоги мне."
    _audio, kwargs = model.calls[0]
    assert kwargs["task"] == "transcribe"  # never `translate`
    assert kwargs["language"] == "ru"
    assert kwargs["vad_filter"] is True
    assert kwargs["beam_size"] == client.beam_size


def test_without_a_locale_the_model_detects_the_language(fake_faster_whisper):
    model = FakeWhisperModel()
    LocalTranscriber(model=model).transcribe(b"m4a", "audio/mp4", None)

    assert model.calls[0][1]["language"] is None


def test_the_upload_is_decoded_before_the_model_sees_it(fake_faster_whisper):
    _module, _built, decoded = fake_faster_whisper
    model = FakeWhisperModel()

    LocalTranscriber(model=model).transcribe(b"m4a-bytes", "audio/mp4", None)

    assert decoded == [(b"m4a-bytes", SAMPLE_RATE)]
    # The model is handed the waveform, not the container.
    assert isinstance(model.calls[0][0], np.ndarray)


def test_a_recording_longer_than_the_ceiling_is_refused_before_the_model(
    fake_faster_whisper, monkeypatch
):
    """The 14 MiB upload cap lets ~30 minutes of AAC through, which locally is
    30 minutes of CPU nobody can interrupt."""
    module, _built, _decoded = fake_faster_whisper
    monkeypatch.setattr(
        module, "decode_audio", lambda source, sampling_rate: waveform(700.0)
    )
    model = FakeWhisperModel()

    with pytest.raises(TranscriptionUnavailable) as exc_info:
        LocalTranscriber(model=model, max_audio_seconds=600.0).transcribe(
            b"long", "audio/mp4", None
        )

    assert "AI_TRANSCRIBE_MAX_AUDIO_SECONDS" in str(exc_info.value)
    assert model.calls == []


def test_an_undecodable_upload_is_reported_by_type(fake_faster_whisper, monkeypatch):
    module, _built, _decoded = fake_faster_whisper

    def broken(source, sampling_rate):
        raise ValueError("moov atom not found in private-recording.m4a")

    monkeypatch.setattr(module, "decode_audio", broken)

    with pytest.raises(TranscriptionUnavailable) as exc_info:
        LocalTranscriber(model=FakeWhisperModel()).transcribe(
            b"junk", "audio/mp4", None
        )

    message = str(exc_info.value)
    assert "ValueError" in message
    assert "private-recording" not in message


def test_a_failing_model_reports_the_type_and_nothing_else(fake_faster_whisper):
    model = FakeWhisperModel(error=RuntimeError("failed on 'помоги мне'"))

    with pytest.raises(TranscriptionUnavailable) as exc_info:
        LocalTranscriber(model=model).transcribe(b"m4a", "audio/mp4", None)

    message = str(exc_info.value)
    assert "RuntimeError" in message
    assert "помоги" not in message


def test_an_empty_transcript_is_a_failure_not_an_empty_answer(
    fake_faster_whisper,
):
    model = FakeWhisperModel(segments=("", "   "))

    with pytest.raises(TranscriptionUnavailable, match="no transcript"):
        LocalTranscriber(model=model).transcribe(b"m4a", "audio/mp4", None)


def test_nothing_about_the_recording_reaches_the_log(
    fake_faster_whisper, caplog
):
    model = FakeWhisperModel(segments=("Господи, помоги мне.",))

    with caplog.at_level(logging.INFO):
        LocalTranscriber(model=model).transcribe(b"m4a", "audio/mp4", "ru-RU")

    assert "Local transcription" in caplog.text
    assert "помоги" not in caplog.text


def test_transcriptions_are_serialised_process_wide(fake_faster_whisper):
    """One set of weights, one transcription at a time: CTranslate2 already
    spreads a run across the cores, so two at once only oversubscribe them."""
    model = FakeWhisperModel(delay=0.05)
    overlaps = []
    running = []
    guard = threading.Lock()
    real_transcribe = model.transcribe

    def watched(audio, **kwargs):
        with guard:
            running.append(1)
            overlaps.append(len(running))
        try:
            return real_transcribe(audio, **kwargs)
        finally:
            with guard:
                running.pop()

    model.transcribe = watched
    # Two clients over ONE model: the lock is module-level for exactly this.
    clients = [LocalTranscriber(model=model) for _ in range(2)]
    threads = [
        threading.Thread(
            target=lambda client=client: client.transcribe(
                b"m4a", "audio/mp4", None
            )
        )
        for client in clients
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert max(overlaps) == 1


def test_a_slow_run_is_reported_against_the_ceiling(fake_faster_whisper, caplog):
    model = FakeWhisperModel(delay=0.02)

    with caplog.at_level(logging.WARNING):
        LocalTranscriber(model=model, slow_after_seconds=0.001).transcribe(
            b"m4a", "audio/mp4", None
        )

    assert "AI_TRANSCRIBE_TIMEOUT_SECONDS" in caplog.text


# --- the remote provider ---------------------------------------------------


@pytest.mark.parametrize(
    "endpoint, expected",
    [
        ("https://whisper.example/v1", "https://whisper.example/v1/audio/transcriptions"),
        ("https://whisper.example/v1/", "https://whisper.example/v1/audio/transcriptions"),
        (
            "https://whisper.example/v1/audio/transcriptions",
            "https://whisper.example/v1/audio/transcriptions",
        ),
    ],
)
def test_the_audio_method_is_appended_once(endpoint, expected):
    assert transcriptions_url(endpoint) == expected


def test_an_empty_key_sends_no_authorization_header():
    assert bearer_headers("") == {}
    assert bearer_headers("k") == {"Authorization": "Bearer k"}


def remote_client(handler, **kwargs):
    """A `RemoteTranscriber` whose httpx client is a MockTransport."""
    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient
    kwargs.setdefault("endpoint", "https://whisper.example:8000/v1")
    kwargs.setdefault("api_key", "audio-key")
    kwargs.setdefault("model", "Systran/faster-whisper-large-v3")
    kwargs.setdefault("sleep", _no_sleep)
    client = RemoteTranscriber(**kwargs)

    def async_client(*args, **inner):
        return real_async_client(*args, transport=transport, **inner)

    return client, async_client


async def _no_sleep(_seconds):
    return None


def run_remote(client, async_client, monkeypatch, *, audio=b"m4a-bytes",
               mime_type="audio/mp4", locale="ru-RU"):
    monkeypatch.setattr(transcription.httpx, "AsyncClient", async_client)
    return asyncio.run(client.transcribe(audio, mime_type, locale))


def test_the_remote_request_is_the_openai_audio_shape(monkeypatch):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("authorization")
        captured["body"] = request.read()
        captured["content_type"] = request.headers["content-type"]
        return httpx.Response(200, json={"text": " Господи, помоги мне. "})

    client, async_client = remote_client(handler)
    text = run_remote(client, async_client, monkeypatch)

    assert text == "Господи, помоги мне."
    assert captured["url"] == (
        "https://whisper.example:8000/v1/audio/transcriptions"
    )
    assert captured["auth"] == "Bearer audio-key"
    assert captured["content_type"].startswith("multipart/form-data")
    body = captured["body"]
    assert b'name="file"; filename="recording.m4a"' in body
    assert b"m4a-bytes" in body
    assert b'name="model"' in body and b"faster-whisper-large-v3" in body
    assert b'name="response_format"' in body and b"json" in body
    # `temperature=0` is the verbatim contract, not a preference: there is no
    # prompt to disobey and nothing for a sampled token to invent.
    assert b'name="temperature"\r\n\r\n0\r\n' in body
    assert b'name="language"' in body and b"\r\n\r\nru" in body


def test_an_unknown_locale_sends_no_language_field(monkeypatch):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.read()
        return httpx.Response(200, json={"text": "text"})

    client, async_client = remote_client(handler)
    run_remote(client, async_client, monkeypatch, locale="xx-YY")

    assert b'name="language"' not in captured["body"]


def test_no_authorization_header_without_a_key(monkeypatch):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"text": "text"})

    client, async_client = remote_client(handler, api_key="")
    run_remote(client, async_client, monkeypatch)

    assert captured["auth"] is None


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="not json at all"),
        httpx.Response(200, json={"error": "no text here"}),
        httpx.Response(200, json={"text": "   "}),
        httpx.Response(200, json=["text"]),
    ],
)
def test_an_answer_that_is_not_a_transcript_is_unavailable(monkeypatch, response):
    client, async_client = remote_client(lambda request: response)

    with pytest.raises(TranscriptionUnavailable):
        run_remote(client, async_client, monkeypatch)


def test_a_retryable_status_is_retried_then_reported(monkeypatch):
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(503, text="model server restarting")

    client, async_client = remote_client(handler, attempts=2)

    with pytest.raises(TranscriptionUnavailable) as exc_info:
        run_remote(client, async_client, monkeypatch)

    assert len(attempts) == 2
    assert "restarting" not in str(exc_info.value)


def test_a_restarting_server_recovers_on_the_second_attempt(monkeypatch):
    answers = [
        httpx.Response(503, text="starting"),
        httpx.Response(200, json={"text": "Господи"}),
    ]
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        return answers.pop(0)

    client, async_client = remote_client(handler, attempts=2)

    assert run_remote(client, async_client, monkeypatch) == "Господи"
    # The recording is carried as BYTES, not as a file object: a second
    # attempt re-encodes the same multipart body rather than posting an
    # exhausted stream. Boundaries differ per request, the parts do not.
    assert len(bodies) == 2
    for body in bodies:
        assert b"m4a-bytes" in body
        assert b'name="model"' in body


def test_a_non_retryable_status_is_not_retried(monkeypatch):
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(400, json={"error": "unknown model"})

    client, async_client = remote_client(handler)

    with pytest.raises(TranscriptionUnavailable):
        run_remote(client, async_client, monkeypatch)

    assert len(attempts) == 1


def test_a_transport_failure_never_quotes_the_url_or_the_key(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            "connection refused to https://audio-key@whisper.example:8000/v1"
        )

    client, async_client = remote_client(handler)

    with pytest.raises(TranscriptionUnavailable) as exc_info:
        run_remote(client, async_client, monkeypatch)

    message = str(exc_info.value)
    assert "ConnectError" in message
    assert "audio-key" not in message
    assert "whisper.example" not in message


def test_the_call_is_bounded_by_a_carved_timeout(monkeypatch):
    """A bare number is per httpx PHASE; the carved one sums to the ceiling."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": "text"})

    client, async_client = remote_client(handler, timeout=60.0)

    def recording_client(*args, **kwargs):
        seen["timeout"] = kwargs.get("timeout")
        return async_client(*args, **kwargs)

    run_remote(client, recording_client, monkeypatch)

    timeout = seen["timeout"]
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.connect + timeout.write + timeout.pool + timeout.read <= 60.0


def test_the_whole_remote_call_stays_inside_the_ceiling(monkeypatch):
    """`AI_TRANSCRIBE_TIMEOUT_SECONDS` bounds the CALL, not one attempt.

    The failure this pins (ClickUp 86cbegg3w, live matrix): a stand-in that
    accepted the connection and answered nothing — the ordinary "process up,
    app dead" outage — was waited out once per attempt with a backoff in
    between, and `/api/ai/transcribe` answered its 502 after **116.1 s**
    against a documented 60 s ceiling.

    Run on a fake clock that advances only where real time would be spent:
    inside the provider call (by the sum of httpx's four phases, which is
    what a hung endpoint may take) and inside the backoff.
    """
    clock = FakeClock()
    start = clock.now
    phases = []

    def handler(request: httpx.Request) -> httpx.Response:
        timeout = dict(request.extensions.get("timeout", {}))
        phases.append(timeout)
        clock.advance(sum(value for value in timeout.values() if value))
        raise httpx.ReadTimeout("timed out", request=request)

    async def sleep(seconds):
        clock.advance(seconds)

    client, async_client = remote_client(
        handler, timeout=60.0, attempts=2, sleep=sleep, clock=clock
    )

    with pytest.raises(TranscriptionUnavailable):
        run_remote(client, async_client, monkeypatch)

    assert clock.now - start <= 60.0
    # The first attempt spent the whole budget, so the second one — which
    # could only have answered past the ceiling — was never started.
    assert len(phases) == 1


def test_a_fast_failure_still_buys_the_retry_inside_the_budget(monkeypatch):
    """The ceiling must not cost the recovery the second attempt exists for.

    A server that fails FAST (a 503 while restarting) leaves almost the whole
    budget, so the backoff is slept and the retry runs — the case
    `test_a_restarting_server_recovers_on_the_second_attempt` covers
    functionally, asserted here against the clock.
    """
    clock = FakeClock()
    start = clock.now
    answers = [
        httpx.Response(503, text="starting"),
        httpx.Response(200, json={"text": "Господи"}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        clock.advance(0.5)
        return answers.pop(0)

    async def sleep(seconds):
        clock.advance(seconds)

    client, async_client = remote_client(
        handler, timeout=60.0, attempts=2, sleep=sleep, clock=clock
    )

    assert run_remote(client, async_client, monkeypatch) == "Господи"
    elapsed = clock.now - start
    assert 2.0 <= elapsed <= 60.0   # the backoff was slept, the budget held


@pytest.mark.parametrize("missing", ["endpoint", "model"])
def test_an_unconfigured_remote_client_refuses_to_post_anything(
    monkeypatch, missing
):
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("a request must not be built at all")

    client, async_client = remote_client(handler, **{missing: ""})

    with pytest.raises(TranscriptionUnavailable):
        run_remote(client, async_client, monkeypatch)


# --- the real weights, on request only -------------------------------------


def silence_wav(seconds: float) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(SAMPLE_RATE)
        handle.writeframes(b"\x00\x00" * int(seconds * SAMPLE_RATE))
    return buffer.getvalue()


@pytest.mark.skipif(
    not os.getenv("AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST"),
    reason=(
        "loads real Whisper weights; run it explicitly with "
        "AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST=/models/whisper/small"
    ),
)
def test_the_real_model_loads_and_decodes():
    """The one test that touches the weights volume and PyAV. Skipped by
    default — the suite must stay a suite — and run by hand after a change to
    the image, the mount or the model directory."""
    path = os.environ["AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST"]
    transcription._model = None  # this test owns the process-wide slot
    try:
        model = transcription.load_transcription_model(path=path)
        assert {"ru", "uk", "en"} <= set(model.supported_languages)
        client = LocalTranscriber(model=model)
        # Silence is what the VAD exists to skip, so the documented "no
        # transcript" failure is the right answer — and reaching it proves
        # the decoder, the VAD and the model all ran.
        with pytest.raises(TranscriptionUnavailable, match="no transcript"):
            client.transcribe(silence_wav(2.0), "audio/wav", "ru-RU")
    finally:
        transcription._model = None
