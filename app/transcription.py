"""
Speech transcription away from Gemini: the two Whisper providers (ADR 0012).

`POST /api/ai/transcribe` chooses one of three transports with
`AI_TRANSCRIBE_PROVIDER`. The Gemini one stays where it always was, in
`twinkler_ai`; the two here are:

- `openai_compat` — `RemoteTranscriber`, a multipart
  `POST {endpoint}/audio/transcriptions` against an OpenAI-compatible **audio**
  server (vLLM, speaches, faster-whisper-server all expose it). This is the
  production provider: Maria's decision of 2026-09-05 is to run Whisper on the
  CPU of the company's model server, whose GPU is fully taken by Qwen while 24
  idle cores sit beside it — so the recording leaves this VM but stays inside
  the company, and the model may be as large as quality wants.
- `local` — `LocalTranscriber`, faster-whisper (CTranslate2) in THIS process
  on this CPU. The fallback: no network at all, and what the measurement of
  ClickUp 86cbegg3m ran on.

Both raise `TranscriptionUnavailable`, which the seam turns into the endpoint's
documented `502`, so no client learns which transport answered.

`LocalTranscriber` is the same shape as the local embedding client of ADR
0010, and for the same reasons:

- the weights are loaded **once per process, at start-up**, from a read-only
  volume (`AI_TRANSCRIBE_MODEL_PATH`) — never lazily, or a missing volume
  would look, from outside, like a provider being briefly down;
- a load failure is fatal at start-up rather than a 502 per request;
- `transcribe` is serialised by a **process-wide lock**: the model is one
  process-wide object and CTranslate2 already spreads one transcription
  across the cores, so two concurrent runs on an 8-core box that also runs
  MySQL would only oversubscribe it;
- nothing here reaches the network, and the image runs with
  `HF_HUB_OFFLINE=1`, so the offline promise is structural.

The contract is `architect/twinkler-ai.md`'s and does not change with the
provider: the recording is transcribed **verbatim in its original language**
(`task="transcribe"`, never `translate`) and the app locale is a **weak hint**
only — it becomes Whisper's `language=` when it names a language the model
knows, and nothing at all otherwise (auto-detection).

Nothing in this module logs the audio, the transcript, or anything derived
from either: durations, languages and timings only.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

import httpx

from config import (
    AI_TRANSCRIBE_BEAM_SIZE,
    AI_TRANSCRIBE_COMPUTE_TYPE,
    AI_TRANSCRIBE_MAX_AUDIO_SECONDS,
    AI_TRANSCRIBE_MODEL_PATH,
    AI_TRANSCRIBE_THREADS,
    AI_TRANSCRIBE_TIMEOUT_SECONDS,
)
from deadline import Deadline
from gemini_retry import RETRYABLE_STATUS, provider_timeout, retry_pause
from llm_client import transport_error

logger = logging.getLogger(__name__)

# The sample rate Whisper's front end works at. It is the model's, not a
# preference: `decode_audio` resamples whatever the container holds to it,
# and the duration of the decoded signal is measured in these samples.
SAMPLE_RATE = 16000

# Whisper's front end pads or trims every window to 30 s, so anything shorter
# than that costs one window whatever it is. Silence is skipped by the VAD
# (`vad_filter=True`) rather than transcribed, which is what keeps a recording
# that is mostly a quiet room from producing invented sentences.
VAD_FILTER = True

# Locales whose primary subtag is not a language code Whisper knows. The first
# three are ISO 639-1's own deprecated spellings, which some platforms still
# emit; `nb` (Bokmål) is Whisper's `no`, while `nn` (Nynorsk) is its own code
# and needs no alias.
LOCALE_LANGUAGE_ALIASES = {
    "iw": "he",
    "in": "id",
    "ji": "yi",
    "nb": "no",
}

# The 100 language codes every multilingual Whisper checkpoint knows
# (`faster_whisper.tokenizer._LANGUAGE_CODES`, copied because it is private
# and because the REMOTE provider has no model object to ask). It is the
# vocabulary of the model family, not of a deployment, so it is a constant.
# The local transcriber intersects it with the loaded model's own list, which
# is how an English-only checkpoint (`small.en`) still rejects `language=ru`.
WHISPER_LANGUAGES = frozenset({
    "af", "am", "ar", "as", "az", "ba", "be", "bg", "bn", "bo", "br",
    "bs", "ca", "cs", "cy", "da", "de", "el", "en", "es", "et", "eu",
    "fa", "fi", "fo", "fr", "gl", "gu", "ha", "haw", "he", "hi", "hr",
    "ht", "hu", "hy", "id", "is", "it", "ja", "jw", "ka", "kk", "km",
    "kn", "ko", "la", "lb", "ln", "lo", "lt", "lv", "mg", "mi", "mk",
    "ml", "mn", "mr", "ms", "mt", "my", "ne", "nl", "nn", "no", "oc",
    "pa", "pl", "ps", "pt", "ro", "ru", "sa", "sd", "si", "sk", "sl",
    "sn", "so", "sq", "sr", "su", "sv", "sw", "ta", "te", "tg", "th",
    "tk", "tl", "tr", "tt", "uk", "ur", "uz", "vi", "yi", "yo", "yue",
    "zh",
})


class TranscriptionUnavailable(RuntimeError):
    """The local speech model is not configured, not loadable, or refused.

    The provider-independent seam in `twinkler_ai.transcribe` turns it into
    the same `AIError` the Gemini path raises, so the endpoint answers its
    documented `502 AI service unavailable` whichever provider served it and
    no caller learns which one did.
    """


_model_lock = threading.Lock()
_model = None
# Serialises `transcribe`. Module-level and not per client on purpose: the
# weights are ONE process-wide object (`_model`), so two transcribers built
# around it queue on the same lock instead of running concurrent CPU
# transcriptions on the same model.
_transcribe_lock = threading.Lock()


def load_transcription_model(
    path: str = AI_TRANSCRIBE_MODEL_PATH,
    compute_type: str = AI_TRANSCRIBE_COMPUTE_TYPE,
    cpu_threads: int = AI_TRANSCRIBE_THREADS,
):
    """The process-wide faster-whisper model, loaded exactly once.

    The directory is `AI_TRANSCRIBE_MODEL_PATH`, a read-only volume holding a
    CTranslate2 conversion of a Whisper model — never a hub id, so nothing can
    be downloaded on a machine that may have no route to the internet
    (`local_files_only=True` here, `HF_HUB_OFFLINE=1` in the image).

    `compute_type="int8"` is a quantisation applied while loading, so the
    float16 conversion published on the hub is what the volume holds and the
    memory it occupies is the int8 one. The load is idempotent and
    thread-safe; `app/main.py` calls it at start-up so the cost — and any
    failure — happens there rather than inside the first prayer.
    """
    global _model
    with _model_lock:
        if _model is not None:
            return _model
        if not path:
            raise TranscriptionUnavailable(
                "AI_TRANSCRIBE_MODEL_PATH is not configured"
            )
        # Imported here, not at module import time: a deployment on `gemini`
        # must not pay for ctranslate2 and PyAV, and the test suite must be
        # able to import this module without faster-whisper installed at all.
        from faster_whisper import WhisperModel

        started = time.time()
        try:
            model = WhisperModel(
                path,
                device="cpu",
                compute_type=compute_type,
                cpu_threads=cpu_threads,
                local_files_only=True,
            )
        except Exception as exc:
            # Type only: the message of a loader exception can quote paths and
            # library internals that end up in a startup banner.
            raise TranscriptionUnavailable(
                f"cannot load the transcription model from {path!r}: "
                f"{type(exc).__name__}"
            ) from exc
        logger.info(
            "Local transcription model loaded from %s "
            "(compute_type=%s, cpu_threads=%s, beam_size=%s) in %.1f s",
            path,
            compute_type,
            cpu_threads or "all",
            AI_TRANSCRIBE_BEAM_SIZE,
            time.time() - started,
        )
        _model = model
        return _model


def whisper_language(
    locale: str | None, supported=WHISPER_LANGUAGES
) -> str | None:
    """The app locale as a Whisper language hint, or `None` for auto-detect.

    `ru-RU` -> `ru`, `en-GB` -> `en`, `zh-Hant-TW` -> `zh`: the primary
    subtag, lower-cased, past the deprecated spellings above. A locale whose
    language this model does not know becomes `None` rather than an error —
    the contract calls the locale a **weak hint**, and refusing a recording
    because the phone is set to a language Whisper cannot name would be a far
    worse answer than transcribing it with auto-detection.

    `supported` is the model's own set of language codes, so this stays a
    pure function of its two arguments and the tests need no weights.
    """
    if not locale:
        return None
    primary = locale.split("-", 1)[0].strip().lower()
    if not primary:
        return None
    primary = LOCALE_LANGUAGE_ALIASES.get(primary, primary)
    return primary if primary in supported else None


class LocalTranscriber:
    """`twinkler_ai.transcribe`'s Gemini path, served by Whisper in-process.

    One method, the same three arguments and the same public failure. The
    MIME type is accepted and not used: the container is sniffed by libav
    while decoding, so the type the client declared cannot make the bytes
    something else — but the argument stays, because both providers must have
    one signature for the seam to be a seam.
    """

    def __init__(
        self,
        model=None,
        beam_size: int = AI_TRANSCRIBE_BEAM_SIZE,
        max_audio_seconds: float = AI_TRANSCRIBE_MAX_AUDIO_SECONDS,
        slow_after_seconds: float = AI_TRANSCRIBE_TIMEOUT_SECONDS,
    ):
        self._model = model if model is not None else load_transcription_model()
        self.beam_size = max(1, beam_size)
        self.max_audio_seconds = max_audio_seconds
        self.slow_after_seconds = slow_after_seconds
        self._lock = _transcribe_lock

    def _supported_languages(self) -> frozenset[str]:
        own = frozenset(getattr(self._model, "supported_languages", ()) or ())
        return own & WHISPER_LANGUAGES if own else WHISPER_LANGUAGES

    def _decode(self, audio: bytes):
        """The upload as a 16 kHz mono waveform, or TranscriptionUnavailable.

        Decoded here rather than inside `WhisperModel.transcribe` (which
        accepts the raw bytes and would do exactly this) for one reason: the
        duration is knowable only after the decode, and a recording longer
        than this deployment accepts must be refused **before** the model
        starts on it. The decode itself is cheap next to the transcription.
        """
        import io

        from faster_whisper import decode_audio

        try:
            return decode_audio(io.BytesIO(audio), sampling_rate=SAMPLE_RATE)
        except Exception as exc:
            # Type only, for the same reason the Gemini path says nothing
            # about the recording: this message is logged.
            raise TranscriptionUnavailable(
                f"cannot decode the recording: {type(exc).__name__}"
            ) from exc

    def transcribe(
        self, audio: bytes, mime_type: str, locale: str | None
    ) -> str:
        """One recording, transcribed verbatim. Blocking and CPU-bound.

        Called from a worker thread (`run_in_threadpool` in the seam), never
        on the event loop: a transcription is seconds of arithmetic, and on
        the loop it would stall every other request in the process.
        """
        waveform = self._decode(audio)
        duration = len(waveform) / SAMPLE_RATE
        if duration > self.max_audio_seconds:
            raise TranscriptionUnavailable(
                f"the recording is {duration:.0f} s long, longer than the "
                f"{self.max_audio_seconds:.0f} s this deployment transcribes "
                f"locally (AI_TRANSCRIBE_MAX_AUDIO_SECONDS)"
            )
        language = whisper_language(locale, self._supported_languages())
        started = time.time()
        try:
            with self._lock:
                segments, info = self._model.transcribe(
                    waveform,
                    task="transcribe",
                    language=language,
                    beam_size=self.beam_size,
                    vad_filter=VAD_FILTER,
                )
                # `segments` is a generator: the work happens while it is
                # consumed, so it is consumed inside the lock.
                text = " ".join(
                    segment.text.strip()
                    for segment in segments
                    if segment.text and segment.text.strip()
                ).strip()
        except TranscriptionUnavailable:
            raise
        except Exception as exc:
            raise TranscriptionUnavailable(
                f"local transcription failed: {type(exc).__name__}"
            ) from exc
        elapsed = time.time() - started
        detected = getattr(info, "language", None) if info is not None else None
        # Never the transcript, and never a length that hints at it: how long
        # the recording was, which language answered, and how long the CPU
        # took. `grep 'Local transcription'` is how the ratio is checked on a
        # new VM.
        logger.info(
            "Local transcription: audio=%.1fs cpu=%.1fs ratio=%.2f "
            "hint=%s detected=%s",
            duration,
            elapsed,
            elapsed / duration if duration else 0.0,
            language or "<none>",
            detected or "<unknown>",
        )
        if elapsed > self.slow_after_seconds:
            # The one place the ceiling can act on the local provider: the run
            # cannot be cancelled (CTranslate2 has no cancellation, and
            # anyio's thread pool waits for its thread even when the awaiting
            # task is cancelled), so it is reported instead of enforced. A
            # deployment that sees this line is on too big a model, or too few
            # cores, for the recordings it gets.
            logger.warning(
                "Local transcription took %.1f s for %.1f s of audio, over "
                "the %.0f s ceiling (AI_TRANSCRIBE_TIMEOUT_SECONDS): this "
                "machine is too slow for AI_TRANSCRIBE_MODEL",
                elapsed,
                duration,
                self.slow_after_seconds,
            )
        if not text:
            raise TranscriptionUnavailable("the model returned no transcript")
        return text


# ---------------------------------------------------------------------------
# The remote provider: the OpenAI audio API (AI_TRANSCRIBE_PROVIDER=openai_compat)
# ---------------------------------------------------------------------------

# Filename sent in the multipart part. Servers built on the OpenAI shape read
# the EXTENSION to pick a decoder, so it must match the container the app
# uploads — and it must carry nothing of the person's own filename, which this
# service never learns and never forwards.
_UPLOAD_FILENAMES = {
    "audio/mp4": "recording.m4a",
    "audio/x-m4a": "recording.m4a",
    "audio/m4a": "recording.m4a",
}
_DEFAULT_UPLOAD_FILENAME = "recording.m4a"

# Linear backoff, the same ladder `llm_client` uses: 2 s before the second
# attempt, 4 s before the third — and only when the budget still affords the
# attempt after it.
_RETRY_BASE_SECONDS = 2.0


def transcriptions_url(endpoint: str) -> str:
    """`https://host/v1` -> `https://host/v1/audio/transcriptions`.

    An endpoint that already names the method is left alone, so both
    spellings can be configured — exactly what `llm_client.completions_url`
    does for the chat protocol.
    """
    base = endpoint.rstrip("/")
    if base.endswith("/audio/transcriptions"):
        return base
    return f"{base}/audio/transcriptions"


def bearer_headers(api_key: str) -> dict[str, str]:
    """`Authorization` alone, or nothing at all.

    `llm_client.auth_headers` is deliberately not reused: it also sets
    `Content-Type: application/json`, and a multipart body's content type is
    httpx's to write (it carries the boundary). The rule about the key is the
    same one — an empty key is the explicit "this endpoint is
    unauthenticated", and sending `Bearer ` would be a different, wrong
    request.
    """
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


class RemoteTranscriber:
    """Whisper on someone else's CPU, through the OpenAI audio API.

    One multipart POST per recording: `file`, `model`, `response_format=json`,
    `temperature=0` and — only when the locale names a language Whisper knows
    — `language`. The answer is `{"text": "..."}`.

    `temperature=0` and the absence of any prompt are the contract, not a
    preference: this endpoint returns a **verbatim** transcript in the
    original language, so there is no instruction to disobey and nothing for
    a sampled token to invent. (The Gemini path needs a written instruction
    for the same guarantee, because it drives a general chat model.)

    Retry discipline is `app/gemini_retry.py`'s, exactly as `llm_client`
    reuses it: retryable statuses, a linear backoff that refuses to sleep
    unless the attempt after it still fits, and `provider_timeout` carving one
    call's ceiling across httpx's four phases (a bare number would authorise
    four times it).

    **`AI_TRANSCRIBE_TIMEOUT_SECONDS` bounds the WHOLE call, retries and
    backoff included** (ClickUp 86cbegg3w). It used to bound each attempt
    separately, so a server that accepted the connection and then answered
    nothing — the ordinary "process up, app dead" outage — was waited out
    twice with a backoff in between: measured **116.1 s** against a
    documented 60 s ceiling, on a request a person is watching a spinner
    for. A per-call `Deadline` (the scripture endpoint's mechanism, ADR
    0006) now owns the budget: `provider_timeout` takes the minimum of the
    ceiling and what is left, and `retry_pause` refuses a backoff whose
    attempt would no longer fit. The recovery the second attempt exists for
    is untouched — a server that *fails fast* (a 503 while restarting) still
    leaves most of the budget, so the retry happens exactly where it can
    help.

    Never logged and never quoted in an error: the recording, the transcript,
    the key, and the endpoint URL (an httpx message carries it, which is why
    every transport failure is re-raised `from None` with its category only).
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        model: str,
        timeout: float = AI_TRANSCRIBE_TIMEOUT_SECONDS,
        attempts: int = 2,
        sleep=asyncio.sleep,
        clock=time.monotonic,
    ):
        self.endpoint = endpoint
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        # Two, not three: a transcription is the longest call this service
        # makes and a person is waiting in front of it with nothing on
        # screen, so the ladder buys one recovery from a restarting server
        # rather than three times the worst-case wait.
        self.attempts = max(1, attempts)
        self._sleep = sleep
        # Injectable for the same reason `sleep` is: the wall-clock ceiling
        # is asserted on a fake clock (tests/test_transcription.py), never by
        # waiting a real minute.
        self._clock = clock

    def _check_configured(self) -> None:
        """Unreachable in a started service (`config._validate` refuses an
        incomplete stage), but a CLI or a test that bypasses config must fail
        loudly instead of posting a recording to an empty URL."""
        if not self.endpoint:
            raise TranscriptionUnavailable(
                "the transcription endpoint is not configured"
            )
        if not self.model:
            raise TranscriptionUnavailable(
                "AI_TRANSCRIBE_MODEL is not configured"
            )

    def _request(self, audio: bytes, mime_type: str, locale: str | None):
        self._check_configured()
        filename = _UPLOAD_FILENAMES.get(mime_type, _DEFAULT_UPLOAD_FILENAME)
        files = {"file": (filename, audio, mime_type or "application/octet-stream")}
        data = {
            "model": self.model,
            "response_format": "json",
            "temperature": "0",
        }
        language = whisper_language(locale)
        if language:
            data["language"] = language
        return transcriptions_url(self.endpoint), files, data, language

    @staticmethod
    def _text_of(response: httpx.Response) -> str:
        """The `text` field, or TranscriptionUnavailable naming the shape.

        Never echoes the body: on a server that answers an error as 200 with
        prose, that body is about a recording.
        """
        try:
            payload = response.json()
        except ValueError:
            raise TranscriptionUnavailable(
                "the transcription response is not JSON"
            ) from None
        text = payload.get("text") if isinstance(payload, dict) else None
        if not isinstance(text, str) or not text.strip():
            raise TranscriptionUnavailable(
                "the transcription response has no text"
            )
        return text.strip()

    async def transcribe(
        self, audio: bytes, mime_type: str, locale: str | None
    ) -> str:
        url, files, data, language = self._request(audio, mime_type, locale)
        headers = bearer_headers(self.api_key)
        last_error: Exception | None = None
        started = time.time()
        # ONE budget for the whole call. Every attempt and every pause below
        # is measured against it, so `AI_TRANSCRIBE_TIMEOUT_SECONDS` is the
        # endpoint's promise about its own latency rather than the length of
        # one rung of the ladder.
        deadline = Deadline(self.timeout, clock=self._clock)
        for attempt in range(self.attempts):
            timeout = provider_timeout(deadline, self.timeout)
            if timeout is None:
                # The budget is gone: starting a call that cannot finish
                # inside it would only make the person wait past the ceiling
                # for an answer this request can no longer use.
                break
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(
                        url, files=files, data=data, headers=headers
                    )
                    if response.status_code in RETRYABLE_STATUS:
                        last_error = TranscriptionUnavailable(
                            f"the transcription server answered HTTP "
                            f"{response.status_code}"
                        )
                        if attempt + 1 >= self.attempts:
                            break
                        pause = retry_pause(
                            deadline, _RETRY_BASE_SECONDS * (attempt + 1)
                        )
                        if pause is None:
                            break
                        await self._sleep(pause)
                        continue
                    response.raise_for_status()
                    text = self._text_of(response)
            except TranscriptionUnavailable:
                raise
            except httpx.TimeoutException as exc:
                last_error = exc
                if attempt + 1 >= self.attempts:
                    break
                pause = retry_pause(
                    deadline, _RETRY_BASE_SECONDS * (attempt + 1)
                )
                if pause is None:
                    break
                await self._sleep(pause)
            except (httpx.HTTPError, ValueError) as exc:
                # `from None`: an httpx message quotes the request URL, and
                # that URL is the one value an operator might have pasted a
                # key into despite `config.validate_endpoint`.
                raise TranscriptionUnavailable(
                    f"the transcription request failed: {transport_error(exc)}"
                ) from None
            else:
                # Nothing about the recording or the transcript: how long the
                # call took and which hint was sent.
                logger.info(
                    "Remote transcription: %.1fs hint=%s",
                    time.time() - started,
                    language or "<none>",
                )
                return text
        raise TranscriptionUnavailable(
            "the transcription request failed after retries"
        ) from last_error
