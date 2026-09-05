# ADR 0012: Transcription providers — Whisper instead of Gemini

Status: accepted (2026-09-05).
Ticket: ClickUp 86cbegg3m (step 6 of the local-models umbrella 86cbe4mtq).
Extends ADR 0009 (a provider per stage) to the one stage it deliberately left
out, and follows ADR 0010's shape for a model that runs on our own hardware.

## Context

`POST /api/ai/transcribe` was the last Gemini-only call in the service. ADR
0009 moved the three chat stages to a named provider and said so explicitly:
speech is not the chat protocol, `AI_TRANSCRIBE_PROVIDER` did not exist, and
setting it aborted the start rather than pretending to work.

The umbrella's reason is not quality and not cost: the app must be usable by
anyone, of any age, in any country, and every external provider's terms fail
at least one of those (monorepo `CLAUDE.md`, Maria's decision of 2026-09-04).
Transcription is also the most privacy-loaded call the service makes — it is
someone's voice, praying — so it is the one where "the recording never leaves
our machines" is worth the most.

Whisper is the proven local answer; the open questions were speed on a CPU and
quality on Russian and Ukrainian. Both were measured on this 8-core host
(`evaluation/README.md`, "Локальная расшифровка речи", 2026-09-05):
faster-whisper `small` and `medium`, int8, beam 1 and 5, against Gemini on the
same 10-60 s excerpts of ru/uk/en Bible audio with the verse text as the
reference.

Then the constraint changed, mid-ticket. Maria, 2026-09-05: the company's
admins will run Whisper on the **CPU of the Qwen server** (i9-14900KS, 24
cores / 32 threads, ~127 GB RAM, essentially idle — the GPU is fully taken by
Qwen3-30B and the CPU beside it is not). So the production answer is not this
VM's CPU: it is a Whisper server inside the company, reached over an
OpenAI-compatible audio API. That machine can hold `large-v3` without any of
the memory arithmetic a 2-4 GB production VM forces.

## Decision

### 1. `AI_TRANSCRIBE_PROVIDER` ∈ `gemini` | `openai_compat` | `local`

Required as soon as the AI surface is configured at all — the same rule as
the three chat providers, so an `.env` that predates this change does not
start and the error names the variable. Which provider hears a person's voice
is exactly the class of decision ADR 0008 forbids defaulting in code.

The value set is this stage's own, and that is why it is not a fourth element
of `AI_PROVIDER_VARS`' chat list:

- **`openai_compat` — the production provider.** A multipart
  `POST {endpoint}/audio/transcriptions` (`file`, `model`,
  `response_format=json`, `temperature=0`, and `language` only when the locale
  names a language Whisper knows). This is the OpenAI audio API that vLLM,
  speaches and faster-whisper-server all expose, so the server can be replaced
  without touching this code. Endpoint and key resolve through the existing
  `config.resolve_stage`, including the per-stage overrides
  `AI_TRANSCRIBE_ENDPOINT` / `AI_TRANSCRIBE_API_KEY` — which matter here and
  not merely for symmetry: the audio server is a different process from the
  chat one and will be on a different port.
- **`local` — the fallback.** faster-whisper (CTranslate2) in this process,
  on this CPU: `AI_TRANSCRIBE_MODEL` (`small`, `medium`) plus
  `AI_TRANSCRIBE_MODEL_PATH`, no network and no key at all. It is what the
  measurement ran on, what this machine can serve with, and the answer if the
  company server is ever unavailable for longer than an outage.
- **`gemini` — unchanged**, byte for byte the call this endpoint always made,
  down to its prompt. It stays because the app is in the App Store review
  pipeline and switching providers must be an `.env` edit, not a release.

`AI_TRANSCRIBE_MODEL` is now required by any of the three (a Gemini key, or
`local`, or `openai_compat`) and means the model **identity** in each: a
Gemini model id, the name the audio server expects
(`Systran/faster-whisper-large-v3`), or which Whisper the weights at
`AI_TRANSCRIBE_MODEL_PATH` are. The path/identity split is ADR 0010's, for
ADR 0010's reason: where the bytes live on this machine is not a fact about
the model, and a report that cannot name the model is worthless.
`AI_TRANSCRIBE_MODEL_PATH` set beside a remote provider is a startup error,
not a harmless leftover — it states something false about the deployment.

### 2. One seam, one contract, one failure

`twinkler_ai.transcribe` dispatches on the provider and nothing else changes:
the handler, the 413/415/422/429/502/503 mapping, the response shape, the
14 MiB cap, the MIME handling and the rate limiter are exactly as they were.
Every provider raises into the same `AIError` → `502 AI service unavailable`,
so a client cannot tell which one answered, and no error text ever carries the
recording, the transcript, the endpoint or the key.

The one behavioural difference between the providers, and it is deliberate:
`local` refuses a recording longer than `AI_TRANSCRIBE_MAX_AUDIO_SECONDS`
(600) with that same `502`, where the two remote providers would transcribe
it. It is a property of the machine, not of the API — a recording that long
is minutes of uninterruptible CPU taken from every other request — and it
sits far above anything the app records (the 14 MiB cap is what the client
meets first for a spoken reply). It is a `502` and not a `413` on purpose:
`413` is the promise the endpoint makes about the *upload*, which is 14 MiB
whoever transcribes, and a second, provider-dependent size rule under the
same status would be the more confusing answer.

The contract of `architect/twinkler-ai.md` is unchanged and is now enforced by
construction rather than by an instruction: **verbatim, in the recording's own
language** is `task="transcribe"` (never `translate`) with `temperature=0` and
no prompt at all on both Whisper paths — there is nothing for a sampled token
to invent. The **locale is a weak hint**: its primary subtag becomes Whisper's
`language=` when the model knows that language, and is dropped otherwise. A
phone set to a language Whisper cannot name gets auto-detection, never a
refusal.

### 3. The local provider is shaped like the local embeddings

- **Loaded once, at start-up, fatally** (`app/main.py`), from a read-only
  volume, `local_files_only=True` and `HF_HUB_OFFLINE=1`. Lazy loading would
  answer the first voice message with a 502 that looks like a provider being
  briefly down.
- **`int8` on CPU**, threads and beam size as operational knobs
  (`AI_TRANSCRIBE_COMPUTE_TYPE`, `AI_TRANSCRIBE_THREADS`,
  `AI_TRANSCRIBE_BEAM_SIZE`) whose defaults are the measured operating point.
  The compute type is validated against a reviewed list, so a typo is one
  aggregated `ConfigError` instead of a crash inside the loader.
- **Serialised by a process-wide lock.** CTranslate2 already spreads one
  transcription across the cores; two at once on a box that also runs MySQL
  would only oversubscribe it.
- **Run on a worker thread** (`run_in_threadpool`): the endpoint is `async`
  and a transcription is seconds of arithmetic, which on the event loop would
  stall every other request in the process.
- **The upload is decoded before the model sees it** (PyAV, via
  `faster_whisper.decode_audio`), which is the only way to know the duration
  in advance — and `AI_TRANSCRIBE_MAX_AUDIO_SECONDS` (600) refuses anything
  longer *before* any work starts. The 14 MiB upload cap is a Gemini
  request-size limit and lets ~30 minutes of 64 kbps AAC through; locally that
  would be ~30 minutes of CPU nobody can interrupt, taken from every other
  request on the box.
- **The ceiling is honest about what it can do.** On `gemini` and
  `openai_compat`, `AI_TRANSCRIBE_TIMEOUT_SECONDS` (60, the literal it
  replaces) bounds the HTTP call. On `local` there is no call to time out:
  CTranslate2 offers no cancellation and anyio's thread pool waits for its
  thread even when the awaiting task is cancelled, so a `wait_for` here would
  free nobody. A run that outlives the ceiling is therefore *reported* — a
  `WARNING` saying this machine is too slow for this model — and the duration
  cap above is what actually bounds the work.

### 4. The remote provider reuses the retry discipline, not the protocol

`RemoteTranscriber` borrows `app/gemini_retry.py` exactly as `llm_client`
does: `provider_timeout` carves the ceiling across httpx's four phases (a bare
number is per phase and would authorise four times it), `retry_pause` plans
the backoff, and `RETRYABLE_STATUS` decides what is worth a second attempt.
**Two attempts, not three**: a transcription is the longest call this service
makes and a person is waiting in front of it with nothing on screen, so the
ladder buys one recovery from a restarting server rather than three times the
worst-case wait. Transport failures are re-raised `from None` with their
category only, because an httpx message quotes the request URL.

## Consequences

- **Image**: +320 MB for `faster-whisper` + `ctranslate2` + `av` +
  `onnxruntime` (2.45 GB → 2.77 GB measured). None of it is CUDA and none of
  it is torch: CTranslate2 is its own inference engine and PyAV carries
  ffmpeg's libraries, so no system ffmpeg is needed either. They are installed
  under the same `-c requirements-torch.txt` constraint as everything else.
  A deployment on `openai_compat` or `gemini` pays the image size and imports
  none of it — the import is inside the loader.
- **Speed is not the constraint; memory and quality are.** Measured on this
  8-core host over 15 excerpts of 17-53 s (`evaluation/README.md`), int8,
  language given, VAD on:

  | configuration | WER ru | WER uk | WER en | time / audio (max) | peak RSS |
  |---|---|---|---|---|---|
  | `small`, beam 1 | 0.153 | 0.129 | 0.003 | 0.08x | 849 MB |
  | `small`, beam 5 | 0.138 | 0.125 | 0.002 | 0.13x | 913 MB |
  | `medium`, beam 1 | 0.100 | 0.097 | 0.000 | 0.22x | 2109 MB |
  | `medium`, beam 5 | 0.094 | 0.087 | 0.002 | 0.36x | 2344 MB |
  | Gemini 3.5-flash-lite | 0.019 | 0.051 | 0.000 | 0.74x (network) | — |

  Every configuration clears the ticket's ≤ 1.5x target with a 4-20x margin,
  and threads saturate at ~4 (8 threads measured *slower* than 4 on this box,
  which also runs bge-m3; 4 → 2 costs ~+45%, not 2x). So a small CPU VM could
  serve `small` comfortably — but `medium` peaks at 2.1-2.3 GB on top of the
  2.13 GiB of bge-m3 in the same process, and on Russian even `medium` is 5x
  Gemini's word error rate. That pair of facts is the case for the remote
  provider: the company machine can run `large-v3` without either constraint.
- **The remote provider is measured, not assumed** (review of this ticket,
  2026-09-05, against the live endpoint the admins raised: **speaches** on the
  company CPU at `https://llm.ai2.ru/whisper/v1` — from this machine through
  the tunnel at `https://llm.ai2.ru:8443/whisper/v1` — model
  `deepdml/faster-whisper-large-v3-turbo-ct2`, `Authorization: Bearer`, a
  14 MB limit at their nginx). The same 15 excerpts, driven through
  `RemoteTranscriber` itself (`transcribe_bench.py remote`), 15/15 answered:

  | | WER ru | WER uk | WER en | WER all | CER ru | time / audio (mean, max) |
  |---|---|---|---|---|---|---|
  | `large-v3-turbo`, company CPU | **0.037** | 0.059 | 0.002 | **0.033** | **0.003** | 0.20x, 0.42x |
  | `medium`, this VM (the fallback) | 0.100 | 0.097 | 0.000 | 0.066 | 0.017 | 0.18x, 0.22x |
  | Gemini 3.5-flash-lite | 0.019 | 0.051 | 0.000 | 0.023 | 0.003 | 0.12x, 0.74x |

  So "quality is not a constraint there" holds in the sense it was decided in
  — Russian is 2.7x better than the best local option and its **character**
  error rate equals Gemini's, so what remains is inflection and spelling, not
  lost meaning — but it is not "better than Gemini": Gemini still leads on
  ru/uk WER on this studio corpus. Our own memory cost is zero: the API
  container stayed at **75 MB RSS** through the run.
- The acceptance criterion Maria set is her own reading of the 15 Russian
  transcripts, not the WER; they are side by side in `evaluation/README.md`,
  the remote provider among them. The corpus is studio-read Scripture, so
  these numbers are a floor for every provider, Gemini included — spontaneous
  prayer speech is harder for all of them.
- **What production still needs is the key and the route, not the code.** The
  endpoint, the port and the model name are known and measured (above); the
  production key (`bible-api-prod`, Passbolt) and the production VM's place in
  the server's IP allow-list are deploy items.

## Alternatives considered

- **Local Whisper on the production VM (the ticket's original plan).** Still
  supported and still the fallback, but it was chosen when the only CPU
  available was this project's own. The company machine is 24 idle cores next
  to the model the rest of the pipeline already talks to; putting Whisper on
  the API VM instead would spend the API's memory and CPU on it for worse
  quality (`small`/`medium` rather than `large-v3`).
- **A separate `whisper` container on the API VM** (the ticket's third
  bullet). It decouples the API's memory from the model and would have been
  the answer if `local` had to serve production — but it is a second stateful
  service on a small VM, and the remote provider gives the same decoupling
  with none of it. The compose service is not added; if it is ever wanted, it
  is the `openai_compat` provider pointed at `http://whisper:8000/v1`, with no
  code change at all. That is the strongest argument for having built the
  remote path.
- **Keeping Gemini for speech alone.** It is the smallest diff and the best
  quality per second, and it leaves a Google call carrying a person's voice —
  the one call the umbrella most wants gone.
- **A `wait_for` around the local run.** Rejected as theatre: anyio's thread
  pool waits for the thread, so the client would not be freed and the CPU
  would keep burning. Bounding the accepted audio duration is the honest
  version of the same intent.

## Open questions

1. ~~The production model, endpoint and quality are unknown.~~ **Answered
   2026-09-05**: `deepdml/faster-whisper-large-v3-turbo-ct2` on speaches at
   `https://llm.ai2.ru/whisper/v1`, measured above. What is still open is the
   **production key** (`bible-api-prod`, Passbolt — the review used a separate
   test key, read into the process environment and never written to a file)
   and whether the production VM is in that server's IP allow-list. Re-measure
   with `transcribe_bench.py remote` whenever the admins change the model
   behind that URL.
2. Whether the audio server should be reachable only over the company network
   (it should) and how — VPN, ssh tunnel like `qwen-tunnel.service`, or an
   allow-list — is a deployment decision, not a code one. Until it is
   answered, a recording travels to it with a bearer key over TLS. (This
   machine already reaches it through the tunnel, port 8443.)
3. `AI_TRANSCRIBE_TIMEOUT_SECONDS` = 60 was Gemini's ceiling. The measured
   sequential calls take 3.9-13.9 s for 17-53 s of audio, so 60 s is ample on
   an idle server; whether it is right for a **queued** one under concurrent
   load is still unmeasured.
4. The mobile app has no "transcription failed" affordance beyond the generic
   error: with three providers and a machine that may be busy, a person may
   see a 502 more often than with Gemini. Product question, Maria's.
