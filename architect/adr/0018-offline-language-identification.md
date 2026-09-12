# ADR 0018: Offline language identification abstains before response routing

Status: accepted (2026-09-13).
Ticket: ClickUp 86cbehk24.
Evaluation: BibleGarden/AI-Evaluation@ec757fe.

## Context

The hand-written detector in `safety.py` distinguished Russian, Ukrainian and
English with alphabet checks and short function-word lists. Every Latin-script
message was labelled English. That made Spanish, Polish and Portuguese select
the English question prompt and could select the English fixed safety reply.

The detector has two consumers with narrower response policies. The question
endpoint has complete prompts for `ru`, `uk` and `en`; every other code and an
unknown language use the universal prompt. The fixed safety response also has
texts for `ru`, `uk` and `en`, with English as its worldwide fallback. Language
identification must not silently expand or change either policy.

Lingua, fastText and py3langid were measured on 24 scenarios, 13 probes, all
139 parametrized safety examples and routing-specific inputs. The reproducible
report and raw rows are in AI-Evaluation. Unrestricted Lingua confidently
misclassified Ukrainian as Kazakh; fastText conflicted with NumPy 2 and had
Ukrainian regressions. py3langid with normalized probabilities and a `0.9`
threshold preserved the routing invariants: no localized prompt disagreement,
all 57 tier-1 and all 25 tier-2 safety languages correct when the tier-2
pattern is allowed to resolve an otherwise unknown language.

## Decision

`app/language_detection.py` owns one eagerly loaded
`LanguageIdentifier.from_model_file(MODEL_FILE, norm_probs=True)` from
`py3langid==0.4.0`. The bundled model covers 139 languages and performs no
network inference. Inputs with no letters return `None` before classification.
For other input, the normalized top result is returned as its ISO code only at
probability `>= 0.9`; `und`, `zxx` and lower probabilities return `None`.

The identifier is a process singleton. `classify` creates its feature and score
arrays per call and only reads the loaded model arrays, so concurrent request
threads share it without mutable inference state. Loading is eager: a missing
package or corrupt bundled model prevents application startup rather than
silently falling back to the removed heuristic.

`safety.detect_language` re-exports the shared function for compatibility.
`language_source` keeps its existing order: last user reply, topic, earlier
user replies newest first, then the last assistant message only when the person
wrote nothing, then the empty English default. An abstention lets that walk
continue; a detected unsupported language is still evidence and stops it.

Prompt routing remains unchanged: `ru`, `uk` and `en` select complete localized
prompts; unsupported codes and `None` select the universal prompt. Fixed reply
routing remains `ru`/`uk`/`en`, with English for other known codes.

Maria approved one tier-2 evidence rule on 2026-09-13. Known conversation
language remains authoritative. When its detection is `None`, tier 2 uses the
language already carried by the matched safety pattern, exactly as tier 1 has
always done:

```python
reply_language = detect_language(language_text) or guard.language
```

## Consequences

Short or ambiguous messages more often use prior context or the universal
prompt instead of receiving a confident guess. Spanish, Polish and Portuguese
long-form inputs are identified as themselves and therefore use the universal
prompt rather than the English localized prompt. Adding a localized prompt or
fixed safety response remains a separate reviewed product change.

The dependency adds a bundled statistical model to the image and resident
memory to every API process even when AI providers are unavailable. The branch
image measurement adds 4,642,352 bytes, of which the compressed bundled model
is 4,586,720 bytes. Importing the full application measured 78,824 KiB RSS
before and 153,740 KiB after, a 74,916 KiB increase. Model loading temporarily
writes a 68,312,620-byte decompressed NPZ through Python's temporary-file API;
production must therefore keep enough temporary filesystem space at startup.
The choice trades that bounded cost for measured abstention and offline
operation.
