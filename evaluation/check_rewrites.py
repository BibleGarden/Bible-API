#!/usr/bin/env python3
"""
Check a rewrite artifact (`bench_data/*_rewrites_*.json`) without running the
benchmark (ClickUp 86cbegg36).

Three of the acceptance criteria of a rewrite run are properties of the
artifact alone, and reading 126 variants by eye is how they get claimed
instead of measured:

* **Failed records** — a record whose `error` is set: the answer could not be
  parsed, or the call itself did not go through (a quota, a timeout). In
  production either is a `rewrite_failed` degradation to the raw query, so the
  count belongs in every report of a run; the record's `error` string says
  which of the two it was.
* **Answer language** — every scenario's variants must be in the language of
  that scenario. The few-shot examples of prompt v8 are shown in three
  languages, which is exactly where a small model drifts; the detector is
  `check_questions.detect_language` (alphabet first, function words as a
  tie-break), reused rather than re-invented.

  The verdict is taken **per scenario**, over its variants joined, and per
  variant only as a diagnostic. Reason: a query in scriptural register is a
  dozen words with, often, none of ы/э/ъ/ё and none of the everyday function
  words the detector tie-breaks on, so a perfectly good Russian near-quote
  comes back "Cyrillic, undecidable". The Gemini baseline shows 15 such
  variants of 126 — treating them as violations would fail the approved
  production configuration. What IS a violation is a *positive* other
  language: Ukrainian letters in a Russian answer, Latin where Cyrillic was
  asked for. Six variants together decide it in practice; a scenario whose
  whole answer is still undecidable is reported as undecided, not as wrong.
* **Copying of the few-shot examples** — measured against
  `rewrite_prompts.example_queries()`: exact (whitespace-normalised), prefix
  (the first 40 characters of an example) and near (`SequenceMatcher >= 0.85`),
  the same three classes the 86cbea05x package used. **Reported, not gated**:
  the examples quote real, well-known verses, so a model that independently
  recalls Romans 15:13 for a prayer about peace trips this check — the
  `gemini-3.7-flash` baseline does, on a prompt that has no examples at all.
  A rising count is the signal to look at; a nonzero one is not a failure.

Also reported, because they cost nothing here and are asked for in every
package: mean/median variant length, `chapter:verse` leaks inside a query and
short answers (fewer variants than requested).

    python check_rewrites.py bench_data/qwen30b_rewrites_v070_p8app.json [more…]

Exit code is 1 when a hard criterion fails (a failed record, a scenario or
variant in a positively wrong language, a reference leak), so the script can
gate a package instead of only describing it. Reads files only: no network, no
database, no model.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
from difflib import SequenceMatcher
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "app"))

# `rewrite_prompts.example_queries()` reaches the production `_EXAMPLES`, and
# importing `app/query_rewrite.py` imports `app/config.py`, which fails fast on
# a missing deployment variable (ADR 0008). This tool reads files: it never
# opens a connection, never computes a vector and never calls a model, so the
# stubs below only have to let `config` import. Same block, same reason, as
# `gen_rewrites.py` — real values in the environment still win (`setdefault`),
# so running this next to a configured deployment changes nothing.
for _name, _value in (
    ("API_KEY", "check-rewrites-unused"),
    ("DB_HOST", "check-rewrites-unused"),
    ("DB_USER", "check-rewrites-unused"),
    ("DB_PASSWORD", ""),
    ("DB_NAME", "check-rewrites-unused"),
    ("EMBEDDING_MODEL", "gemini-embedding-001"),
    ("EMBEDDING_DIMENSIONS", "768"),
    ("EMBEDDING_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_REWRITE_MODEL", "gemini-3.7-flash"),
    ("AI_SCRIPTURE_RERANK_MODEL", "gemini-3.5-flash-lite"),
    ("AI_QUESTION_MODEL", "gemini-3.5-flash-lite"),
    ("AI_TRANSCRIBE_MODEL", "gemini-3.5-flash-lite"),
    ("AI_QUESTION_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_REWRITE_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_RERANK_PROVIDER", "gemini"),
    # And who transcribes (ADR 0012): a fourth provider variable,
    # required by the same rule. This tool never transcribes anything.
    ("AI_TRANSCRIBE_PROVIDER", "gemini"),
):
    os.environ.setdefault(_name, _value)

from check_questions import detect_language  # noqa: E402

# Length of the example prefix a "copy with a tail" is recognised by, and the
# similarity above which a rewritten example still counts as one — both carried
# over from the 86cbea05x statistics so the numbers stay comparable.
_PREFIX_CHARS = 40
_NEAR_RATIO = 0.85


def _example_queries() -> set[str]:
    import rewrite_prompts  # noqa: WPS433

    return rewrite_prompts.example_queries()


def _reference_leaks(text: str) -> list[str]:
    import rewrite_prompts  # noqa: WPS433

    return rewrite_prompts.find_reference_leaks(text)


def copy_class(variant: str, examples: set[str]) -> str:
    """"exact", "prefix", "near" or "" for one variant against the examples."""
    normalised = " ".join(variant.split())
    if normalised in examples:
        return "exact"
    for example in examples:
        if normalised.startswith(example[:_PREFIX_CHARS]):
            return "prefix"
    for example in examples:
        if SequenceMatcher(None, normalised, example).ratio() >= _NEAR_RATIO:
            return "near"
    return ""


def check(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    meta = payload.get("meta", {})
    records = payload.get("scenarios", [])
    examples = _example_queries()

    failures = [r["id"] for r in records if r.get("error")]
    short = [r["id"] for r in records if r.get("warning")]
    wrong_language: list[str] = []
    undetermined: list[str] = []
    scenario_language_ok = 0
    scenario_language_bad: list[str] = []
    undetermined_scenarios: list[str] = []
    copies: dict[str, list[str]] = {"exact": [], "prefix": [], "near": []}
    leaks: list[str] = []
    lengths: list[int] = []

    for record in records:
        expected = record["language"]
        variants = record.get("variants") or []
        if variants:
            joined = detect_language(" ".join(variants))
            if joined == expected:
                scenario_language_ok += 1
            elif joined in ("cyr?", "?"):
                # Six near-quotes together almost always decide it; when even
                # they do not, the honest answer is "undecided", not "wrong".
                # The same rule as per variant, for the same reason.
                undetermined_scenarios.append(f"{record['id']} ({joined})")
            else:
                scenario_language_bad.append(f"{record['id']} "
                                             f"{expected}->{joined}")
        for number, variant in enumerate(variants, start=1):
            lengths.append(len(variant))
            where = f"{record['id']}#{number}"
            detected = detect_language(variant)
            if detected == expected:
                pass
            elif detected in ("cyr?", "?"):
                # Cyrillic that carries no distinguishing letter or word: not
                # a violation on its own, but it is not a confirmation either.
                undetermined.append(f"{where} ({detected})")
            else:
                wrong_language.append(f"{where} {expected}->{detected}")
            klass = copy_class(variant, examples)
            if klass:
                copies[klass].append(where)
            if _reference_leaks(variant):
                leaks.append(where)

    return {
        "file": path.name,
        "model": meta.get("model", "?"),
        "prompt": f"{meta.get('prompt_version', '?')}"
                  f"r{meta.get('prompt_revision', '?')}",
        "transport": meta.get("transport", "raw?"),
        "partial": meta.get("partial"),
        "records": len(records),
        # A record whose call failed has nothing to check the language of, so
        # it is not in the denominator — it is already counted as a failure.
        "scenarios_answered": sum(1 for r in records if r.get("variants")),
        "variants": len(lengths),
        "json_failures": failures,
        "short_answers": short,
        "scenarios_language_ok": scenario_language_ok,
        "scenarios_language_bad": scenario_language_bad,
        "scenarios_language_undetermined": undetermined_scenarios,
        "wrong_language": wrong_language,
        "undetermined_language": undetermined,
        "copies": copies,
        "reference_leaks": leaks,
        "length_mean": round(statistics.mean(lengths), 1) if lengths else 0.0,
        "length_median": statistics.median(lengths) if lengths else 0,
    }


def report(result: dict) -> bool:
    """Print one artifact's verdict; True when every hard criterion passed."""
    ok = (
        not result["json_failures"]
        and not result["scenarios_language_bad"]
        and not result["wrong_language"]
        and not result["reference_leaks"]
    )
    print(f"{result['file']} — {result['model']}, prompt {result['prompt']}, "
          f"transport {result['transport']}, "
          f"partial={result['partial']}")
    print(f"  records {result['records']}, variants {result['variants']}, "
          f"length mean {result['length_mean']} / median "
          f"{result['length_median']}")
    print(f"  failed records (bad JSON, or the call itself): "
          f"{len(result['json_failures'])}"
          + (f" {result['json_failures']}" if result["json_failures"] else ""))
    print(f"  language == scenario: "
          f"{result['scenarios_language_ok']}/{result['scenarios_answered']} "
          f"answered scenarios"
          + (f", WRONG: {result['scenarios_language_bad']}"
             if result["scenarios_language_bad"] else "")
          + (f", undecided: {result['scenarios_language_undetermined']}"
             if result["scenarios_language_undetermined"] else "")
          + f"; per variant: wrong {len(result['wrong_language'])}"
          + (f" {result['wrong_language']}" if result["wrong_language"]
             else "")
          + f", Cyrillic-undecidable {len(result['undetermined_language'])}")
    print(f"  example copies: exact {len(result['copies']['exact'])}, "
          f"prefix {len(result['copies']['prefix'])}, "
          f"near {len(result['copies']['near'])}"
          + (f" {result['copies']}" if any(result["copies"].values()) else ""))
    print(f"  reference leaks: {len(result['reference_leaks'])}"
          + (f" {result['reference_leaks']}" if result["reference_leaks"]
             else ""))
    if result["short_answers"]:
        print(f"  short answers (not a failure): {result['short_answers']}")
    print(f"  => {'OK' if ok else 'PROBLEMS'}")
    return ok


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    verdicts = []
    for name in argv:
        path = Path(name)
        if not path.is_absolute():
            path = HERE / name
        verdicts.append(report(check(path)))
    return 0 if all(verdicts) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
