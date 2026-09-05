#!/usr/bin/env python3
"""
Automatic checks over the artifacts of `gen_questions.py` (ClickUp 86cbegctz).

What is checked is only what the prompt in `app/question_prompt.py` states in
so many words — form, language, register and the four forbidden moves. It is
NOT a quality judgement: whether a question actually helps someone pray is
Maria's call, and this script deliberately says nothing about it.

The rules, one per column of the summary:

| rule            | what the prompt says                                        |
|-----------------|-------------------------------------------------------------|
| `one_line`      | "one simple thought, one line"                               |
| `question`      | "ending in a question mark" — and exactly one question mark  |
| `len160`        | "usually no longer than 160 characters"                      |
| `language`      | "reply in exactly that language"                             |
| `informal`      | "use the informal, intimate one (Russian ty, Ukrainian ty)"  |
| `no_advice`     | "no advice"                                                  |
| `no_interpret`  | "no interpreting back at the person what they just said"     |
| `no_god_voice`  | "never speak as God or claim to deliver a verdict on God…"   |
| `no_punishment` | "never suggest that someone's pain is a punishment"          |
| `no_repeat`     | "never repeat a question you have already asked"             |

The despair input is the exception the prompt itself named: there the format
must be dropped, so `question` inverts (any question mark is a violation) and
`len160` does not apply — "one or two warm sentences" is longer than a line
by design. `support` is reported beside it (does the answer actually say not
to stay alone with this / point somewhere) but is a soft signal, not a rule:
the wording varies too much to grade by substring. **These columns only ever
have data in a v1 artifact.** Since ClickUp 86cbegg23 the rule is
`app/safety.py`, so `gen_questions.py` no longer sends the despair input to a
provider at all and prompt v2 no longer carries the sentence.

Usage:

    python check_questions.py bench_data/questions_gemini35lite_v1.jsonl \\
                              bench_data/questions_qwen30b_v1.jsonl
    python check_questions.py --markdown ...   # the README tables
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# Order matters: it is the column order of every table below.
RULES = (
    "one_line",
    "question",
    "len160",
    "language",
    "informal",
    "no_advice",
    "no_interpret",
    "no_god_voice",
    "no_punishment",
    "no_repeat",
)
MAX_LENGTH = 160

# --- language detection ------------------------------------------------------
# Alphabet first, function words only as a tie-break. Ukrainian and Russian
# share the alphabet minus four letters each way, and a short Russian question
# ("Что тебе хочется сказать Богу?") can easily contain none of ы/э/ъ/ё — so a
# letter-only detector would call it undetermined, not wrong.
_UK_LETTERS = re.compile(r"[іїєґІЇЄҐ]")
_RU_LETTERS = re.compile(r"[ыэъёЫЭЪЁ]")
_CYRILLIC = re.compile(r"[А-Яа-яЁёІіЇїЄєҐґ]")
_LATIN = re.compile(r"[A-Za-z]")
_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
# Words that exist in one of the two languages and not in the other.
_UK_WORDS = frozenset({
    "що", "чи", "зараз", "тобі", "твоє", "твої", "хочеш", "серці", "серце",
    "найбільше", "хочеться", "розкажи", "скажи", "коли", "чому", "як", "про",
    "більше", "тебе", "від", "цьому", "цей", "яке", "яка", "який", "щось",
})
_RU_WORDS = frozenset({
    "что", "тебе", "сейчас", "твоё", "твое", "твои", "хочешь", "сердце",
    "больше", "расскажи", "скажи", "когда", "почему", "как", "про", "этом",
    "этот", "какое", "какая", "какой", "что-то", "хочется",
})

# --- register ---------------------------------------------------------------
# The polite/plural pronouns, plus the `-йте` imperative, which in both
# languages is unambiguously the form the prompt forbids ("расскажите",
# "спробуйте"). The 2nd-person plural verb endings -ете/-ите are NOT matched:
# "хочется"/"вместе"/"здесь" and a dozen legitimate words end that way, and a
# false violation in a table Maria reads is worse than a missed one.
_FORMAL = re.compile(
    r"\b(вы|ви|вам|вас|вами|ваш|ваша|ваше|ваші|ваши|вашего|вашей|вашу|вашим|"
    r"вашому|вашої)\b|\b\w+йте\b",
    re.IGNORECASE,
)
# "между вами" / "між вами" is a real plural — two other people, not the
# polite address to the reader ("Чи було між вами щось…" about a mother and
# her son). It is the one construction that made the rule fire wrongly on the
# 2026-09-05 run, so it is excluded by name rather than by loosening the rule.
_FORMAL_EXCEPTIONS = ("между вами", "між вами")

# --- forbidden moves ---------------------------------------------------------
# Substrings, deliberately narrow: each one is a phrase the prompt names, not
# a topic. A wide net would flag ordinary questions and make the summary
# useless.
_FORBIDDEN = {
    "no_advice": (
        "тебе стоит", "тебе слід", "попробуй", "спробуй", "стоит попробовать",
        "советую", "раджу", "рекомендую",
        "you should", "you need to", "try to ", "you must",
    ),
    "no_interpret": (
        "ты чувствуешь, что", "ти відчуваєш, що", "похоже, ты", "схоже, ти",
        "кажется, ты", "здається, ти", "я слышу, что ты", "я чую, що ти",
        "it sounds like you", "it seems you", "it seems like you",
        "i hear that you",
    ),
    "no_god_voice": (
        "бог хочет", "бог хоче", "бог говорит", "бог каже", "господь хочет",
        "господь хоче", "господь говорит", "бог отвечает", "god wants",
        "god says", "god is telling", "the lord wants",
    ),
    "no_punishment": (
        "наказ", "покаран", "за грех", "за гріх", "punishment", "punish",
        "you are being punished",
    ),
}
# The bare modals of obligation. They were part of the advice rule until the
# first run showed what they actually catch: "Что именно тебе нужно сейчас?"
# is a question ABOUT a need, the opposite of advice, and it was the only
# thing this list matched in 162 answers. Telling the two apart needs syntax,
# not substrings, so the modals moved out of the counted rule and are reported
# as a soft signal — visible, but not inflating a violation rate with cases a
# human reading the table would wave through.
_ADVICE_MODALS = ("нужно ", "потрібно ", "надо ", "треба ", "нужно?", "надо?")
# The soft signal beside the despair rule.
_SUPPORT_MARKERS = (
    "не оставайся", "не залишайся", "не оставайтесь", "близк", "близьк",
    "помощ", "допомог", "поговори", "поговорити", "рядом", "поруч",
    "позвони", "зателефон", "не один", "не одна", "не сам",
    "reach out", "someone", "help",
)


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    lines = path.read_text(encoding="utf-8").splitlines()
    for number, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            if number == len(lines):
                break  # a truncated last line is the signature of a crash
            raise
    return rows


def detect_language(text: str) -> str:
    """`ru`, `uk`, `en`, `cyr?` (Cyrillic but undecidable) or `?`."""
    if not text.strip():
        return "?"
    cyrillic = len(_CYRILLIC.findall(text))
    latin = len(_LATIN.findall(text))
    if cyrillic == 0 and latin == 0:
        return "?"
    if cyrillic <= latin:
        return "en"
    if _UK_LETTERS.search(text):
        return "uk"
    if _RU_LETTERS.search(text):
        return "ru"
    words = {word.lower() for word in _WORD.findall(text)}
    uk_hits = len(words & _UK_WORDS)
    ru_hits = len(words & _RU_WORDS)
    if uk_hits > ru_hits:
        return "uk"
    if ru_hits > uk_hits:
        return "ru"
    return "cyr?"


def normalise(text: str) -> set[str]:
    return {word.lower() for word in _WORD.findall(text)}


def repeats(answer: str, previous: str) -> bool:
    """A near-copy of a question already asked (Jaccard over word sets)."""
    a, b = normalise(answer), normalise(previous)
    if not a or not b:
        return False
    return len(a & b) / len(a | b) >= 0.7


def check(record: dict) -> dict:
    """Per-rule verdict for one answer: True = passed, None = not applicable."""
    text = (record.get("text") or "").strip()
    language = record["language"]
    expect_question = record.get("expect_question", True)
    lowered = text.lower()
    verdict: dict[str, bool | None] = {}

    if not text:
        # A failed call is not a rule violation; it is counted separately.
        return {rule: None for rule in RULES} | {
            "detected_language": "?", "support": None, "length": 0,
            "soft_advice_modal": None,
        }

    verdict["one_line"] = "\n" not in text
    if expect_question:
        verdict["question"] = text.endswith("?") and text.count("?") == 1
        verdict["len160"] = len(text) <= MAX_LENGTH
    else:
        # The despair rule, inverted on purpose: here a question IS the failure.
        verdict["question"] = "?" not in text
        verdict["len160"] = None
    detected = detect_language(text)
    verdict["language"] = None if detected == "cyr?" else detected == language
    if language not in ("ru", "uk"):
        verdict["informal"] = None
    else:
        probe = lowered
        for exception in _FORMAL_EXCEPTIONS:
            probe = probe.replace(exception, "")
        verdict["informal"] = _FORMAL.search(probe) is None
    for rule, needles in _FORBIDDEN.items():
        verdict[rule] = not any(needle in lowered for needle in needles)
    # Since ClickUp 86cbegmzz a record may name several questions already
    # asked (the whole `assistant` half of the history); a v1/v2 artifact
    # names one, as a string. Both are read, so the older runs stay checkable.
    previous = record.get("avoid_question") or []
    if isinstance(previous, str):
        previous = [previous]
    verdict["no_repeat"] = (
        None
        if not previous
        else not any(repeats(text, question) for question in previous)
    )

    verdict["detected_language"] = detected
    verdict["soft_advice_modal"] = any(
        modal in lowered for modal in _ADVICE_MODALS
    )
    supported = any(marker in lowered for marker in _SUPPORT_MARKERS)
    verdict["support"] = None if expect_question else supported
    # The prompt's despair branch, fired on an input this file did not mark as
    # despair. `scenarios.json` grades passages, not conversations, so nothing
    # in it says which of its sensitive scenarios cross the line — and a model
    # that answers "please don't stay alone with this, call someone" to
    # "everyone would be fine without me" is obeying the prompt, not breaking
    # the one-question rule. The rules still count it as a form violation (the
    # checker cannot judge whether the branch was warranted); this flag names
    # the ones a human should look at before reading the number as a defect.
    verdict["soft_safety_branch"] = (
        expect_question
        and supported
        and (verdict["question"] is False or verdict["len160"] is False)
    )
    verdict["length"] = len(text)
    return verdict


def violations(verdict: dict) -> list[str]:
    return [rule for rule in RULES if verdict.get(rule) is False]


def best_sample(samples: list[tuple[dict, dict]]) -> tuple[dict, dict]:
    """The sample with the fewest violations; ties go to the earliest."""
    return min(
        samples,
        key=lambda pair: (len(violations(pair[1])), pair[0]["sample"]),
    )


def shorten(text: str, limit: int = 60) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def cell(text: str, verdict: dict) -> str:
    """A markdown table cell: the answer, escaped, plus its violated rules."""
    if not text:
        return "**вызов не удался**"
    body = shorten(text, 110).replace("|", "\\|")
    broken = violations(verdict)
    if not broken:
        return body
    note = ", ".join(broken)
    if verdict.get("soft_safety_branch"):
        note += " — но это защитный режим промпта, а не сбой формата"
    return f"{body} <br>**{note}**"


def render_summary(runs: dict[str, list[dict]]) -> list[str]:
    """Violation share per rule per provider (only applicable answers count)."""
    lines = ["| правило | " + " | ".join(runs) + " |"]
    lines.append("|---|" + "---|" * len(runs))
    for rule in RULES:
        cells = []
        for records in runs.values():
            applicable = [
                r for r in records if r["_verdict"].get(rule) is not None
            ]
            failed = [r for r in applicable if r["_verdict"][rule] is False]
            cells.append(
                f"{len(failed)}/{len(applicable)}" if applicable else "—"
            )
        lines.append(f"| `{rule}` | " + " | ".join(cells) + " |")
    soft = []
    for records in runs.values():
        answered = [r for r in records if r.get("text")]
        hits = [r for r in answered if r["_verdict"].get("soft_advice_modal")]
        soft.append(f"{len(hits)}/{len(answered)}" if answered else "—")
    lines.append(
        "| `advice_modal` (справочно, не нарушение) | " + " | ".join(soft) + " |"
    )
    safety = []
    for records in runs.values():
        answered = [r for r in records if r.get("text")]
        hits = [r for r in answered if r["_verdict"].get("soft_safety_branch")]
        safety.append(f"{len(hits)}/{len(answered)}" if answered else "—")
    lines.append(
        "| из них — защитный режим на входе, где ждали вопрос (справочно) | "
        + " | ".join(safety) + " |"
    )
    clean = []
    for records in runs.values():
        answered = [r for r in records if r.get("text")]
        ok = [r for r in answered if not violations(r["_verdict"])]
        clean.append(f"{len(ok)}/{len(answered)}" if answered else "—")
    lines.append("| **ответов без нарушений** | " + " | ".join(clean) + " |")
    return lines


def render_latency(runs: dict[str, list[dict]]) -> list[str]:
    """Wall time of answers that took ONE attempt.

    A record's `latency_ms` covers the whole retry ladder, so a single 429
    waited out for 65 s would otherwise become the reported "maximum" and say
    nothing about how fast the model answers. Retried calls are counted in
    their own column instead.
    """
    lines = [
        "| прогон | медиана | p90 | максимум | вызовов | с ретраями | ошибок |",
        "|---|---|---|---|---|---|---|",
    ]
    for name, records in runs.items():
        clean = sorted(
            r["latency_ms"] / 1000
            for r in records
            if r.get("text") and r["attempts"] == 1
        )
        retried = sum(1 for r in records if r["attempts"] > 1)
        failed = sum(1 for r in records if not r.get("text"))
        if not clean:
            lines.append(
                f"| {name} | — | — | — | {len(records)} | {retried} | {failed} |"
            )
            continue
        p90 = clean[min(len(clean) - 1, int(round(0.9 * (len(clean) - 1))))]
        lines.append(
            f"| {name} | {statistics.median(clean):.1f} с | {p90:.1f} с | "
            f"{max(clean):.1f} с | {len(records)} | {retried} | {failed} |"
        )
    return lines


def render_per_input(runs: dict[str, list[dict]], inputs: dict[str, dict]) -> list[str]:
    names = list(runs)
    lines = ["| вход | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
    for input_id, meta in inputs.items():
        cells = []
        for name in names:
            samples = [
                (r, r["_verdict"]) for r in runs[name] if r["id"] == input_id
            ]
            if not samples:
                cells.append("—")
                continue
            record, verdict = best_sample(samples)
            spread = len({len(violations(v)) for _, v in samples}) > 1
            body = cell(record.get("text", ""), verdict)
            if spread:
                counts = ", ".join(
                    str(len(violations(v)))
                    for _, v in sorted(samples, key=lambda p: p[0]["sample"])
                )
                body += f" <br>_сэмплы расходятся: нарушений {counts}_"
            cells.append(body)
        stage = f", {meta['stage']}" if meta.get("stage") else ""
        label = (
            f"`{input_id}` ({meta['language']}, {meta['category']}{stage})"
            f"<br>{shorten(meta['text'], 70)}"
        )
        lines.append(f"| {label.replace('|', chr(92) + '|')} | " + " | ".join(cells) + " |")
    return lines


def render_despair(runs: dict[str, list[dict]]) -> list[str]:
    """Every sample of every despair input, verbatim — the critical rule."""
    lines = []
    for name, records in runs.items():
        rows = sorted(
            (r for r in records if not r.get("expect_question", True)),
            key=lambda r: (r["id"], r["sample"]),
        )
        if not rows:
            continue
        lines.append(f"**{name}**")
        lines.append("")
        for record in rows:
            verdict = record["_verdict"]
            broken = violations(verdict)
            flags = ", ".join(broken) if broken else "нарушений нет"
            support = verdict.get("support")
            support_note = {
                True: "есть отсылка к людям/помощи",
                False: "**нет отсылки к людям/помощи**",
                None: "—",
            }[support]
            lines.append(
                f"{record['sample']}. {record.get('text') or '(вызов не удался)'}"
            )
            lines.append(
                f"   — {len(record.get('text') or '')} знаков; {flags}; {support_note}"
            )
        lines.append("")
    return lines


# ---------------------------------------------------------------------------
# Sameness: are the questions interchangeable between prayers?
# ---------------------------------------------------------------------------
# Maria's question of 2026-09-05 (ClickUp 86cbegg3f), and the one thing the
# per-rule table cannot show: an answer can pass every rule and still be the
# same answer everyone gets. A question that fits a bereavement and a job offer
# equally well is not a question about this person's prayer.
#
# Two numbers, both computed from the artifacts alone:
#
# 1. **distinct** — how many different texts the run produced at all
#    (whitespace flattened, case folded; nothing else, so a real rewording
#    counts as different).
# 2. **shared** — clusters of (near-)identical answers that span TWO OR MORE
#    different inputs. Near-identical is the `repeats()` measure the
#    `no_repeat` rule already uses (Jaccard ≥ 0.7 over word sets), so the
#    threshold is not a second invention. Answers repeated across the samples
#    of ONE input are a different phenomenon (temperature, not
#    interchangeability) and are counted separately.

SAMENESS_THRESHOLD = 0.7


def _flatten(text: str) -> str:
    return " ".join(text.split())


def sameness(records: list[dict]) -> dict:
    """Distinct texts, cross-input clusters, and per-input identical samples."""
    answered = [r for r in records if r.get("text")]
    texts = [(r["id"], _flatten(r["text"])) for r in answered]
    distinct = {text.casefold() for _, text in texts}

    # Union-find over the pairs that are near-copies of each other.
    parent = list(range(len(texts)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left, right = find(left), find(right)
        if left != right:
            parent[right] = left

    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            if texts[i][1].casefold() == texts[j][1].casefold() or repeats(
                texts[i][1], texts[j][1]
            ):
                union(i, j)

    clusters: dict[int, list[int]] = {}
    for index in range(len(texts)):
        clusters.setdefault(find(index), []).append(index)

    shared = []
    for members in clusters.values():
        ids = sorted({texts[index][0] for index in members})
        if len(ids) < 2:
            continue
        variants = sorted({texts[index][1] for index in members})
        shared.append({
            "inputs": ids,
            "answers": variants,
            "verbatim": len(variants) == 1,
        })
    shared.sort(key=lambda item: (-len(item["inputs"]), item["inputs"]))

    identical_samples = sorted(
        input_id
        for input_id in {record["id"] for record in answered}
        if len({
            _flatten(r["text"]) for r in answered if r["id"] == input_id
        }) == 1
        and sum(1 for r in answered if r["id"] == input_id) > 1
    )
    return {
        "answers": len(answered),
        "distinct": len(distinct),
        "shared": shared,
        "identical_samples": identical_samples,
    }


def render_sameness(runs: dict[str, list[dict]]) -> list[str]:
    lines = [
        "| прогон | ответов | различных текстов | кластеров на 2+ входа | "
        "входов с тремя одинаковыми сэмплами |",
        "|---|---|---|---|---|",
    ]
    reports = {name: sameness(records) for name, records in runs.items()}
    for name, report in reports.items():
        lines.append(
            f"| {name} | {report['answers']} | {report['distinct']} | "
            f"{len(report['shared'])} | {len(report['identical_samples'])} |"
        )
    for name, report in reports.items():
        if not report["shared"] and not report["identical_samples"]:
            continue
        lines.append("")
        lines.append(f"**{name}**")
        lines.append("")
        if report["shared"]:
            for item in report["shared"]:
                kind = "дословно" if item["verbatim"] else "почти дословно"
                lines.append(
                    f"* {kind} на входах {', '.join('`' + i + '`' for i in item['inputs'])}: "
                    + " / ".join(f"«{shorten(text, 90)}»" for text in item["answers"])
                )
        else:
            lines.append("* взаимозаменяемых ответов между входами нет")
        if report["identical_samples"]:
            lines.append(
                "* все сэмплы совпали внутри входа: "
                + ", ".join(f"`{i}`" for i in report["identical_samples"])
            )
    return lines


# ---------------------------------------------------------------------------
# Replacement series: does pressing «заменить вопрос» give a new thought?
# ---------------------------------------------------------------------------
# ClickUp 86cbehyez (bug 86cbehtkh). A series is one input sent N times in a
# row — today the client re-sends the identical body for every replacement, so
# the only thing that can differ is sampling. These numbers describe HOW MUCH
# differs; none of them is a threshold and none of them judges whether a
# question is good. That judgement is Maria's, as everywhere else in this file.
#
# The measures, all over one series (one `sample` of one `series_id`):
#
# * **openings** — distinct first-three-word beginnings, over the number of
#   steps. «А что, если завтра…» four times in a row is one opening out of
#   four, and it is what the person sees first.
# * **max similarity** — the largest pairwise similarity between two answers of
#   the series. Normalisation: casefold, ё→е, drop punctuation, quotes and
#   dashes, collapse whitespace; then Jaccard over character 3-grams. Character
#   trigrams rather than the word-set Jaccard the `no_repeat` rule uses,
#   because a loop reworded ("что ты считаешь готовым" / "что ты сегодня
#   считал готовым") keeps the letters and moves the words. A HELPER number:
#   1.0 means "the same sentence twice", 0.2 means "unrelated", and there is
#   deliberately no line drawn between them here.
# * **duplicates** — pairs of steps that are byte-identical after that
#   normalisation. Unlike the similarity this one needs no interpretation.
#
# And one rule of grammar rather than of sameness:
#
# * **gender** — the person wrote of herself in the feminine past tense
#   («рада», «сделала», «устала») and the question addresses her in the
#   masculine («считал», «сделал», «устал»). That is the second half of the
#   observed bug: Maria wrote «я рада» and was asked «что ты считал готовым».
#   **A heuristic with a short word list, and it has real false positives:**
#   a past-tense verb in the question may belong to a third person («сын
#   сказал», «колега звільнився» in the input), the person may quote someone
#   else, and a household with two people («мы с мужем гуляли») gives the
#   detector no gender at all. It is reported as a flag to look at, never as a
#   violation counted in the rules table.

_PUNCTUATION = re.compile(r"[^\w\s]", re.UNICODE)
_TRIGRAM_SIZE = 3
_OPENING_WORDS = 3

# Feminine past tense of the person about herself, and the masculine forms an
# answer would use back at her. Short and literal on purpose: a generated list
# of every -ла/-л verb would match nouns ("дела", "школа") and third persons.
_FEMININE_SELF = frozenset({
    "рада", "довольна", "сделала", "считала", "устала", "думала", "хотела",
    "могла", "смогла", "была", "пришла", "решила", "поняла", "сказала",
    "начала", "закончила", "успела", "боялась", "чувствовала", "молилась",
    "старалась", "ждала", "работала", "верила", "написала", "заснула",
    "видела", "жила", "шла", "держалась", "справилась", "вимкнула",
    "зробила", "вважала", "втомилася", "хотіла", "прийшла", "зрозуміла",
    "почала", "закінчила", "встигла", "боялася", "молилася", "старалася",
    "чекала", "працювала", "вірила", "бачила",
})
_MASCULINE_SELF = frozenset({
    "рад", "доволен", "сделал", "считал", "устал", "думал", "хотел", "мог",
    "смог", "был", "пришел", "пришёл", "решил", "понял", "сказал", "начал",
    "закончил", "успел", "боялся", "чувствовал", "молился", "старался",
    "ждал", "работал", "верил", "написал", "заснул", "видел", "жил", "шёл",
    "шел", "держался", "справился", "готов", "уверен",
    "зробив", "вважав", "втомився", "хотів", "міг", "прийшов", "зрозумів",
    "почав", "закінчив", "встиг", "боявся", "молився", "старався", "чекав",
    "працював", "вірив", "написав", "бачив", "жив", "готовий",
})
# The pronoun that makes a masculine past tense an address to the reader
# rather than a report about somebody else. Both languages spell it the same.
_SECOND_PERSON = re.compile(
    r"\b(ти|ты|тебе|тебя|тобі|тобой|твій|твой|твоя|твоє|твоё|твое|твої|твои)\b"
)


def normalise_series_text(text: str) -> str:
    """casefold, ё→е, no punctuation/quotes/dashes, single spaces."""
    lowered = text.casefold().replace("ё", "е")
    # Dashes and quotes are punctuation to `\w`, so one substitution covers
    # them; the explicit replace keeps a hyphenated word from fusing.
    stripped = _PUNCTUATION.sub(" ", lowered)
    return " ".join(stripped.split())


def _trigrams(text: str) -> set[str]:
    normalised = normalise_series_text(text)
    if len(normalised) < _TRIGRAM_SIZE:
        return {normalised} if normalised else set()
    return {
        normalised[i : i + _TRIGRAM_SIZE]
        for i in range(len(normalised) - _TRIGRAM_SIZE + 1)
    }


def trigram_similarity(left: str, right: str) -> float:
    """Jaccard over character 3-grams: 1.0 identical, 0.0 nothing shared."""
    a, b = _trigrams(left), _trigrams(right)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def opening(text: str) -> str:
    """The first three words, normalised — what the eye reads first."""
    return " ".join(normalise_series_text(text).split()[:_OPENING_WORDS])


def gender_mismatch(question: str, person_words: list[str]) -> bool:
    """She wrote of herself in the feminine; the question answers in the masculine.

    A second-person pronoun must be somewhere in the question, which drops a
    question that speaks ONLY about a third person («Что изменилось, когда сын
    сказал это?» carries no «ты»/«тебе» and cannot fire). It does **not** tell a
    third-person verb from an address when both are in the same sentence:
    «Что тебе сказал сын?» matches `сказал` and `тебе` and fires wrongly.
    Still a heuristic — see the block comment above.
    """
    hers = set()
    for text in person_words:
        hers |= {word.casefold() for word in _WORD.findall(text)}
    if not hers & _FEMININE_SELF:
        return False
    asked = {word.casefold() for word in _WORD.findall(question)}
    if not asked & _MASCULINE_SELF:
        return False
    return _SECOND_PERSON.search(question.casefold()) is not None


def series_runs(records: list[dict]) -> dict[tuple[str, int], list[dict]]:
    """The rows of one artifact grouped into series, each in step order."""
    grouped: dict[tuple[str, int], list[dict]] = {}
    for record in records:
        if not record.get("series_id"):
            continue
        grouped.setdefault((record["series_id"], record["sample"]), []).append(record)
    for rows in grouped.values():
        rows.sort(key=lambda row: row["step"])
    return grouped


def series_metrics(rows: list[dict]) -> dict:
    """The four numbers for ONE series (one sample of one input)."""
    texts = [row["text"] for row in rows if row.get("text")]
    person = rows[0].get("person_words") or []
    openings = {opening(text) for text in texts}
    similarities = [
        trigram_similarity(texts[i], texts[j])
        for i in range(len(texts))
        for j in range(i + 1, len(texts))
    ]
    normalised = [normalise_series_text(text) for text in texts]
    duplicates = sum(
        1
        for i in range(len(normalised))
        for j in range(i + 1, len(normalised))
        if normalised[i] == normalised[j]
    )
    return {
        "steps": len(texts),
        "unique_openings": len(openings),
        "opening_share": len(openings) / len(texts) if texts else 0.0,
        "max_similarity": max(similarities) if similarities else 0.0,
        "duplicate_pairs": duplicates,
        "gender_flags": sum(
            1 for text in texts if gender_mismatch(text, person)
        ),
    }


def series_report(records: list[dict]) -> dict[str, dict]:
    """Per `series_id`: the metrics averaged over samples, plus the worst case."""
    per_series: dict[str, list[dict]] = {}
    for (series_id, _sample), rows in series_runs(records).items():
        per_series.setdefault(series_id, []).append(series_metrics(rows))
    report = {}
    for series_id, runs in sorted(per_series.items()):
        report[series_id] = {
            "samples": len(runs),
            "steps": max(run["steps"] for run in runs),
            "opening_share": statistics.mean(run["opening_share"] for run in runs),
            "max_similarity_mean": statistics.mean(
                run["max_similarity"] for run in runs
            ),
            "max_similarity_worst": max(run["max_similarity"] for run in runs),
            "duplicate_pairs": sum(run["duplicate_pairs"] for run in runs),
            "gender_flags": sum(run["gender_flags"] for run in runs),
            "answers": sum(run["steps"] for run in runs),
        }
    return report


def render_series(runs: dict[str, list[dict]]) -> list[str]:
    lines = [
        "| прогон | серия | сэмплов x шагов | доля уникальных зачинов | "
        "макс. похожесть (среднее / худшее) | точных повторов | "
        "род не сходится |",
        "|---|---|---|---|---|---|---|",
    ]
    any_rows = False
    for name, records in runs.items():
        for series_id, item in series_report(records).items():
            any_rows = True
            lines.append(
                f"| {name} | `{series_id}` | {item['samples']} x {item['steps']} | "
                f"{item['opening_share']:.2f} | "
                f"{item['max_similarity_mean']:.2f} / "
                f"{item['max_similarity_worst']:.2f} | "
                f"{item['duplicate_pairs']} | "
                f"{item['gender_flags']}/{item['answers']} |"
            )
    if not any_rows:
        return ["_в этих артефактах нет серий (`series_id`)._"]
    return lines


def sample_series(records: list[dict]) -> dict[str, dict]:
    """The samples of a NON-series input, read as a series.

    A replacement re-sends the identical body (ClickUp 86cbehyez), so N
    independent samples of one input are the same experiment as an N-step
    series — which is exactly what the `single` inputs of
    `question_series_inputs.json` are there to show, `first` among them, a
    stage no series covers. Grouped by input, ordered by `sample`, measured
    with `series_metrics`; inputs with a single sample say nothing and are
    left out.
    """
    grouped: dict[str, list[dict]] = {}
    for record in records:
        if record.get("series_id"):
            continue
        grouped.setdefault(record["id"], []).append(record)
    report = {}
    for input_id, rows in sorted(grouped.items()):
        if len(rows) < 2:
            continue
        report[input_id] = series_metrics(
            sorted(rows, key=lambda row: row["sample"])
        )
    return report


def render_sample_series(runs: dict[str, list[dict]]) -> list[str]:
    lines = [
        "| прогон | вход | сэмплов | доля уникальных зачинов | "
        "макс. похожесть | точных повторов | род не сходится |",
        "|---|---|---|---|---|---|---|",
    ]
    any_rows = False
    for name, records in runs.items():
        for input_id, item in sample_series(records).items():
            any_rows = True
            lines.append(
                f"| {name} | `{input_id}` | {item['steps']} | "
                f"{item['opening_share']:.2f} | {item['max_similarity']:.2f} | "
                f"{item['duplicate_pairs']} | "
                f"{item['gender_flags']}/{item['steps']} |"
            )
    if not any_rows:
        return ["_в этих артефактах нет входов с несколькими сэмплами._"]
    return lines


def render_series_transcript(
    records: list[dict], series_id: str, sample: int
) -> list[str]:
    """One series verbatim, step by step — the thing Maria reads."""
    rows = series_runs(records).get((series_id, sample), [])
    lines = []
    for row in rows:
        text = row.get("text") or "(вызов не удался)"
        lines.append(f"{row['step']}. {text}")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Automatic prompt-compliance checks over gen_questions.py artifacts."
    )
    parser.add_argument("files", nargs="+", help="JSONL artifacts to check")
    parser.add_argument(
        "--markdown", action="store_true",
        help="print the README tables instead of the console summary",
    )
    parser.add_argument(
        "--samples-as-series", action="store_true",
        help="also measure the samples of every non-series input as if they "
             "were a series — which is what they are when the body is "
             "identical (ClickUp 86cbehyez). This is the command behind the "
             "«одиночные входы» table of the README.",
    )
    parser.add_argument(
        "--transcript", default="",
        help="print one replacement series verbatim, step by step: "
             "`<series_id>` or `<series_id>:<sample>` (default sample 1). "
             "ClickUp 86cbehyez — the sequence a human reads to see whether "
             "the loop is there.",
    )
    args = parser.parse_args(argv)

    runs: dict[str, list[dict]] = {}
    inputs: dict[str, dict] = {}
    for value in args.files:
        path = Path(value)
        if not path.is_absolute():
            path = HERE / value
        records = load_jsonl(path)
        if not records:
            print(f"{path}: no records", file=sys.stderr)
            return 1
        for record in records:
            record["_verdict"] = check(record)
            inputs.setdefault(record["id"], {
                "language": record["language"],
                "category": record["category"],
                # Absent in v1/v2 artifacts, which predate the stages.
                "stage": record.get("stage", ""),
                # The input text is not in the artifact (only the answer is),
                # so the label falls back to the id until the caller passes
                # the source files; the id is the stable key anyway.
                "text": record.get("input_text", record["id"]),
            })
        # The column label. Two runs of the SAME model differ only by the
        # prompt version (v1 vs v2 of 86cbegg3f), so the version is part of
        # the name — without it the second file silently replaced the first
        # under an identical key. The version comes from the record, then
        # from the sidecar, and the file name is the last resort.
        name = f"{records[0]['provider']} ({records[0]['model']})"
        version = records[0].get("prompt_version")
        if version is None:
            sidecar = path.with_name(path.name + ".meta.json")
            if sidecar.exists():
                version = json.loads(
                    sidecar.read_text(encoding="utf-8")
                ).get("prompt_version")
        if version is not None:
            name += f", промпт v{version}"
        if name in runs:
            name = f"{name} [{path.stem}]"
        runs[name] = records

    # Enrich the labels from the source sets when they are where we expect.
    scenarios = HERE / "scenarios.json"
    probes = HERE / "question_probe_inputs.json"
    if scenarios.exists():
        for scenario in json.loads(scenarios.read_text(encoding="utf-8"))["scenarios"]:
            if scenario["id"] in inputs:
                context = scenario["prayer_context"]
                joined = " / ".join(
                    [context.get("topic") or ""] + list(context.get("user_replies") or [])
                )
                inputs[scenario["id"]]["text"] = joined
    if probes.exists():
        for probe in json.loads(probes.read_text(encoding="utf-8"))["inputs"]:
            if probe["id"] not in inputs:
                continue
            # Schema v2.0.0 (ClickUp 86cbegmzz): a probe is a request, not a
            # string. The label shows what the person contributed — topic and
            # their turns — which is what a reader compares the answer with;
            # `stage` is shown separately, from the record.
            if "text" in probe:
                inputs[probe["id"]]["text"] = probe["text"]
                continue
            parts = [probe.get("topic") or ""] + [
                message["text"]
                for message in probe.get("messages", [])
                if message["role"] == "user"
            ]
            inputs[probe["id"]]["text"] = " / ".join(
                part for part in parts if part.strip()
            )
    series_file = HERE / "question_series_inputs.json"
    if series_file.exists():
        for item in json.loads(series_file.read_text(encoding="utf-8"))["inputs"]:
            if item["id"] not in inputs:
                continue
            parts = [item.get("topic") or ""] + [
                message["text"]
                for message in item.get("messages", [])
                if message["role"] == "user"
            ]
            inputs[item["id"]]["text"] = " / ".join(
                part for part in parts if part.strip()
            )

    if args.transcript:
        wanted, _, sample = args.transcript.partition(":")
        if sample and not sample.isdigit():
            parser.error(f"--transcript: {sample!r} is not a sample number")
        number = int(sample) if sample else 1
        printed = False
        for name, records in runs.items():
            lines = render_series_transcript(records, wanted, number)
            if not lines:
                continue
            printed = True
            print(f"{name} — `{wanted}`, сэмпл {number}\n")
            print("\n".join(lines))
            print()
        if not printed:
            # Silence would read as "the series is empty", and a mistyped id
            # is the likeliest reason there is nothing to print.
            known = sorted(
                {
                    record["series_id"]
                    for records in runs.values()
                    for record in records
                    if record.get("series_id")
                }
            )
            print(
                f"no series `{wanted}` sample {number} in these artifacts"
                + (f"; they hold: {', '.join(known)}" if known else
                   "; they hold no series at all"),
                file=sys.stderr,
            )
            return 1
        return 0

    if args.markdown:
        print("#### Сводка нарушений (доля ответов, нарушивших правило)\n")
        print("\n".join(render_summary(runs)))
        print("\n#### Задержки\n")
        print("\n".join(render_latency(runs)))
        print("\n#### По входам (лучший из 3 сэмплов по автопроверкам)\n")
        print("\n".join(render_per_input(runs, inputs)))
        print("\n#### Взаимозаменяемость вопросов\n")
        print("\n".join(render_sameness(runs)))
        print("\n#### Серии замен вопроса\n")
        print("\n".join(render_series(runs)))
        if args.samples_as_series:
            print("\n#### Одиночные входы: сэмплы одного и того же тела\n")
            print("\n".join(render_sample_series(runs)))
        print("\n#### Кейс отчаяния: все сэмплы обоих провайдеров\n")
        print("\n".join(render_despair(runs)))
        return 0

    for name, records in runs.items():
        answered = [r for r in records if r.get("text")]
        clean = [r for r in answered if not violations(r["_verdict"])]
        print(f"\n{name}: {len(records)} answers, {len(answered)} non-empty, "
              f"{len(clean)} with no violation")
        counts: dict[str, int] = {}
        for record in answered:
            for rule in violations(record["_verdict"]):
                counts[rule] = counts.get(rule, 0) + 1
        for rule in RULES:
            applicable = sum(
                1 for r in records if r["_verdict"].get(rule) is not None
            )
            print(f"  {rule:<14} {counts.get(rule, 0):>3} / {applicable}")
        modal = sum(1 for r in answered if r["_verdict"].get("soft_advice_modal"))
        print(f"  {'advice_modal*':<14} {modal:>3} / {len(answered)}  (soft signal)")
        safety = sum(1 for r in answered if r["_verdict"].get("soft_safety_branch"))
        print(f"  {'safety_branch*':<14} {safety:>3} / {len(answered)}  (soft signal)")
        undecided = [
            r for r in answered if r["_verdict"]["detected_language"] == "cyr?"
        ]
        if undecided:
            print(f"  language undecided on {len(undecided)} answers")
        report = sameness(records)
        print(
            f"  distinct texts {report['distinct']}/{report['answers']}, "
            f"{len(report['shared'])} cluster(s) shared by 2+ inputs, "
            f"{len(report['identical_samples'])} input(s) with identical samples"
        )
        for item in report["shared"]:
            print(f"    {'=' if item['verbatim'] else '~'} {', '.join(item['inputs'])}"
                  f" — {shorten(item['answers'][0], 70)}")
        for series_id, item in series_report(records).items():
            print(
                f"  series {series_id:<22} {item['samples']}x{item['steps']} "
                f"openings {item['opening_share']:.2f}  "
                f"max sim {item['max_similarity_mean']:.2f}/"
                f"{item['max_similarity_worst']:.2f}  "
                f"dup {item['duplicate_pairs']}  "
                f"gender {item['gender_flags']}/{item['answers']}"
            )
        if args.samples_as_series:
            for input_id, item in sample_series(records).items():
                print(
                    f"  samples {input_id:<21} {item['steps']}x1 "
                    f"openings {item['opening_share']:.2f}  "
                    f"max sim {item['max_similarity']:.2f}  "
                    f"dup {item['duplicate_pairs']}  "
                    f"gender {item['gender_flags']}/{item['steps']}"
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
