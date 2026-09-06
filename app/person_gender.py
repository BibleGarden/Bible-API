"""The person's grammatical gender, decided in code (ClickUp 86cbejvt2).

Prompt v4 asked the model to take the gender from the person's own words and
v5 kept the rule with named example forms. The independent assessment of
2026-09-06
(`evaluation/bench_data/question_comparison_prompt_v5_before_after/FABLE_ASSESSMENT.md`)
measured what that is worth on Qwen3-30B: **15 answers of 99** addressed a
woman in the masculine, ten of them in the Ukrainian series where the prompt
carried Ukrainian example forms. The assessment's own first recommendation is
the one implemented here — compute the gender from the person's words in code
and state it in the message, the way `app/safety.py` took the despair rule out
of the prompt and `app/question_novelty.py` took the repeat check out of it.

**A minimal reviewed list, not a morphological analyser.** The listed words are
the forms a person typically uses about themselves, in the pairs the reviewer
approved; everything else answers `None`. That is deliberate: a wrong gender is
worse than none, because the message's `None` branch tells the model to word
the question without gendered forms at all, while a wrong `f`/`m` puts an
error into every sentence. So the list may only ever grow through review.

**Honest limitation: Russian and Ukrainian past tense does not mark person.**
«Сделала» is feminine, but it is feminine for *whoever* did it — «мама сказала»,
«сестра успела», «брат отправил» carry the same endings as «я сделала». Only
the short adjectives «рада»/«рад»/«радий» are close to first-person by usage,
and even they are not exclusive («она рада»). So the known false positives are
real and named rather than papered over:

| the person writes | this answers | why |
| --- | --- | --- |
| «мама успела закончить» | `f` | past tense, third person |
| «брат отправил письмо» | `m` | the same |
| «муж сделал, а я не успела» | `None` | both genders present — a contradiction |
| «я устала» | `None` | `устала` is not on the reviewed list |
| "I was glad" | `None` | English has no such form to read |

**A proximity rule was tried and rejected** (review of 86cbejvt2). Requiring a
first-person pronoun («я», «мне», «мені» …) within ±4 words of the form was
measured against the 16 scenarios of
`evaluation/question_quality_inputs.json`, the reference set: it would have
turned **five of the eight** decided cases into `None` — `series-gratitude-ru`
(«Успела закончить отчёт», the sentence starts with the verb),
`series-exhaustion-uk` («заснула просто в одязі», five words after the last
«я»), `joy-ru` («Сегодня увиделась с подругой»), `waiting-ru` («Отправил
заявку»), `reflect-ru` («пока молилась, поняла») — because Russian and
Ukrainian drop the subject pronoun exactly where the verb already carries the
person. Widening the window would have bought back a false-positive rate no
narrower than today's. So the behaviour is unchanged, and the table above plus
`tests/test_person_gender.py` say what it costs; the mistakes it makes are on
sentences about a third person, which is the case the assessment of v5 never
observed in a prayer.

Three answers and nothing else:

* `"f"` — only feminine forms matched;
* `"m"` — only masculine forms matched;
* `None` — nothing matched, or **both** did. A conversation carrying both
  «сделала» and «сделал» is either two people or a quotation, and neither is
  evidence about the one person being addressed.

English (and any text without these Cyrillic forms) is `None` by
construction: English second-person address carries no gender, so there is
nothing to decide and nothing to state.

The module imports nothing from the application — the same rule
`question_prompt.py` and `question_novelty.py` follow, so the evaluation tools
can build the production message without a FastAPI or `config` import.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

FEMININE = "f"
MASCULINE = "m"

# The reviewed list (2026-09-06), written as the pairs it was reviewed as:
# feminine form -> its masculine counterparts. `рада` has two — Russian «рад»
# and Ukrainian «радий» — which is why this is a mapping rather than two
# parallel tuples. `заснув`, `втомився`, `радів` and `зробив` are Ukrainian;
# `заснула`, `рада` and `сделала` are shared or Russian. The Russian «заснул»
# is deliberately absent: it was not in the reviewed list, and this file grows
# only through review.
FORM_PAIRS: dict[str, tuple[str, ...]] = {
    "рада": ("рад", "радий"),
    "сделала": ("сделал",),
    "заснула": ("заснув",),
    "ответила": ("ответил",),
    "успела": ("успел",),
    "поняла": ("понял",),
    "увиделась": ("увиделся",),
    "молилась": ("молился",),
    "отправила": ("отправил",),
    "втомилася": ("втомився",),
    "раділа": ("радів",),
    "зробила": ("зробив",),
}
FEMININE_FORMS: tuple[str, ...] = tuple(FORM_PAIRS)
MASCULINE_FORMS: tuple[str, ...] = tuple(
    form for forms in FORM_PAIRS.values() for form in forms
)

# Whole words only: `рад` is a prefix of `рада`, and a substring search would
# read every woman's «я рада» as a man's «я рад».
_WORD_PATTERN = re.compile(r"[^\W\d_]+", re.UNICODE)

_FEMININE_SET = frozenset(FEMININE_FORMS)
_MASCULINE_SET = frozenset(MASCULINE_FORMS)


def _words(text: str) -> list[str]:
    """The words of `text`, casefolded — `ё` and `е` are not conflated here.

    None of the listed forms differs by `ё`, so the normalisation
    `question_novelty` needs is not needed at all: adding it would only widen
    the list silently the next time a form is appended.
    """
    return [word.casefold() for word in _WORD_PATTERN.findall(text)]


def detect_gender(texts: Iterable[str]) -> str | None:
    """`"f"`, `"m"` or `None` for the person who wrote `texts`.

    `texts` must be the person's **own** words and nothing else — their `user`
    turns and the topic. A question of ours is never evidence: it may itself
    carry the wrong gender, which is the very bug this module exists to close.
    """
    feminine = False
    masculine = False
    for text in texts:
        for word in _words(text):
            if word in _FEMININE_SET:
                feminine = True
            elif word in _MASCULINE_SET:
                masculine = True
        if feminine and masculine:
            return None
    if feminine and not masculine:
        return FEMININE
    if masculine and not feminine:
        return MASCULINE
    return None
