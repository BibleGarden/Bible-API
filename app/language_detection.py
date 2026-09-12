"""Offline language identification shared by the question and safety paths.

The bundled py3langid model covers 139 languages.  A normalized top
probability below the reviewed threshold means that the text does not provide
enough evidence, so callers receive ``None`` and may use their own context.
The model is loaded eagerly once: a missing or broken dependency must fail at
startup, before a request needs the detector.
"""

from __future__ import annotations

import math

from py3langid.langid import MODEL_FILE, LanguageIdentifier

MIN_LANGUAGE_CONFIDENCE = 0.9
_NON_LANGUAGE_CODES = frozenset({"und", "zxx"})

# LanguageIdentifier.classify builds its score and feature arrays per call and
# only reads these model arrays.  The singleton is therefore safe to share
# between FastAPI worker threads, and avoids loading the ~4.6 MB model again.
_IDENTIFIER = LanguageIdentifier.from_model_file(MODEL_FILE, norm_probs=True)


def detect_language(text: str) -> str | None:
    """Return an ISO language code, or ``None`` without sufficient evidence."""
    if not any(character.isalpha() for character in text):
        return None

    language, confidence = _IDENTIFIER.classify(text)
    if not math.isfinite(confidence) or not 0.0 <= confidence <= 1.0:
        raise RuntimeError("py3langid returned an invalid normalized confidence")
    if language in _NON_LANGUAGE_CODES:
        return None
    return language if confidence >= MIN_LANGUAGE_CONFIDENCE else None
