from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys

import language_detection


def test_detects_supported_and_other_iso_languages_offline():
    samples = {
        "ru": "Мне очень тяжело сейчас и я не знаю, что делать дальше",
        "uk": "Мені дуже важко зараз і я не знаю, що робити далі",
        "en": "This is very hard for me and I do not know what to do next",
        "es": "Necesito ayuda porque estoy muy triste",
        "pl": "Potrzebuję pomocy, bo jest mi bardzo ciężko",
        "pt": "Preciso de ajuda porque estou muito triste",
    }

    assert {
        language: language_detection.detect_language(text)
        for language, text in samples.items()
    } == {language: language for language in samples}


def test_wordless_input_is_unknown_without_calling_the_model(monkeypatch):
    class UnexpectedIdentifier:
        def classify(self, text):
            raise AssertionError("wordless input must not reach the classifier")

    monkeypatch.setattr(language_detection, "_IDENTIFIER", UnexpectedIdentifier())

    for text in ("", "  \n", "123 !!!", "🙏💔"):
        assert language_detection.detect_language(text) is None


def test_confidence_boundary_is_inclusive(monkeypatch):
    class Identifier:
        def __init__(self, confidence):
            self.confidence = confidence

        def classify(self, text):
            return "es", self.confidence

    monkeypatch.setattr(language_detection, "_IDENTIFIER", Identifier(0.899999))
    assert language_detection.detect_language("texto") is None

    monkeypatch.setattr(language_detection, "_IDENTIFIER", Identifier(0.9))
    assert language_detection.detect_language("texto") == "es"


def test_non_linguistic_model_codes_are_unknown(monkeypatch):
    class Identifier:
        def classify(self, text):
            return "zxx", 1.0

    monkeypatch.setattr(language_detection, "_IDENTIFIER", Identifier())

    assert language_detection.detect_language("https://example.com") is None


def test_invalid_model_confidence_fails_loudly(monkeypatch):
    class Identifier:
        def classify(self, text):
            return "en", float("nan")

    monkeypatch.setattr(language_detection, "_IDENTIFIER", Identifier())

    try:
        language_detection.detect_language("text")
    except RuntimeError as error:
        assert "invalid normalized confidence" in str(error)
    else:
        raise AssertionError("invalid dependency output must not be hidden")


def test_singleton_is_safe_under_concurrent_detection():
    text = "Мені дуже важко зараз і я не знаю, що робити далі"

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(language_detection.detect_language, [text] * 64))

    assert results == ["uk"] * 64


def test_calls_reuse_the_loaded_identifier():
    identifier = language_detection._IDENTIFIER

    language_detection.detect_language("Necesito ayuda porque estoy muy triste")
    language_detection.detect_language("Preciso de ajuda porque estou muito triste")

    assert language_detection._IDENTIFIER is identifier


def test_model_initialization_failure_aborts_import(tmp_path: Path):
    package = tmp_path / "py3langid"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "langid.py").write_text(
        "MODEL_FILE = 'missing'\n"
        "class LanguageIdentifier:\n"
        "    @classmethod\n"
        "    def from_model_file(cls, *args, **kwargs):\n"
        "        raise RuntimeError('broken bundled model')\n",
        encoding="utf-8",
    )
    app_path = Path(language_detection.__file__).parent
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join((str(tmp_path), str(app_path)))

    completed = subprocess.run(
        [sys.executable, "-c", "import language_detection"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "broken bundled model" in completed.stderr
