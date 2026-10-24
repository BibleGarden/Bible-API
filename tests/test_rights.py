"""Registry integrity, fail-fast loading and the authenticated catalog contract."""

from copy import deepcopy
import re
import subprocess
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
import pytest
import yaml

from rights import REGISTRY, REGISTRY_PATH, RightsRegistry, load_registry, router


@pytest.fixture
def document():
    return yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))


def test_reviewed_materials_and_aliases():
    assert {
        "text:bsb", "text:webus", "text:webbe", "text:npu",
        "audio:bsb:bsb_souer", "audio:webus:winfred_henson", "audio:webbe:web_british",
    } <= set(REGISTRY.items)
    for item in REGISTRY.items.values():
        assert re.fullmatch(r"[a-z][a-z0-9_]*", item.translation_alias)
        # Sources document license evidence from publishers, not the download location.
        assert all("bible.com" != source.host for source in item.sources)
    npu = REGISTRY.items["text:npu"]
    assert npu.trademark_required is True
    for lang in ("en", "ru", "uk"):
        assert "© 2022" in getattr(npu.attribution, lang)
        assert "https://creativecommons.org/licenses/by-sa/4.0/" in getattr(npu.attribution, lang)
        assert "The original work by Biblica, Inc." in getattr(npu.attribution, lang)
        for field in (npu.title, npu.attribution, npu.changes, npu.trademark, npu.credit):
            for forbidden in ("Biblica®", "Бібліка®", "™", "Used with permission"):
                assert forbidden not in getattr(field, lang)
    assert "has not been performed" in npu.changes.en
    assert "must not be called World English Bible" in REGISTRY.items["text:webus"].trademark.en


def test_public_domain_materials_use_credit_instead_of_rights_holder(document):
    for item_id, item in REGISTRY.items.items():
        if item.public_domain_basis is not None or (item.license and item.license.id == "CC0-1.0"):
            assert item.rights_holder is None
            assert item.credit is not None
            document["items"][item_id]["rights_holder"] = "A narrator is not a rights holder"
            with pytest.raises(ValidationError, match="public-domain materials have no rights holder"):
                RightsRegistry.model_validate(document)
            document["items"][item_id]["rights_holder"] = None
    assert REGISTRY.items["text:npu"].rights_holder == "Biblica, Inc."
    assert "has been removed" in REGISTRY.items["text:npu"].trademark.en


@pytest.mark.parametrize("item_id", ["text:npu", "text:webus", "text:webbe"])
@pytest.mark.parametrize("missing", [True, False])
def test_required_trademark_cannot_be_missing_or_null(document, item_id, missing):
    item = document["items"][item_id]
    assert item["trademark_required"] is True
    if missing:
        del item["trademark"]
    else:
        item["trademark"] = None
    with pytest.raises(ValidationError, match="require a localized trademark terms notice"):
        RightsRegistry.model_validate(document)


def test_trademark_requirement_is_data_driven(document):
    item = document["items"].pop("text:bsb")
    item["translation_alias"] = "another_translation"
    item["trademark_required"] = True
    document["items"]["text:another_translation"] = item
    with pytest.raises(ValidationError, match="require a localized trademark terms notice"):
        RightsRegistry.model_validate(document)
    item["trademark"] = {"en": "Terms", "ru": "Условия", "uk": "Умови"}
    RightsRegistry.model_validate(document)
    item["trademark_required"] = False
    del item["trademark"]
    RightsRegistry.model_validate(document)


@pytest.mark.parametrize("value", [None, "true", 1])
def test_trademark_requirement_must_be_explicit_boolean(document, value):
    document["items"]["text:npu"]["trademark_required"] = value
    with pytest.raises(ValidationError):
        RightsRegistry.model_validate(document)


@pytest.mark.parametrize("field", ["title", "rights_holder", "sources", "attribution", "changes", "checked_on", "trademark_required"])
def test_required_fields(document, field):
    del document["items"]["text:npu"][field]
    with pytest.raises(ValidationError):
        RightsRegistry.model_validate(document)


@pytest.mark.parametrize("field", ["title", "attribution", "changes", "trademark", "credit"])
@pytest.mark.parametrize("lang", ["en", "ru", "uk"])
def test_all_localizations_are_required(document, field, lang):
    del document["items"]["text:npu"][field][lang]
    with pytest.raises(ValidationError):
        RightsRegistry.model_validate(document)


@pytest.mark.parametrize("patch", [
    {"sources": ["javascript:alert(1)"]}, {"sources": []},
    {"translation_alias": "npu/../syn"}, {"translation_alias": "NPU"},
    {"voice_alias": "npu_uk"}, {"rights_holder": "  "},
    {"checked_on": "2999-01-01"}, {"checked_on": "not-a-date"},
    {"copyright": None}, {"unexpected": "field"},
    {"license": None},
    {"license": {"id": "MIT", "name": "MIT", "url": "https://example.org/"}},
    {"license": {"id": "CC-BY-SA-4.0", "name": "CC BY-SA", "url": "https://example.org/"}},
    {"public_domain_basis": {"en": "PD", "ru": "PD", "uk": "PD"}},
    {"permission": {"en": "Permission", "ru": "Разрешение", "uk": "Дозвіл"}},
])
def test_invalid_entries_are_rejected(document, patch):
    document["items"]["text:npu"].update(patch)
    with pytest.raises(ValidationError):
        RightsRegistry.model_validate(document)


def test_permission_is_the_only_basis_and_names_its_grantor(document):
    item = document["items"]["audio:srp:orgin"]
    assert item["license"] is None and item["public_domain_basis"] is None
    for lang in ("en", "ru", "uk"):
        assert "2026-10-20" in item["permission"][lang]
    assert "AI voice cloning" in item["permission"]["en"]
    item["rights_holder"] = None
    with pytest.raises(ValidationError, match="permission requires the rights holder"):
        RightsRegistry.model_validate(document)
    item["rights_holder"] = "Дмитрий Оргин"
    item["permission"] = None
    with pytest.raises(ValidationError, match="exactly one license, public-domain basis or permission"):
        RightsRegistry.model_validate(document)


def test_ids_and_audio_voice_are_required(document):
    wrong_id = deepcopy(document)
    wrong_id["items"]["text:wrong"] = wrong_id["items"].pop("text:npu")
    with pytest.raises(ValidationError, match="id must match"):
        RightsRegistry.model_validate(wrong_id)
    document["items"]["audio:bsb:bsb_souer"]["voice_alias"] = None
    with pytest.raises(ValidationError, match="voice_alias"):
        RightsRegistry.model_validate(document)


@pytest.mark.parametrize("contents", ["schema_version: [", "items: {}", "schema_version: 1\nschema_version: 2\n"])
def test_invalid_files_fail_loudly(tmp_path, contents):
    path = tmp_path / "rights.yaml"
    path.write_text(contents)
    with pytest.raises(RuntimeError, match="Invalid rights registry"):
        load_registry(path)
    with pytest.raises(RuntimeError, match="Invalid rights registry"):
        load_registry(tmp_path / "missing.yaml")


def test_invalid_registry_aborts_router_import():
    result = subprocess.run([
        sys.executable, "-c",
        "from pathlib import Path; original = Path.read_text; "
        "Path.read_text = lambda self, *a, **k: 'items: {}' if self.name == 'rights.yaml' else original(self, *a, **k); "
        "import rights",
    ], cwd=REGISTRY_PATH.parent, capture_output=True, text=True)
    assert result.returncode != 0
    assert "Invalid rights registry" in result.stderr


def test_endpoint_returns_registry_and_enforces_catalog_auth():
    app = FastAPI()
    app.include_router(router, prefix="/api")
    with TestClient(app) as client:
        for key in ("test-api-key", "lampada-test-key-12345678901234567890", "ops-test-key-1234567890123456789012"):
            response = client.get("/api/rights", headers={"X-API-Key": key})
            assert response.status_code == 200
            assert response.json() == REGISTRY.model_dump(mode="json")
        for headers in ({}, {"X-API-Key": "wrong"}, {"X-API-Key": "site-test-key-12345678901234567890123"}):
            assert client.get("/api/rights", headers=headers).status_code == 403
        assert app.openapi()["paths"]["/api/rights"]["get"]["operationId"] == "getRights"
