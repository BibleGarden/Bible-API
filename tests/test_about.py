from fastapi import FastAPI
from fastapi.testclient import TestClient

from about import ABOUT_DATA, router

app = FastAPI()
app.include_router(router, prefix="/api")
client = TestClient(app)
headers = {"X-API-Key": "test-api-key"}


def test_default_and_explicit_bible_garden_preserve_existing_response():
    for path in ("/api/about", "/api/about?app=bible-garden"):
        response = client.get(path, headers=headers)
        assert response.status_code == 200
        assert response.json() == ABOUT_DATA


def test_lampada_uses_own_website_and_preserves_shared_contacts():
    response = client.get("/api/about?app=lampada", headers=headers)
    assert response.status_code == 200
    data = response.json()
    website = next(item for item in data["contacts"] if item["id"] == "website")
    assert website["url"] == "https://lampada.bible.garden"
    assert website["subtitle"] == dict.fromkeys(("en", "ru", "uk"), website["url"])
    assert data["contacts"][:2] == ABOUT_DATA["contacts"][:2]
    assert "Лампада" in data["about_text"]["ru"]
    assert client.get("/api/about", headers=headers).json() == ABOUT_DATA
    assert ABOUT_DATA["contacts"][2]["url"] == "https://bible.garden"


def test_unknown_application_is_rejected():
    assert client.get("/api/about?app=unknown", headers=headers).status_code == 422


def test_application_selection_still_requires_api_key():
    assert client.get("/api/about?app=lampada").status_code == 403


def test_openapi_documents_optional_application_selector():
    operation = app.openapi()["paths"]["/api/about"]["get"]
    parameter = next(item for item in operation["parameters"] if item["name"] == "app")
    assert parameter["required"] is False
    assert parameter["schema"]["default"] == "bible-garden"
    assert parameter["schema"]["enum"] == ["bible-garden", "lampada"]
