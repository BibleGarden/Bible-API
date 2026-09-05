import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import version_check as versions

app = FastAPI()
app.include_router(versions.router, prefix="/api")
client = TestClient(app)
headers = {"X-API-Key": "test-api-key"}


def request(version, application="lampada"):
    return client.get('/api/version-check', params={"app_version": version, "app": application}, headers=headers)


def test_unpublished_lampada_never_blocks():
    assert request("0.1").json()["update_type"] == "none"
    assert request("1.0.0").json()["store_url"] == "https://apps.apple.com/app/id6806024678"


@pytest.mark.parametrize("version,expected", [("0.9", "hard"), ("1.0", "soft"), ("1.0.0", "soft"), ("1.1", "none"), ("2.0", "none")])
def test_lampada_policy(monkeypatch, version, expected):
    monkeypatch.setattr(versions, 'LAMPADA_UPDATES_ENABLED', True)
    monkeypatch.setattr(versions, 'LAMPADA_LATEST_VERSION', '1.1.0')
    response = request(version)
    assert response.status_code == 200
    data = response.json()
    assert data['update_type'] == expected
    if expected != 'none':
        assert 'Lampada' in data['message']['ru']


def test_legacy_bible_garden_default():
    implicit = client.get('/api/version-check?app_version=1.0', headers=headers)
    assert implicit.json() == request('1.0', 'bible-garden').json()
    assert implicit.json()['update_type'] == 'hard'
    assert 'Bible Garden' in implicit.json()['message']['ru']


@pytest.mark.parametrize('version', ['abc', '1.2-beta', '-1', '1..0', '1.2.3.4'])
def test_invalid_versions_return_422(version):
    assert request(version).status_code == 422


def test_invalid_app_and_missing_auth():
    assert request('1', 'unknown').status_code == 422
    assert client.get('/api/version-check?app_version=1').status_code == 403
