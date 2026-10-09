import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import version_check as versions

app = FastAPI()
app.include_router(versions.router, prefix="/api")
client = TestClient(app)
headers = {"X-API-Key": "test-api-key"}

APP_STORE_URL = "https://apps.apple.com/app/id6806024678"
PLAY_STORE_URL = "https://play.google.com/store/apps/details?id=com.nf404.twinkler"


def request(version, application="lampada", platform=None):
    params = {"app_version": version, "app": application}
    if platform is not None:
        params["platform"] = platform
    return client.get('/api/version-check', params=params, headers=headers)


@pytest.mark.parametrize("platform,store_url", [("ios", APP_STORE_URL), ("android", PLAY_STORE_URL)])
def test_unpublished_lampada_never_blocks(platform, store_url):
    data = request("0.1", platform=platform).json()
    assert data["platform"] == platform
    assert data["update_type"] == "none"
    assert data["store_url"] == store_url


def test_missing_platform_is_answered_for_ios():
    implicit = request("1.0.0").json()
    assert implicit == request("1.0.0", platform="ios").json()
    assert implicit["platform"] == "ios"
    assert implicit["store_url"] == APP_STORE_URL


@pytest.mark.parametrize("version,expected", [("0.9", "hard"), ("1.0", "soft"), ("1.0.0", "soft"), ("1.1", "none"), ("2.0", "none")])
@pytest.mark.parametrize("platform", ["ios", "android"])
def test_lampada_policy(monkeypatch, platform, version, expected):
    monkeypatch.setitem(versions.LAMPADA_UPDATES_ENABLED, platform, True)
    monkeypatch.setitem(versions.LAMPADA_LATEST_VERSION, platform, '1.1.0')
    response = request(version, platform=platform)
    assert response.status_code == 200
    data = response.json()
    assert data['platform'] == platform
    assert data['update_type'] == expected
    if expected != 'none':
        assert 'Lampada' in data['message']['ru']


def test_platform_policies_are_independent(monkeypatch):
    monkeypatch.setitem(versions.LAMPADA_UPDATES_ENABLED, "android", True)
    monkeypatch.setitem(versions.LAMPADA_MIN_SUPPORTED_VERSION, "android", "2.0.0")
    monkeypatch.setitem(versions.LAMPADA_LATEST_VERSION, "android", "2.0.0")
    android = request("1.0.0", platform="android").json()
    assert android["update_type"] == "hard"
    assert android["latest_version"] == "2.0.0"
    assert android["store_url"] == PLAY_STORE_URL
    ios = request("1.0.0", platform="ios").json()
    assert ios["update_type"] == "none"
    assert ios["latest_version"] == "1.0.0"
    assert ios["store_url"] == APP_STORE_URL


@pytest.mark.parametrize("platform", ["ios", "android"])
def test_lampada_policy_constants_are_consistent(platform):
    assert versions.parse_version(versions.LAMPADA_MIN_SUPPORTED_VERSION[platform]) <= versions.parse_version(versions.LAMPADA_LATEST_VERSION[platform])
    assert versions.LAMPADA_STORE_URL[platform].startswith("https://")


def test_legacy_bible_garden_default():
    implicit = client.get('/api/version-check?app_version=1.0', headers=headers)
    assert implicit.json() == request('1.0', 'bible-garden').json()
    assert implicit.json() == request('1.0', 'bible-garden', 'ios').json()
    assert implicit.json()['platform'] == 'ios'
    assert implicit.json()['update_type'] == 'hard'
    assert 'Bible Garden' in implicit.json()['message']['ru']


def test_bible_garden_has_no_android_release():
    assert request('1.0', 'bible-garden', 'android').status_code == 422


@pytest.mark.parametrize('version', ['abc', '1.2-beta', '-1', '1..0', '1.2.3.4'])
def test_invalid_versions_return_422(version):
    assert request(version).status_code == 422


@pytest.mark.parametrize('platform', ['web', 'IOS', ''])
def test_unknown_platform_returns_422(platform):
    assert request('1.0.0', platform=platform).status_code == 422


def test_invalid_app_and_missing_auth():
    assert request('1', 'unknown').status_code == 422
    assert client.get('/api/version-check?app_version=1').status_code == 403
