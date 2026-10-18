"""Reviewed rights metadata, independent of the Bible content databases."""

from datetime import date
from pathlib import Path
from typing import Annotated, Literal

import yaml
from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, StringConstraints, model_validator

from auth import RequireAPIKey

Text = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1)]
Alias = Annotated[str, StringConstraints(strict=True, pattern=r"^[a-z][a-z0-9_]*$")]


class RightsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LocalizedNotice(RightsModel):
    en: Text
    ru: Text
    uk: Text


class RightsLicense(RightsModel):
    id: Literal["CC0-1.0", "CC-BY-SA-4.0"]
    name: Text
    url: HttpUrl

    @model_validator(mode="after")
    def check_license_url(self):
        expected = {
            "CC0-1.0": "https://creativecommons.org/publicdomain/zero/1.0/",
            "CC-BY-SA-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
        }
        if str(self.url) != expected[self.id]:
            raise ValueError("license URL must match the license id")
        return self


class RightsItem(RightsModel):
    kind: Literal["text", "audio"]
    translation_alias: Alias
    voice_alias: Alias | None = None
    title: LocalizedNotice
    rights_holder: Text | None
    credit: LocalizedNotice | None = None
    copyright: Text | None = None
    license: RightsLicense | None = None
    public_domain_basis: LocalizedNotice | None = None
    sources: list[HttpUrl] = Field(min_length=1)
    attribution: LocalizedNotice
    changes: LocalizedNotice
    trademark_required: bool = Field(strict=True)
    trademark: LocalizedNotice | None = Field(
        default=None, description="Trademark restrictions, including removal terms for adaptations."
    )
    checked_on: date

    @model_validator(mode="after")
    def check_material(self):
        if (self.kind == "audio") != (self.voice_alias is not None):
            raise ValueError("only audio entries require a voice_alias")
        if (self.license is None) == (self.public_domain_basis is None):
            raise ValueError("provide exactly one license or public-domain basis")
        if self.license and self.license.id == "CC-BY-SA-4.0" and not self.copyright:
            raise ValueError("CC BY-SA attribution requires a copyright line")
        if self.public_domain_basis is not None or (self.license and self.license.id == "CC0-1.0"):
            if self.rights_holder is not None:
                raise ValueError("public-domain materials have no rights holder; use localized credit")
        if self.trademark_required and self.trademark is None:
            raise ValueError("material terms require a localized trademark terms notice")
        if self.checked_on > date.today():
            raise ValueError("checked_on must not be in the future")
        return self


class RightsRegistry(RightsModel):
    schema_version: Literal[1]
    items: dict[str, RightsItem] = Field(min_length=1)

    @model_validator(mode="after")
    def check_ids(self):
        for item_id, item in self.items.items():
            expected = f"{item.kind}:{item.translation_alias}"
            if item.voice_alias is not None:
                expected += f":{item.voice_alias}"
            if item_id != expected:
                raise ValueError(f"rights id must match material aliases: {expected}")
        return self


class RegistryLoader(yaml.SafeLoader):
    """Reject duplicate YAML keys instead of losing a reviewed notice."""


def _unique_mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in result:
            raise ValueError(f"duplicate rights registry key: {key}")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


RegistryLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping)
REGISTRY_PATH = Path(__file__).with_name("rights.yaml")


def load_registry(path: Path = REGISTRY_PATH) -> RightsRegistry:
    try:
        return RightsRegistry.model_validate(yaml.load(path.read_text(encoding="utf-8"), Loader=RegistryLoader))
    except (OSError, ValueError, yaml.YAMLError) as error:
        raise RuntimeError(f"Invalid rights registry {path}: {error}") from error


# Validated while the application imports its routers, before it can serve.
REGISTRY = load_registry()
router = APIRouter()


@router.get("/rights", response_model=RightsRegistry, operation_id="getRights", tags=["Rights"])
def get_rights(api_key: bool = RequireAPIKey):
    """Confirmed terms for individual texts and recordings; not a complete catalog."""
    return REGISTRY
