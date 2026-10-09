from __future__ import annotations

import logging
import unicodedata

logger = logging.getLogger(__name__)
MAX_RESOURCES = 100
MAX_RESOURCE_BYTES = 1024 * 1024
MAX_TOTAL_RESOURCE_BYTES = 5 * MAX_RESOURCE_BYTES
MAX_WORKER_INPUT_BYTES = 8 * MAX_RESOURCE_BYTES
RESERVED_NAMES = frozenset({"con", "prn", "aux", "nul", "conin$", "conout$"}
                           | {f"{prefix}{i}" for prefix in ("com", "lpt") for i in "123456789¹²³"})
RESERVED_FILES = frozenset({"dbt_project.yml", "profiles.yml", "packages.yml", "dependencies.yml", "selectors.yml"})


def normalize_resources(value: object) -> dict[str, str]:
    """Validate before filtering; never parse or strip a nonblank resource."""
    if not isinstance(value, dict) or len(value) > MAX_RESOURCES:
        raise ValueError("resources must be a bounded filename mapping")
    names: set[str] = set()
    total = 0
    result: dict[str, str] = {}
    for name, raw in value.items():
        if not isinstance(name, str) or not isinstance(raw, str):
            raise ValueError("resource names and contents must be strings")
        folded = name.casefold()
        contains_forbidden_character = any(
            character in '/\\<>:"|?*' or unicodedata.category(character).startswith("C")
            for character in name
        )
        if (
            not 1 <= len(name) <= 128
            or not name.endswith((".yml", ".yaml"))
            or name.startswith(".")
            or contains_forbidden_character
            or name.split(".")[0].rstrip(" ").casefold() in RESERVED_NAMES
            or folded in RESERVED_FILES
            or folded in names
        ):
            raise ValueError("resource filename is invalid or ambiguous")
        names.add(folded)
        try:
            size = len(raw.encode("utf-8"))
        except UnicodeError as error:
            raise ValueError("resource contents must be UTF-8") from error
        total += size
        if size > MAX_RESOURCE_BYTES or total > MAX_TOTAL_RESOURCE_BYTES:
            raise ValueError("resource contents exceed size limit")
        if raw.strip():
            result[name] = raw
    return result
