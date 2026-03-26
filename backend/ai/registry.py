"""
backend/ai/registry.py

Connector Registry — dynamically discovers every connector in
backend/connectors/ and builds alias → canonical-name mappings.
No connector names are hardcoded. Scales to 100+ connectors.
"""

import os
import re

# ──────────────────────────────────────────────
# 1. Dynamic scan of backend/connectors/
# ──────────────────────────────────────────────

def _connectors_dir() -> str:
    """Return absolute path to backend/connectors/, robust to cwd."""
    here = os.path.dirname(os.path.abspath(__file__))          # backend/ai/
    return os.path.join(here, "..", "connectors")              # backend/connectors/


def _scan_connectors() -> list[str]:
    """Return sorted list of connector canonical names from filenames."""
    cdir = _connectors_dir()
    names = []
    try:
        for fname in os.listdir(cdir):
            if fname.startswith("_") or not fname.endswith(".py"):
                continue
            names.append(fname[:-3])   # strip .py
    except FileNotFoundError:
        pass
    return sorted(names)


CONNECTORS: list[str] = _scan_connectors()

# ──────────────────────────────────────────────
# 2. Display-name / alias map
#    Generated automatically + a few hand-crafted overrides
# ──────────────────────────────────────────────

def _build_display_names(connectors: list[str]) -> dict[str, list[str]]:
    """
    For every connector create default aliases from its filename, then
    apply a curated override table so common human phrases also match.
    """
    mapping: dict[str, list[str]] = {}

    for name in connectors:
        aliases = {name}
        # "google_gmail" → ["google gmail", "gmail"]
        human = name.replace("_", " ")
        aliases.add(human)
        parts = name.split("_")
        if len(parts) > 1:
            # add each individual word as an alias
            for p in parts:
                aliases.add(p)
            # add last word(s) joined ("google_gmail" → "gmail")
            aliases.add(" ".join(parts[1:]))
        mapping[name] = sorted(aliases - {""})

    # ── Curated overrides (additive) ──────────────────────────
    _OVERRIDES: dict[str, list[str]] = {
        "google_gmail":          ["gmail", "google mail", "google gmail"],
        "google_drive":          ["drive", "google drive", "gdrive"],
        "google_sheets":         ["sheets", "google sheets", "gsheets"],
        "google_calendar":       ["calendar", "google calendar"],
        "google_contacts":       ["contacts", "google contacts"],
        "google_tasks":          ["tasks", "google tasks"],
        "google_ga4":            ["ga4", "analytics", "google analytics", "google analytics 4"],
        "google_search_console": ["search console", "gsc", "google search console"],
        "google_youtube":        ["youtube", "google youtube"],
        "google_gcs":            ["gcs", "google cloud storage"],
        "google_webfonts":       ["webfonts", "google fonts"],
        "google_pagespeed":      ["pagespeed", "lighthouse", "google pagespeed"],
        "google_forms":          ["forms", "google forms"],
        "facebook_pages":        ["facebook", "facebook pages", "fb pages", "fb"],
        "facebook_ads":          ["facebook ads", "fb ads", "meta ads"],
        "google_classroom":      ["classroom", "google classroom"],
        "classroom":             ["classroom", "google classroom"],
        "aws_rds":               ["rds", "aws rds", "amazon rds"],
        "amazon_seller":         ["amazon seller", "amazon", "amz"],
        "zoho_crm":              ["zoho", "zoho crm"],
        "power_bi":              ["power bi", "powerbi", "microsoft power bi"],
        "azure_blob":            ["azure", "azure blob", "azure storage"],
        "stackoverflow":         ["stack overflow", "stackoverflow"],
        "openstreetmap":         ["osm", "open street map", "openstreetmap"],
        "googlenews":            ["google news", "news"],
        "googletrends":          ["google trends", "trends"],
        "googlebooks":           ["google books", "books"],
        "googlefactcheck":       ["fact check", "google fact check"],
    }

    for canonical, extra_aliases in _OVERRIDES.items():
        if canonical in mapping:
            existing = set(mapping[canonical])
            existing.update(extra_aliases)
            mapping[canonical] = sorted(existing)

    return mapping


DISPLAY_NAMES: dict[str, list[str]] = _build_display_names(CONNECTORS)

# ──────────────────────────────────────────────
# 3. Reverse lookup: alias → canonical name
# ──────────────────────────────────────────────

def _build_alias_index(display_names: dict[str, list[str]]) -> dict[str, str]:
    """Map every lowercase alias back to its canonical connector name."""
    index: dict[str, str] = {}
    for canonical, aliases in display_names.items():
        index[canonical.lower()] = canonical          # canonical maps to itself
        for alias in aliases:
            index[alias.lower()] = canonical
    return index


ALIAS_INDEX: dict[str, str] = _build_alias_index(DISPLAY_NAMES)

# Helpers
def resolve_alias(alias: str) -> str | None:
    """Return canonical connector name for an alias, or None if unknown."""
    return ALIAS_INDEX.get(alias.lower().strip())


def list_connectors() -> list[str]:
    """Return fresh list of connector names (re-scans filesystem)."""
    return _scan_connectors()

# ──────────────────────────────────────────────
# 4. Frontend template mapping (CRITICAL FIX)
# ──────────────────────────────────────────────

def _templates_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))   # backend/ai/
    return os.path.join(here, "..", "..", "frontend", "templates", "connectors")


def _scan_templates() -> set[str]:
    """Get all available frontend connector template names."""
    tdir = _templates_dir()
    names = set()
    try:
        for fname in os.listdir(tdir):
            if fname.endswith(".html"):
                names.add(fname.replace(".html", ""))
    except FileNotFoundError:
        pass
    return names


TEMPLATES = _scan_templates()


def _generate_slug(canonical: str) -> str:
    """
    Convert backend connector name → frontend template name
    """
    name = canonical

    # remove common prefixes
    prefixes = ["google_", "facebook_", "aws_", "azure_"]
    for p in prefixes:
        if name.startswith(p):
            name = name[len(p):]

    # remove underscores
    name = name.replace("_", "")

    return name


def get_connector_url(name: str) -> str:
    """
    Return correct frontend URL for a connector.
    Handles mismatches between backend naming and frontend templates.
    """

    # 1. try direct match
    if name in TEMPLATES:
        return f"/connectors/{name}"

    # 2. try generated slug
    slug = _generate_slug(name)
    if slug in TEMPLATES:
        return f"/connectors/{slug}"

    # 3. special cases (fallback for edge mismatches)
    SPECIAL_MAP = {
        "facebook_pages": "facebookpages",
        "google_ga4": "ga4",
        "google_search_console": "search_console",
        "google_gcs": "gcs",
        "google_sheets": "sheets",
        "google_tasks": "tasks",
        "google_forms": "forms",
        "google_contacts": "contacts",
        "google_webfonts": "webfonts",
        "google_youtube": "youtube",
    }

    if name in SPECIAL_MAP:
        mapped = SPECIAL_MAP[name]
        if mapped in TEMPLATES:
            return f"/connectors/{mapped}"

    # 4. last fallback (safe)
    return f"/connectors/{slug}"


if __name__ == "__main__":
    print(f"Found {len(CONNECTORS)} connectors:")
    for c in CONNECTORS:
        print(f"  {c:40s} → {DISPLAY_NAMES.get(c, [])}")
