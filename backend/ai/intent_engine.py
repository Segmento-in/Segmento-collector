"""
backend/ai/intent_engine.py

Intent Engine — parses natural-language messages and returns a
structured intent dict:

    {
        "action":     "connect" | "sync" | "status" | "list" | "help" | "greeting" | "unknown",
        "connectors": ["google_gmail", "airtable", ...]   # canonical names
    }

No connector names are hardcoded here — all lookups go through registry.
"""

from __future__ import annotations
import re
from .registry import ALIAS_INDEX, CONNECTORS

# ──────────────────────────────────────────────
# 1. Action keyword maps
# ──────────────────────────────────────────────

_ACTION_KEYWORDS: dict[str, list[str]] = {
    "connect": [
        "connect", "login", "authorize", "authenticate", "link",
        "enable", "add", "setup", "set up", "integrate",
        "configure", "sign in", "sign-in",
    ],
    "sync": [
        "sync", "synchronize", "run", "fetch", "extract", "pull",
        "import", "refresh", "update", "execute", "trigger",
        "start sync", "begin sync", "start a sync", "do a sync",
    ],
    "disconnect": [
        "disconnect", "unlink", "remove", "disable", "delete",
        "revoke", "turn off", "deactivate",
    ],
    "status": [
        "status", "check", "is connected", "connected", "health",
        "show status", "what is the status",
    ],
    "list": [
        "list", "show", "what connectors", "available connectors",
        "which connectors", "all connectors", "supported connectors",
    ],
    "help": [
        "help", "how do i", "how to", "what can you do",
        "what can i do", "capabilities", "guide",
    ],
    "greeting": [
        "hello", "hi", "hey", "good morning", "good afternoon",
        "good evening", "howdy", "what's up", "sup",
    ],
}


# ──────────────────────────────────────────────
# 2. Action detection
# ──────────────────────────────────────────────

def detect_action(message: str) -> str:
    """
    Return the primary action detected in *message*.
    Priority order: greeting → help → list → connect → disconnect → sync → status → unknown
    """
    lower = message.lower()

    # Check from highest to lowest priority
    priority = ["greeting", "help", "list", "connect", "disconnect", "sync", "status"]

    for action in priority:
        for kw in _ACTION_KEYWORDS[action]:
            # word-boundary match so "sync" doesn't match "syncing" issues
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, lower):
                return action

    return "unknown"


# ──────────────────────────────────────────────
# 3. Connector detection
# ──────────────────────────────────────────────

def _tokenize(text: str) -> list[str]:
    """Return lowercase words and bigrams from text."""
    words = re.sub(r"[^\w\s]", " ", text.lower()).split()
    tokens = list(words)
    # Add bigrams
    for i in range(len(words) - 1):
        tokens.append(f"{words[i]} {words[i+1]}")
    # Add trigrams (for "google search console" etc.)
    for i in range(len(words) - 2):
        tokens.append(f"{words[i]} {words[i+1]} {words[i+2]}")
    return tokens


def detect_connectors(message: str) -> list[str]:
    """
    Return list of canonical connector names mentioned in *message*.
    Uses all aliases from the registry, longest-match wins.
    Deduplication preserves first-seen order.
    """
    tokens = _tokenize(message)
    found: dict[str, int] = {}   # canonical → position of first match

    for i, token in enumerate(tokens):
        canonical = ALIAS_INDEX.get(token)
        # ignore weak matches like just "google"
        if canonical:
            # ensure token is meaningful (not just generic word)
            if len(token.split()) == 1 and token in ["google", "meta", "aws"]:
                continue
            if len(token) < 3:
                continue
        if canonical and canonical not in found:
                found[canonical] = i

    # Sort by position so the result order mirrors the original message
    return [k for k, _ in sorted(found.items(), key=lambda x: x[1])]


# ──────────────────────────────────────────────
# 4. Combined intent detection
# ──────────────────────────────────────────────

def detect_intent(message: str) -> dict:
    """
    Parse *message* and return:

        {
            "action":     str,
            "connectors": list[str],
            "raw":        str       # original message
        }
    """
    if not message or not message.strip():
        return {"action": "unknown", "connectors": [], "raw": message}

    action = detect_action(message)
    connectors = detect_connectors(message)

    # "list" / "help" / "greeting" usually don't need connectors
    return {
        "action": action,
        "connectors": connectors,
        "raw": message,
    }


# ──────────────────────────────────────────────
# 5. Quick smoke test
# ──────────────────────────────────────────────

if __name__ == "__main__":
    samples = [
        "connect gmail and airtable",
        "sync stripe data",
        "hello there",
        "what connectors do you support?",
        "disconnect notion",
        "please fetch my hubspot contacts",
        "check the status of slack",
        "run a sync for google analytics",
    ]
    for s in samples:
        intent = detect_intent(s)
        print(f"[{intent['action']:12s}] connectors={intent['connectors']!r:50s}  ← {s!r}")
