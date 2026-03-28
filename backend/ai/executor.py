"""
backend/ai/executor.py

Stateless Execution Engine — handles simple, single-turn AI intents like
greetings, help, and listing available connectors.
Complex multi-step flows and all platform actions (connect, sync, etc.)
are handled by the Orchestrator.
"""

from __future__ import annotations
from .registry import CONNECTORS, get_connector_url

# ──────────────────────────────────────────────
# 1. HELPERS
# ──────────────────────────────────────────────

def normalize_source(connector: str) -> str:
    """Convert canonical name → API source name."""
    if not connector: return ""
    source = connector.lower()
    for prefix in ["google_", "facebook_", "aws_", "azure_"]:
        if source.startswith(prefix):
            return source.replace(prefix, "")
    return source

def _pretty(name: str) -> str:
    """Turn 'google_gmail' → 'Google Gmail'."""
    return name.replace("_", " ").title()

def _links_for(connectors: list[str]) -> list[str]:
    return [get_connector_url(c) for c in connectors]

def _response(
    rtype: str,
    message: str,
    connectors: list[str] | None = None,
    links: list[str] | None = None,
    data: dict | None = None,
) -> dict:
    return {
        "type":       rtype,
        "message":    message,
        "connectors": connectors or [],
        "links":      links or [],
        "data":       data,
    }

# ──────────────────────────────────────────────
# 2. MAIN EXECUTOR (Stateless Intents)
# ──────────────────────────────────────────────

def execute_intent(intent: dict, uid: str) -> dict:
    action     = intent.get("action", "unknown")
    connectors = intent.get("connectors", [])

    if action == "greeting":
        return _response(
            "greeting",
            "Hi there! 👋 I'm your Segmento AI Companion.\n\n"
            "I can help you build your entire data pipeline using natural language:\n"
            "• **Connect** connectors (\"connect gmail\")\n"
            "• **Manage Destinations** (\"set destination for stripe\")\n"
            "• **Sync & Schedule** (\"run sync\" or \"schedule at 10:00\")\n"
            "• **Query Stats** (\"how many records pushed today?\")\n\n"
            "What would you like to start with?",
        )

    if action == "help":
        return _response(
            "help",
            "I can automate your entire workflow. Try these:\n\n"
            "**1. Connect** — \"connect airtable\"\n"
            "**2. Destination** — \"set destination for airtable\"\n"
            "**3. Sync** — \"sync airtable now\"\n"
            "**4. Schedule** — \"schedule airtable at 09:00\"\n"
            "**5. Query** — \"how many records today?\"\n\n"
            f"I support **{len(CONNECTORS)} connectors**. Just tell me what you need!",
        )

    if action == "list":
        names = [_pretty(c) for c in CONNECTORS]
        preview = names[:20]
        tail = f" … and {len(names) - 20} more." if len(names) > 20 else ""
        return _response(
            "list",
            f"I support **{len(CONNECTORS)} connectors**:\n\n"
            + ", ".join(preview) + tail
            + "\n\nSpecify a connector to get started!",
            data={"connectors": CONNECTORS},
        )

    # All other intents (connect, sync, etc.) are handled by the orchesrator.
    # If they reach here, it means the orchestrator delegation didn't catch them.
    # We return a generic clarification in that case.
    return _response(
        "clarification",
        "I'm not sure how to handle that request directly. Try asking for **help** to see what I can do!",
    )
