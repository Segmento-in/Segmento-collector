"""
backend/ai/executor.py

Execution Engine — converts a structured intent into a real platform
action and returns a rich response dict consumed by the API layer.

Response contract:
    {
        "type":       "connect" | "sync" | "status" | "list" | "help" | "greeting" | "clarification" | "error",
        "message":    str,          # human-readable, shown in the chat panel
        "connectors": list[str],    # canonical names involved (may be empty)
        "links":      list[str],    # frontend URLs (for connect / status)
        "data":       dict | None,  # extra payload (sync results, list, etc.)
    }
"""
from __future__ import annotations
from backend.ai.route_executor import call_connector_route
from .registry import CONNECTORS, get_connector_url, DISPLAY_NAMES

import time
# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def normalize_source(connector: str) -> str:
    """
    Convert canonical name → API source name
    """
    # remove common prefixes
    if connector.startswith("google_"):
        return connector.replace("google_", "")
    if connector.startswith("facebook_"):
        return connector.replace("facebook_", "")
    if connector.startswith("aws_"):
        return connector.replace("aws_", "")
    if connector.startswith("azure_"):
        return connector.replace("azure_", "")

    return connector

def get_latest_rows_pushed(uid, source):
    from backend.api_server import get_db

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT rows_pushed
        FROM destination_push_logs
        WHERE uid=? AND source=?
        ORDER BY pushed_at DESC
        LIMIT 1
    """, (uid, source))

    row = cur.fetchone()
    con.close()

    return row[0] if row else 0
    
def _pretty(name: str) -> str:
    """Turn 'google_gmail' → 'Google Gmail' for display."""
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
# Main executor
# ──────────────────────────────────────────────

def execute_intent(intent: dict, uid: str) -> dict:
    """
    Execute *intent* for *uid* and return a response dict.

    Parameters
    ----------
    intent : dict
        Output of ``detect_intent()`` — must have keys ``action`` and
        ``connectors``.
    uid : str
        Authenticated user ID.
    """
    action     = intent.get("action", "unknown")
    connectors = intent.get("connectors", [])

    # ── Greeting ──────────────────────────────────────────────
    if action == "greeting":
        return _response(
            "greeting",
            "Hi there! 👋 I'm your Segmento AI Companion.\n\n"
            "I can help you:\n"
            "• **Connect** connectors (\"connect gmail and airtable\")\n"
            "• **Sync** data (\"sync stripe data\")\n"
            "• **Check status** of any connector\n"
            "• **List** all available connectors\n\n"
            "What would you like to do?",
        )

    # ── Help ──────────────────────────────────────────────────
    if action == "help":
        return _response(
            "help",
            "Here's what I can do:\n\n"
            "**Connect a connector** — e.g. \"connect gmail\"\n"
            "**Sync data** — e.g. \"sync stripe\"\n"
            "**Disconnect** — e.g. \"disconnect notion\"\n"
            "**Check status** — e.g. \"status of slack\"\n"
            "**List connectors** — e.g. \"show all connectors\"\n\n"
            f"I currently know about **{len(CONNECTORS)} connectors**. "
            "Just tell me what you need!",
        )

    # ── List all connectors ───────────────────────────────────
    if action == "list":
        names = [_pretty(c) for c in CONNECTORS]
        preview = names[:20]
        tail = f" … and {len(names) - 20} more." if len(names) > 20 else ""
        return _response(
            "list",
            f"I support **{len(CONNECTORS)} connectors**:\n\n"
            + ", ".join(preview) + tail
            + "\n\nTell me which one you'd like to connect or sync!",
            data={"connectors": CONNECTORS},
        )

    # ── No connectors detected → ask for clarification ────────
    if not connectors and action in ("connect", "sync", "disconnect", "status"):
        return _response(
            "clarification",
            f"I'd love to help you **{action}** — which connector(s) are you referring to?\n\n"
            "For example: \"connect gmail\", \"sync stripe\", \"status of slack\".",
        )

    # ── Connect ───────────────────────────────────────────────
    if action == "connect":
        results = []
        messages = []

        for connector in connectors:
            source = normalize_source(connector)

            # STEP 1: REAL STATUS CHECK (FIXED)
            status_outcome = call_connector_route(f"/api/status/{source}", uid)
            status_data = status_outcome.get("data") or {}

            is_connected = False
            if isinstance(status_data, dict):
                is_connected = (
                    status_data.get("connected") == True
                    or status_data.get("enabled") == 1
                    or status_data.get("status") == "connected"
                )

            if is_connected:
                messages.append(f"✅ **{_pretty(connector)}** — already connected")
                results.append((connector, {"status": "already_connected"}))
                continue

            # STEP 2: CONNECT USING REAL ROUTE
            outcome = call_connector_route(f"/connectors/{source}/connect", uid)
            data = outcome.get("data") or {}

            if outcome["ok"]:
                messages.append(f"🔐 **{_pretty(connector)}** — authentication required or started")
            else:
                messages.append(f"⚠️ **{_pretty(connector)}** — requires manual setup or credentials")

            results.append((connector, outcome))

        return _response(
            "connect",
            "Connecting your selected connectors...\n\n" + "\n".join(messages),
            connectors=connectors,
            links=_links_for(connectors),
            data={"results": results},
        )

    # ── Sync ──────────────────────────────────────────────────
    if action == "sync":
        results  = []
        failed   = []
        messages = []

        for connector in connectors:
            source = normalize_source(connector)

            outcome = call_connector_route(f"/connectors/{source}/sync", uid)

            if outcome["ok"]:
                results.append(connector)

                time.sleep(1.2)
                rows = get_latest_rows_pushed(uid, source)

                if rows == 0:
                    messages.append(f"ℹ️ **{_pretty(connector)}** — no new data to sync")
                else:
                    messages.append(f"✅ **{_pretty(connector)}** — {rows} rows synced")

            else:
                failed.append(connector)
                messages.append(
                    f"⚠️ **{_pretty(connector)}** — failed"
                )

        if results and not failed:
            summary = f"Sync started for **{', '.join(_pretty(c) for c in results)}**!"
        elif failed and not results:
            summary = f"Couldn't sync **{', '.join(_pretty(c) for c in failed)}**."
        else:
            summary = f"Partial sync: {len(results)} succeeded, {len(failed)} failed."

        return _response(
            "sync",
            summary + "\n\n" + "\n".join(messages),
            connectors=connectors,
            links=_links_for(connectors),
            data={"synced": results, "failed": failed},
        )

    # ── Disconnect ────────────────────────────────────────────
    if action == "disconnect":
        links  = _links_for(connectors)
        labels = [_pretty(c) for c in connectors]
        return _response(
            "connect",   # reuse connect card with disconnect context
            f"To disconnect **{', '.join(labels)}**, visit the connector page and click Disconnect.",
            connectors=connectors,
            links=links,
        )

    # ── Status ────────────────────────────────────────────────
    if action == "status":
        links  = _links_for(connectors)
        labels = [_pretty(c) for c in connectors]
        return _response(
            "connect",
            f"Check the current status of **{', '.join(labels)}** on the connector page.",
            connectors=connectors,
            links=links,
        )

    # ── Unknown / fallback ────────────────────────────────────
    # Even if the action is unknown, if connectors were detected we can
    # at least surface their pages.
    if connectors:
        links  = _links_for(connectors)
        labels = [_pretty(c) for c in connectors]
        return _response(
            "connect",
            f"I found these connectors in your message: **{', '.join(labels)}**. "
            "Here are the links — let me know what you'd like to do with them!",
            connectors=connectors,
            links=links,
        )

    return _response(
        "clarification",
        "I'm not sure what you'd like to do. You can ask me to:\n"
        "• **Connect** a connector — e.g. \"connect gmail\"\n"
        "• **Sync** data — e.g. \"sync stripe\"\n"
        "• **List** all connectors — \"show all connectors\"\n"
        "• **Help** — just say \"help\"",
    )
