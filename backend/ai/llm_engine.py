import requests
import json

OLLAMA_URL = "http://localhost:11434/api/generate"

SYSTEM_PROMPT = """
You are an AI assistant that converts user requests into structured JSON actions.

This platform supports:
- Connecting connectors (with credentials or OAuth)
- Disconnecting connectors
- Running syncs
- Setting or changing destinations
- Scheduling sync jobs
- Querying platform stats (usage, records, connectors, destinations)
- Handle spelling mistakes and variations (e.g., gmial → gmail)

Your job:
Understand the user's intent and return structured JSON.

Return ONLY JSON.

Schema:
{
  "action": "connect | disconnect | sync | destination | schedule | query | help | unknown",
  "connector": "connector_name_if_any",
  "time": "HH:MM if mentioned",
  "target": "optional (destination / stats type)"
}

Rules:
- Extract connector names (gmail, airtable, notion, stripe, etc.)
- Normalize connector names to lowercase
- Convert time like "7 pm" → "19:00"
- If no connector mentioned, leave it null
- If unclear, return action="unknown"
- Do NOT explain anything
"""

def call_llm(message: str):
    try:
        res = requests.post(OLLAMA_URL, json={
            "model": "phi3",
            "prompt": f"{SYSTEM_PROMPT}\n\nUser: {message}\nJSON:",
            "stream": False
        })

        text = res.json().get("response", "").strip()

        import re

        # Extract JSON safely
        match = re.search(r"\{.*\}", text, re.DOTALL)

        if match:
            try:
                return json.loads(match.group(0))
            except Exception as e:
                print("[LLM PARSE ERROR]", e, flush=True)

        print("[LLM RAW TEXT]", text, flush=True)

        return {"action": "unknown"}

    except Exception as e:
        return {
            "action": "unknown",
            "error": str(e)
        }