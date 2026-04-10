import re
import os

filepath = r"c:\Users\Dell\Desktop\Segmento-app-website-dev\Segmento-collector\backend\api_server.py"

with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

# Pattern to find status endpoints and their return logic
# We want to ensure "connected" is based on conn_row and conn_row.get("enabled") == 1

def fix_status_logic(match):
    before = match.group(0)
    # If it already has the standard check, leave it
    if 'bool(conn_row and conn_row.get("enabled") == 1)' in before:
        return before
    
    # If it has a conn_row check but might be different
    # Example: "connected": bool(cfg_row) -> change to "connected": bool(conn_row and conn_row.get("enabled") == 1)
    
    # Make sure conn_row is fetched
    if 'conn_row =' not in before:
        # We need to find where cfg_row is fetched and add conn_row fetch after it
        # This might be too complex for a simple regex if the structure varies too much.
        return before

    fixed = re.sub(r'"connected":\s*bool\(.*?\)', r'"connected": bool(conn_row and conn_row.get("enabled") == 1)', before)
    return fixed

# Apply to all @app.route("/api/status/...) blocks
# Using a broad match for the function body
pattern = r'@app.route\("/api/status/\w+"\).*?return jsonify\(.*?\)'
# Wait, return jsonify might be multiline or at different indentations.

# Simpler: just replace the "connected" line globally where it appears inside status-like contexts?
# No, that's risky.

# Let's try a more targeted approach for the common pattern
# "connected": bool(cfg_row) -> "connected": bool(conn_row and conn_row.get("enabled") == 1)

new_content = re.sub(r'"connected":\s*bool\(cfg_row\)', r'"connected": bool(conn_row and conn_row.get("enabled") == 1)', content)

# Also fix "has_credentials": bool(cfg_row) to be more accurate if row exists
# new_content = re.sub(r'"has_credentials":\s*bool\(cfg_row\)', r'"has_credentials": bool(cfg_row and cfg_row.get("config_json"))', new_content)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(new_content)

print("Standardized status fields in api_server.py")
