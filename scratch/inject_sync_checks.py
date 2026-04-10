import re

filepath = r"c:\Users\Dell\Desktop\Segmento-app-website-dev\Segmento-collector\backend\api_server.py"

with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

# Registry of sync functions to fix
# Pattern: @app.route("/connectors/<source>/sync")\ndef <func_name>():

def inject_enabled_check(match):
    full_block = match.group(0)
    source = match.group(1)
    
    # Check if it already has the check
    if 'google_connections' in full_block and 'enabled' in full_block:
        return full_block
    
    # Injection code
    injection = f"""
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT enabled FROM google_connections WHERE uid=? AND source=? LIMIT 1", (uid, '{source}'))
    conn_row = fetchone_secure(cur)
    if not conn_row or conn_row.get("enabled") != 1:
        if con: con.close()
        return jsonify({{"error": "Connector is disabled. Please authorize first.", "status": "disconnected"}}), 403
    """
    
    # Find the uid = get_uid() ... if not uid: return ... block
    # and insert after it.
    
    uid_block_pattern = r"(uid = get_uid\(\)\s+if not uid:\s+return jsonify\(.*?\), 401)"
    if re.search(uid_block_pattern, full_block, flags=re.DOTALL):
        new_block = re.sub(uid_block_pattern, r"\1" + injection, full_block, flags=re.DOTALL)
        return new_block
    
    return full_block

# Match @app.route("/connectors/<source>/sync") ... def ...
# We stop at the next route or function
sync_pattern = r'@app.route\("/connectors/(\w+)/sync"\)\n(def \w+\(\):.*?)(?=\n@app.route|\ndef |\Z)'

new_content = re.sub(sync_pattern, inject_enabled_check, content, flags=re.DOTALL)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(new_content)

print("Injected enabled checks into sync routes in api_server.py")
