import os
import re

dir_path = r"c:\Users\Dell\Desktop\Segmento-app-website-dev\Segmento-collector\backend\connectors"

def fix_backend_disconnect(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Identify the connector source name from the file (e.g. SOURCE = 'airtable')
    source_match = re.search(r"SOURCE\s*=\s*['\"](\w+)['\"]", content)
    if not source_match:
        return
    source = source_match.group(1)
    
    # We want to find the disconnect_<source> function
    pattern = rf"def disconnect_{source}\(uid: str\) -> dict:(.*?)(?=\ndef |\Z)"
    match = re.search(pattern, content, flags=re.DOTALL)
    
    if match:
        body = match.group(1)
        # Check if it already returns the standardized JSON
        if 'return {"status": "success"}' in body and 'return {"status": "error"' in body:
            return # Already fixed
            
        print(f"Fixing disconnect for {source}...")
        
        # New standardized body
        new_body = """
    try:
        _set_connection_enabled(uid, False)
        _update_status(uid, "disconnected")
        _log(f"Disconnected uid={uid}")
        return {"status": "success"}
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}
"""
        new_content = content[:match.start(1)] + new_body + content[match.end(1):]
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_content)

# Special cases or OAuth connectors might need manual handling, but standardizing non-OAuth ones first.
# OAuth connectors like Gmail are usually in api_server.py routes (google/disconnect/...)
# but some might be in connectors/*.py (e.g. Hubspot, Salesforce)

oauth_connectors = ['gmail', 'google_gmail', 'outlook', 'facebook', 'facebook_ads', 'linkedin', 'github', 'instagram', 'tiktok']

for filename in os.listdir(dir_path):
    if filename.endswith(".py") and not filename.startswith("__"):
        fix_backend_disconnect(os.path.join(dir_path, filename))
