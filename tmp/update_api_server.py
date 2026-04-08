import os
import re

file_path = r"c:\Users\HP\OneDrive\Desktop\PROJECTS\Segmento_Collector\backend\api_server.py"

# Patterns to replace
legacy_pattern = re.compile(r'request\.host_url\.replace\("/_backend", ""\)\.rstrip\("/"\)')
legacy_pattern_no_rstrip = re.compile(r'request\.host_url\.replace\("/_backend", ""\)')
# Pattern for oauth2callback route
oauth2callback_route = re.compile(r'@app\.route\("/oauth2callback"\)')
# Pattern for some other hardcoded local callbacks if any
x_callback_pattern = re.compile(r'request\.host_url\.rstrip\("/"\) \+ "/connectors/x/callback"')

def update_api_server():
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Replace legacy host_url patterns with get_base_url()
    content = legacy_pattern.sub('get_base_url()', content)
    content = legacy_pattern_no_rstrip.sub('get_base_url()', content)
    
    # 2. Update google_callback route to unified /oauth/callback
    content = oauth2callback_route.sub('@app.route("/oauth/callback")', content)
    
    # 3. Handle X callback special case found in previous view
    content = x_callback_pattern.sub('get_base_url() + "/oauth/callback"', content)
    
    # 4. We also need to check for /connectors/linkedin/callback etc. in instructions
    # but those are mostly in the templates which we already updated.
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    update_api_server()
