import re
import os

filepath = r"c:\Users\Dell\Desktop\Segmento-app-website-dev\Segmento-collector\frontend\ui_server.py"

with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

# Pattern to find disconnect proxy functions and their internal logic
# We look for functions ending in _disconnect_proxy or matching the known list
# and ensure they return safe_backend_json_response(r, include_status=True)

# Example target:
# def airtable_disconnect_proxy():
#     r = connector_disconnect("airtable")
#     return jsonify(r) / return jsonify(res) / return r

def fix_proxy_return(match):
    indent = match.group(1)
    func_name = match.group(2)
    body = match.group(3)
    
    # Standardize the return line
    # We want to find the line that returns r or res or jsonify(r)
    # and replace it with safe_backend_json_response(r, include_status=True)
    
    # First, let's identify what the response variable is called (usually r or res)
    # Most use 'r = connector_disconnect(...)' or 'r = proxy_get(...)'
    
    new_body = body
    
    # Replace 'return jsonify(res)' or 'return jsonify(r)'
    new_body = re.sub(r'return jsonify\((r|res|res_data)\)', r'return safe_backend_json_response(r, include_status=True)', new_body)
    
    # Replace direct 'return r' or 'return res'
    # But be careful not to replace things like 'return redirect'
    new_body = re.sub(r'return (r|res)(?!\w)', r'return safe_backend_json_response(r, include_status=True)', new_body)

    # Ensure the variable being passed is 'r' if it was 'res'
    if 'res = ' in new_body and 'safe_backend_json_response(r' in new_body:
        new_body = new_body.replace('res = ', 'r = ')

    return f"{indent}def {func_name}():{new_body}"

# This regex matches the function definition and its body until the next function or route
pattern = r"(\n)def (\w+_disconnect_proxy)\(\):(\s+.*?)(?=\n@|\ndef |\Z)"

new_content = re.sub(pattern, fix_proxy_return, content, flags=re.DOTALL)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(new_content)

print("Processed ui_server.py proxies.")
