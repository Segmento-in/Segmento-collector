import os, glob, re

target_dir = 'backend/connectors'
count = 0

for file_path in glob.glob(os.path.join(target_dir, '*.py')):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Search for get_active_destination or _get_active_destination 
    # that returns a dictionary mapping and normalize it.
    
    # We want to replace the SQL and the return dict
    
    # Let's target the exact SQL replacing `dest_type, host, port, username, password, database_name`
    # with `dest_type, host, port, username, password, database_name, format`
    sql_pattern = r'SELECT dest_type,\s*host,\s*port,\s*username,\s*password,\s*database_name(\s*)FROM destination_configs'
    new_content = re.sub(sql_pattern, r'SELECT dest_type, host, port, username, password, database_name, format\1FROM destination_configs', content)
    
    # Let's target the return dict to safely use .get() and return "type" and "format" cleanly.
    dict_pattern = r'return \{\s*"type":\s*row\["dest_type"\],\s*"host":\s*row\["host"\],\s*"port":\s*row\["port"\],\s*"username":\s*row\["username"\],\s*"password":\s*row\["password"\],\s*"database_name":\s*row\["database_name"\],?\s*\}'
    
    replacement_dict = '''return {
        "type": row.get("dest_type"),
        "host": row.get("host"),
        "port": row.get("port"),
        "username": row.get("username"),
        "password": row.get("password"),
        "database_name": row.get("database_name"),
        "format": row.get("format")
    }'''

    new_content = re.sub(dict_pattern, replacement_dict, new_content)
    
    if new_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        count += 1

print(f'Updated {count} destination dicts.')
