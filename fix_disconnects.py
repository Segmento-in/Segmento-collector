import os, glob, re

target_dir = 'backend/connectors'
count = 0

for file_path in glob.glob(os.path.join(target_dir, '*.py')):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update _set_connection_enabled to have try..except
    pattern_enabled = r'(def _set_connection_enabled\(uid:[^\)]*\):.*?)(?=\n\n|\n[a-z_A-Z0-9]+ =|\ndef )'
    
    def repl_enabled(m):
        func_body = m.group(1)
        if 'try:' in func_body: return func_body # Skip if already wrapped
        
        lines = func_body.split('\n')
        new_lines = [lines[0], '    try:']
        for line in lines[1:]:
            new_lines.append('    ' + line)
        new_lines.append('    except Exception as e:')
        new_lines.append('        pass') # Silently ignore legacy google_connections table errors
        return '\n'.join(new_lines)
        
    new_content = re.sub(pattern_enabled, repl_enabled, content, flags=re.DOTALL)
    
    # 2. Update disconnect_* to have try..except and return status success
    pattern_disc = r'(def disconnect_[a-zA-Z_0-9]+\(uid(?:[^)]*)?\)[^-:]*(?:->[^:]+)?:\n)(.*?)(?=\n\n(?:def |[A-Z0-9_]+ =)|\Z)'
    
    def repl_disc(m):
        header = m.group(1)
        body = m.group(2)
        if 'try:' in body and 'except Exception' in body: return header + body
        
        lines = body.split('\n')
        new_lines = [header, '    try:']
        for line in lines:
            if line.strip().startswith('return'):
                new_lines.append('        return {"status": "success"}')
            else:
                new_lines.append('    ' + line)
                
        # ensure it returns status success if didn't exist
        if not any(x.strip().startswith('return') for x in lines):
            new_lines.append('        return {"status": "success"}')
            
        new_lines.append('    except Exception as e:')
        new_lines.append('        import traceback; traceback.print_exc()')
        new_lines.append('        return {"status": "error", "message": str(e)}')
            
        return '\n'.join(new_lines)

    new_content = re.sub(pattern_disc, repl_disc, new_content, flags=re.DOTALL)

    if new_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        count += 1
        
print(f'Updated {count} connectors.')
