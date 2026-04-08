import os
import re

directory = r"c:\Users\HP\OneDrive\Desktop\PROJECTS\Segmento_Collector\frontend\templates\connectors"
callback_pattern = re.compile(r'(/_backend/oauth2callback|/_backend/connectors/[^/]+/callback|/_backend/[^/]+/callback|http://localhost:[0-9]+/oauth/callback)')

def replace_in_files():
    for filename in os.listdir(directory):
        if filename.endswith(".html"):
            path = os.path.join(directory, filename)
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # The user specifically mentioned replacing /_backend/... with {{ base_url }}/oauth/callback
            new_content = callback_pattern.sub('{{ base_url }}/oauth/callback', content)
            
            if new_content != content:
                print(f"Updated {filename}")
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(new_content)

if __name__ == "__main__":
    replace_in_files()
