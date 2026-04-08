import os
import re

file_path = r"c:\Users\HP\OneDrive\Desktop\PROJECTS\Segmento_Collector\backend\api_server.py"

replacements = [
    (r'xero\.connect_xero\(\)', 'xero.connect_xero(uid=uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'tiktok\.get_tiktok_auth_url\(uid\)', 'tiktok.get_tiktok_auth_url(uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'pinterest\.pinterest_get_auth_url\(uid\)', 'pinterest.pinterest_get_auth_url(uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'instagram\.get_auth_url\(uid\)', 'instagram.get_auth_url(uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'linkedin\.get_linkedin_auth_url\(uid\)', 'linkedin.get_linkedin_auth_url(uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'quickbooks\.connect_quickbooks\(\)', 'quickbooks.connect_quickbooks(uid=uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'amazon_seller\.connect_amazon_seller\(\)', 'amazon_seller.connect_amazon_seller(uid=uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'x\.handle_x_connect\(uid\)', 'x.handle_x_connect(uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'github\.connect_github\(uid\)', 'github.connect_github(uid, redirect_uri=get_base_url() + "/oauth/callback")'),
    (r'gitlab\.get_auth_url\(uid\)', 'gitlab.get_auth_url(uid, redirect_uri=get_base_url() + "/oauth/callback")'),
]

def update_api_server_connectors():
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    update_api_server_connectors()
