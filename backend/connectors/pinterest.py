import requests,sqlite3,os,time,json
from datetime import datetime
from dotenv import load_dotenv

from backend.security.token_manager import ensure_valid_pinterest_token
from backend.security.crypto import decrypt_value

load_dotenv()

DB = os.getenv("DB_PATH", "/tmp/identity.db")

AUTH_URL="https://www.pinterest.com/oauth/"
TOKEN_URL="https://api.pinterest.com/v5/oauth/token"
# Local redirect (legacy, now resolved dynamically)
_LEGACY_REDIRECT_URI="/_backend/pinterest/callback"
API_BASE="https://api.pinterest.com/v5"


# ---------------- DB ----------------

def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def get_app_credentials(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector='pinterest'
        LIMIT 1
    """,(uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return json.loads(row[0])

# ---------------- Utils ----------------

def safe_json(o):
    try:
        return json.dumps(o,ensure_ascii=False)
    except:
        return "{}"


# ---------------- Tokens ----------------

def get_token(uid):

    con=db()
    cur=con.cursor()

    cur.execute("""
    SELECT access_token FROM pinterest_tokens
    WHERE uid=?
    """,(uid,))

    row=cur.fetchone()
    con.close()

    return row[0] if row else None


def pinterest_save_token(uid, token, refresh, exp):

    con = db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO pinterest_tokens
        (uid, access_token, refresh_token, expires_at)
        VALUES (?, ?, ?, ?)
    """, (uid, token, refresh, exp))

    con.close()

# ---------------- OAuth ----------------

def pinterest_get_auth_url(uid, redirect_uri=None):

    cfg = get_app_credentials(uid)

    if not cfg:
        return None

    from urllib.parse import urlencode

    # Use provided redirect_uri or fallback to legacy logic
    final_redirect_uri = redirect_uri or _LEGACY_REDIRECT_URI

    params = {
        "response_type": "code",
        "client_id": cfg["client_id"],
        "redirect_uri": final_redirect_uri,
        "scope": "boards:read,pins:read,user_accounts:read",
        "state": "pinterest" # Pass connector name for unified routing
    }

    return AUTH_URL + "?" + urlencode(params)

import base64


def pinterest_exchange_code(uid, code, redirect_uri=None):

    cfg = get_app_credentials(uid)

    if not cfg:
        return None

    import base64

    client_id = cfg["client_id"]
    if client_id.startswith("gAAAA"):
        client_id = decrypt_value(client_id)

    client_secret = decrypt_value(cfg["client_secret"])

    auth_str = f"{client_id}:{client_secret}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()

    final_redirect_uri = redirect_uri or _LEGACY_REDIRECT_URI

    headers = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": final_redirect_uri
    }

    response = requests.post(
        "https://api.pinterest.com/v5/oauth/token",
        headers=headers,
        data=data,
        timeout=20
    )

    print("Pinterest token response:", response.text, flush=True)

    if response.status_code == 200:
        return response.json()

    print("Pinterest token error:", response.text, flush=True)
    return None

# ---------------- HTTP ----------------

def api_get(uid,path,params=None):

    token=ensure_valid_pinterest_token(uid)

    if not token:
        return None


    headers={
        "Authorization":f"Bearer {token}",
        "Content-Type":"application/json"
    }

    url=API_BASE+path

    try:
        r=requests.get(url,headers=headers,params=params,timeout=20)

        if r.status_code==200:
            return r.json()

        print("PINTEREST API ERROR:",r.text[:200], flush=True)

    except Exception as e:
        print("PINTEREST ERROR:",e, flush=True)

    return None

def fetch_user(uid):

    res=api_get(uid,"/user_account")

    if res and "username" in res:
        return res

    return None


# ---------------- Inserts ----------------

def insert_boards(uid,rows):

    con=db()
    cur=con.cursor()

    now=datetime.utcnow().isoformat()

    data=[]

    for b in rows:

        data.append((
            uid,
            b.get("id"),
            b.get("name"),
            b.get("description"),
            b.get("privacy"),
            b.get("url"),
            safe_json(b),
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO pinterest_boards
    (uid,board_id,name,description,privacy,url,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?)
    """,data)

    con.close()


def insert_pins(uid,rows):

    con=db()
    cur=con.cursor()

    now=datetime.utcnow().isoformat()

    data=[]

    for p in rows:

        media=p.get("media") or {}

        data.append((
            uid,
            p.get("id"),
            p.get("board_id"),
            p.get("title"),
            p.get("description"),
            p.get("link"),
            media.get("url"),
            p.get("created_at"),
            safe_json(p),
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO pinterest_pins
    (uid,pin_id,board_id,title,description,link,
     media_url,created_at,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


# ---------------- Fetchers ----------------

def fetch_boards(uid,user_id):

    res=api_get(uid,"/boards",{
        "owner":user_id,
        "page_size":50
    })

    if res:
        return res.get("items",[])

    return []

def fetch_pins(uid,user_id):

    res=api_get(uid,"/pins",{
        "owner":user_id,
        "page_size":50
    })

    if res:
        return res.get("items",[])

    return []

# ---------------- Main Sync ----------------

def sync_pinterest(uid):

    # Get user account first
    user=fetch_user(uid)

    if not user:
        return {
            "status":"error",
            "message":"Unable to fetch user profile"
        }


    user_id=user.get("id")

    boards=fetch_boards(uid,user_id)

    if boards:
        insert_boards(uid,boards)


    pins=fetch_pins(uid,user_id)

    if pins:
        insert_pins(uid,pins)


    return {
        "status":"ok",
        "user":user.get("username"),
        "boards":len(boards),
        "pins":len(pins),
        "authorized":True
    }
