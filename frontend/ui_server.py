import sys
import os
import datetime

# Add project root to PYTHONPATH
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
import requests
import sqlite3
import json
from functools import wraps
from werkzeug.middleware.proxy_fix import ProxyFix
from urllib.parse import urlsplit, urlencode

from flask import (
    Flask,
    render_template as flask_render_template,
    redirect,
    jsonify,
    request,
    Response,
)

# ================= CORE SETUP =================

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static"
)
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_SAMESITE="None",
    SESSION_COOKIE_PATH="/",
)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", "identity.db")
SESSION_COOKIE_NAME = "segmento_session"
IMAGE_BASE_URL = os.getenv(
    "IMAGE_BASE_URL",
    "https://res.cloudinary.com/dqxzfuory/image/upload"
)

_BACKEND_APP = None


def _get_backend_app():
    global _BACKEND_APP
    if _BACKEND_APP is None:
        from backend.api_server import app as backend_app
        _BACKEND_APP = backend_app
    return _BACKEND_APP


class _BackendLocalResponse:
    def __init__(self, flask_response):
        self.status_code = flask_response.status_code
        self.headers = flask_response.headers
        self.content = flask_response.get_data()
        self.text = flask_response.get_data(as_text=True)
        self.raw = type("RawResponse", (), {"headers": flask_response.headers})()

    def json(self):
        if not self.text:
            return {}
        return json.loads(self.text)


class _SafeBackendResponse:
    def __init__(self, response):
        self._response = response

    def __getattr__(self, name):
        return getattr(self._response, name)

    def json(self):
        try:
            return self._response.json()
        except Exception:
            return {"status": "error", "message": (getattr(self._response, "text", "") or "Invalid backend response")}


class _BackendAwareRequests:
    def __init__(self, real_requests):
        self._real = real_requests

    def _extract_internal_path(self, url):
        if not isinstance(url, str):
            return None

        if url.startswith("/_backend"):
            return url[len("/_backend"):] or "/"

        parsed = urlsplit(url)
        if not parsed.path:
            return None

        is_localhost = parsed.hostname in {"localhost", "127.0.0.1"}
        is_backend_path = parsed.path.startswith("/_backend")

        if not (is_localhost or is_backend_path):
            return None

        path = parsed.path
        if path.startswith("/_backend"):
            path = path[len("/_backend"):] or "/"

        if parsed.query:
            path = f"{path}?{parsed.query}"
        return path

    def request(self, method, url, **kwargs):
        internal_path = self._extract_internal_path(url)
        if internal_path is None:
            if method.upper() == "GET" and "timeout" not in kwargs:
                kwargs["timeout"] = 5
            return self._real.request(method, url, **kwargs)

        backend_app = _get_backend_app()
        headers = dict(kwargs.pop("headers", {}) or {})
        cookies = kwargs.pop("cookies", None)
        params = kwargs.pop("params", None)
        allow_redirects = kwargs.pop("allow_redirects", True)
        kwargs.pop("timeout", None)

        if cookies and "Cookie" not in headers:
            headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in dict(cookies).items())

        if params:
            query = urlencode(params, doseq=True)
            internal_path = f"{internal_path}&{query}" if "?" in internal_path else f"{internal_path}?{query}"

        with backend_app.test_client(use_cookies=False) as client:
            response = client.open(
                path=internal_path,
                method=method,
                headers=headers,
                data=kwargs.pop("data", None),
                json=kwargs.pop("json", None),
                follow_redirects=allow_redirects,
            )

        return _BackendLocalResponse(response)

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)

    def put(self, url, **kwargs):
        return self.request("PUT", url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("DELETE", url, **kwargs)


requests = _BackendAwareRequests(requests)

def copy_auth_cookies(source_response, target_response):
    raw_headers = getattr(getattr(source_response, "raw", None), "headers", None)
    if raw_headers and hasattr(raw_headers, "getlist"):
        cookies = raw_headers.getlist("Set-Cookie")
    else:
        set_cookie = source_response.headers.get("Set-Cookie")
        cookies = [set_cookie] if set_cookie else []

    for cookie in cookies:
        target_response.headers.add("Set-Cookie", cookie)


def build_proxy_response(source_response):
    response = Response(
        source_response.content,
        status=source_response.status_code,
    )

    excluded_headers = {
        "content-length",
        "transfer-encoding",
        "content-encoding",
        "connection",
        "set-cookie",
    }

    for key, value in source_response.headers.items():
        if key.lower() in excluded_headers:
            continue
        response.headers[key] = value

    copy_auth_cookies(source_response, response)
    return response


def proxy_request(method, path, **kwargs):
    base = request.host_url.rstrip("/")
    raw_response = requests.request(
        method,
        f"{base}/_backend{path}",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")},
        **kwargs
    )
    return _SafeBackendResponse(raw_response)


def render_ui_template(template_name_or_list, **context):
    context.setdefault("IMAGE_BASE_URL", IMAGE_BASE_URL)
    return flask_render_template(template_name_or_list, **context)


def safe_backend_json_response(r, include_status=False):
    try:
        payload = r.json()
    except Exception:
        payload = {"status": "error", "message": (getattr(r, "text", "") or "Invalid backend response")}

    if include_status:
        return jsonify(payload), getattr(r, "status_code", 500)
    return jsonify(payload)


# ================= AUTH UTILITIES =================# ================= AUTH UTILITIES =================

def get_google_status(source):

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/{source}",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        data = r.json()
        return data.get("connected", False)
    except:
        return False
    
def logged_in():
    try:
        if "segmento_session" not in request.cookies:
            return False

        base = request.host_url.rstrip("/")
        r = requests.get(
            f"{base}/_backend/auth/me",
            cookies=request.cookies,
            headers={"Cookie": request.headers.get("Cookie", "")}, timeout=2
        )

        return r.status_code == 200

    except:
        return False
    
def require_login(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not logged_in():
            next_url = request.path
            return redirect(f"/login?next={next_url}&auth_required=1")
        return f(*args, **kwargs)
    return decorated


# ================= PAGE ROUTES =================

@app.route("/")
def home():
    return render_ui_template("index.html")

@app.route("/signup")
def signup_page():
    if logged_in():
        return redirect("/")
    return render_ui_template(
        "signup.html",
        next_url=request.args.get("next", ""),
        auth_required=request.args.get("auth_required", "")
    )

@app.route("/login")
def login_page():
    if logged_in():
        return redirect("/")
    return render_ui_template("login.html", next_url=request.args.get("next", ""), auth_required=request.args.get("auth_required", ""))

@app.route("/auth/login", methods=["POST"])
def ui_login():
    """
    Frontend Proxy for Login.
    Ensures that authentication cookies are set within the frontend context.
    """
    base = request.host_url.rstrip("/")
    
    # Forward the credentials to the backend API
    r = requests.post(
        f"{base}/_backend/auth/login",
        data=request.form,
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")},
        allow_redirects=False
    )
    
    # Handle authentication failure (backend redirecting with error=1)
    location = r.headers.get("Location", "")
    if r.status_code == 302 and "error=1" in location:
        return redirect("/login?error=1")

    if r.status_code in [301, 302, 303, 307, 308]:
        resp = redirect(r.headers.get("Location", "/"))
    else:
        resp = Response(r.content, status=r.status_code)

    raw_headers = getattr(getattr(r, "raw", None), "headers", None)

    if raw_headers and hasattr(raw_headers, "getlist"):
        for cookie in raw_headers.getlist("Set-Cookie"):
            resp.headers.add("Set-Cookie", cookie)
    else:
        if "Set-Cookie" in r.headers:
            resp.headers.add("Set-Cookie", r.headers["Set-Cookie"])

    return resp

@app.route("/oauth/callback")
def unified_oauth_callback_proxy():
    """
    Unified OAuth Callback Proxy.
    Receives callbacks from all OAuth providers and forwards them to the backend server.
    """
    base = request.host_url.rstrip("/")
    params = request.args.to_dict()
    
    # Forward the callback request to the backend unified callback endpoint
    r = requests.get(
        f"{base}/_backend/oauth/callback",
        params=params,
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")},
        allow_redirects=False
    )
    
    # If the backend returned a redirect, forward it to the browser
    if r.status_code in [301, 302, 303, 307, 308]:
        resp = redirect(r.headers.get("Location", "/"))
    else:
        # Otherwise, wrap the result in a Flask response
        from flask import make_response
        resp = make_response(r.text, r.status_code)
        for key, value in r.headers.items():
            # Filter out sensitive or hop-by-hop headers
            if key.lower() not in ["content-length", "transfer-encoding", "content-encoding", "set-cookie", "content-type"]:
                resp.headers[key] = value
        if "Content-Type" in r.headers:
            resp.headers["Content-Type"] = r.headers["Content-Type"]

    # Explicitly copy any Set-Cookie headers from the backend
    copy_auth_cookies(r, resp)

    return resp

@app.context_processor
def inject_global_vars():
    return dict(
        is_logged_in=logged_in(),
        base_url=os.getenv("BASE_URL", request.host_url.rstrip("/")),
        IMAGE_BASE_URL=IMAGE_BASE_URL
    )


@app.route("/logout")
def ui_logout():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/auth/logout",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    resp = redirect("/")

    resp.delete_cookie(
        SESSION_COOKIE_NAME,
        path="/",
        secure=True,
        samesite="None",
    )

    return resp

@app.route("/usage")
def usage_page():
    return render_ui_template("usage.html")

@app.route("/account")
@require_login
def account_page():
    return render_ui_template("account.html")

@app.route("/tracking")
def tracking():
    return render_ui_template("tracking.html")


@app.route("/connectors")
def connectors():
    return render_ui_template("connectors.html")

# ================= PROXY UTILITIES =================

def proxy_get(path, **kwargs):
    return proxy_request("GET", path, **kwargs)


def proxy_post(path, **kwargs):
    return proxy_request("POST", path, **kwargs)


def proxy_put(path, **kwargs):
    return proxy_request("PUT", path, **kwargs)


def proxy_delete(path, **kwargs):
    return proxy_request("DELETE", path, **kwargs)


def proxy_get_response(path, **kwargs):
    return build_proxy_response(proxy_get(path, **kwargs))


def proxy_post_response(path, **kwargs):
    return build_proxy_response(proxy_post(path, **kwargs))


def proxy_put_response(path, **kwargs):
    return build_proxy_response(proxy_put(path, **kwargs))


def proxy_delete_response(path, **kwargs):
    return build_proxy_response(proxy_delete(path, **kwargs))


def connector_sync(source):
    return proxy_get(f"/connectors/{source}/sync")


def connector_status(source):
    return proxy_get(f"/api/status/{source}")


def connector_disconnect(source):
    return proxy_get(f"/connectors/{source}/disconnect")


def connector_job_get(source):
    return proxy_get(f"/connectors/{source}/job/get")


def connector_job_save(source):
    return proxy_post(f"/connectors/{source}/job/save", json=request.get_json())

@app.route("/connectors/<source>/recover", methods=["POST"])
@require_login
def ui_recover_connector_data(source):
    r = proxy_post(f"/connectors/{source}/recover", json=request.get_json(silent=True) or {})
    return safe_backend_json_response(r, include_status=True)

# ================= CONNECTOR ROUTES =================
# ================= SOCIAL INSIDER ========================

@app.route("/connectors/socialinsider")
@require_login
def socialinsider_page():
    return render_ui_template("connectors/socialinsider.html")


@app.route("/connectors/socialinsider/connect")
@require_login
def socialinsider_connect():
    r = proxy_get("/connectors/socialinsider/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/socialinsider/sync")
@require_login
def socialinsider_sync():
    r = connector_sync("socialinsider")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/socialinsider/status")
@require_login
def socialinsider_status_proxy():
    r = connector_status("socialinsider")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/socialinsider/job/get")
@require_login
def socialinsider_job_get_proxy():
    r = connector_job_get("socialinsider")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/socialinsider/job/save", methods=["POST"])
@require_login
def socialinsider_job_save_proxy():
    r = connector_job_save("socialinsider")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/socialinsider/save_app", methods=["POST"])
@require_login
def socialinsider_save_app_proxy():
    r = proxy_post("/connectors/socialinsider/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/socialinsider/disconnect")
@require_login
def socialinsider_disconnect():
    r = connector_disconnect("socialinsider")
    return safe_backend_json_response(r, include_status=True)

# ================= STATUS APIs =================

@app.route("/api/status/<source>")
def generic_google_status(source):

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/{source}",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "error": r.text,
            "status_code": r.status_code
        }), r.status_code

@app.route("/connectors/<source>/job/save", methods=["POST"])
def ui_save_job(source):

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/google/job/save/{source}",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/<source>/job/get")
def ui_get_job(source):

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/job/get/{source}",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/<source>/disconnect")
def ui_disconnect(source):

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/{source}/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# ================= GITHUB ========================

@app.route("/connectors/github")
@require_login
def github_page():
    return render_ui_template("connectors/github.html")


@app.route("/connectors/github/connect")
def github_connect():
    return redirect("/_backend/github/connect")

@app.route("/connectors/github/sync")
def github_sync():
    return jsonify(connector_sync("github").json())


# ================= DASHBOARD ROUTES =================
@app.route("/dashboard/github")
def github_dashboard():
    return render_ui_template("dashboards/github.html")

# ================= DATA APIs =================

@app.route("/api/github/data/<table>")
def github_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "repos":
        cur.execute("SELECT * FROM github_repos")

    elif table == "commits":
        cur.execute("SELECT * FROM github_commits")

    elif table == "issues":
        cur.execute("SELECT * FROM github_issues")

    else:
        return jsonify([])

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/api/status/github")
def github_status_proxy():
    return jsonify(connector_status("github").json())

@app.route("/connectors/github/job/get")
def github_job_get_proxy():
    return jsonify(connector_job_get("github").json())

@app.route("/connectors/github/job/save", methods=["POST"])
def github_job_save_proxy():
    return jsonify(connector_job_save("github").json())

@app.route("/connectors/github/save_app", methods=["POST"])
def github_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/github/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

# ================= INSTAGRAM ========================

@app.route("/connectors/instagram")
@require_login
def instagram_page():
    return render_ui_template("connectors/instagram.html")

@app.route("/connectors/instagram/connect")
def instagram_connect():
    return redirect("/_backend/instagram/connect")

@app.route("/connectors/instagram/sync")
def instagram_sync():
    return jsonify(connector_sync("instagram").json())

@app.route("/api/status/instagram")
def instagram_status_proxy():
    return jsonify(connector_status("instagram").json())

@app.route("/connectors/instagram/job/get")
def instagram_job_get_proxy():
    return jsonify(connector_job_get("instagram").json())

@app.route("/connectors/instagram/job/save", methods=["POST"])
def instagram_job_save_proxy():
    return jsonify(connector_job_save("instagram").json())

@app.route("/connectors/instagram/save_app", methods=["POST"])
def instagram_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/instagram/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/instagram/disconnect")
def instagram_disconnect():
    connector_disconnect("instagram")
    return redirect("/connectors/instagram")

# ================= TIKTOK ========================

@app.route("/connectors/tiktok")
@require_login
def tiktok_page():
    return render_ui_template("connectors/tiktok.html")

@app.route("/connectors/tiktok/connect")
def tiktok_connect():
    return redirect("/_backend/connectors/tiktok/connect")

@app.route("/connectors/tiktok/sync")
def tiktok_sync():
    return jsonify(connector_sync("tiktok").json())

@app.route("/api/status/tiktok")
def tiktok_status_proxy():
    return jsonify(connector_status("tiktok").json())

@app.route("/connectors/tiktok/job/get")
def tiktok_job_get_proxy():
    return jsonify(connector_job_get("tiktok").json())

@app.route("/connectors/tiktok/job/save", methods=["POST"])
def tiktok_job_save_proxy():
    return jsonify(connector_job_save("tiktok").json())

@app.route("/connectors/tiktok/save_app", methods=["POST"])
def tiktok_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/tiktok/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/tiktok/disconnect")
def tiktok_disconnect():
    connector_disconnect("tiktok")
    return redirect("/connectors/tiktok")

# ================= TABOOLA ========================

@app.route("/connectors/taboola")
@require_login
def taboola_page():
    return render_ui_template("connectors/taboola.html")

@app.route("/connectors/taboola/connect")
def taboola_connect():
    r = proxy_get("/connectors/taboola/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/taboola/sync")
def taboola_sync():
    return jsonify(connector_sync("taboola").json())

@app.route("/api/status/taboola")
def taboola_status_proxy():
    return jsonify(connector_status("taboola").json())

@app.route("/connectors/taboola/job/get")
def taboola_job_get_proxy():
    return jsonify(connector_job_get("taboola").json())

@app.route("/connectors/taboola/job/save", methods=["POST"])
def taboola_job_save_proxy():
    return jsonify(connector_job_save("taboola").json())

@app.route("/connectors/taboola/save_app", methods=["POST"])
def taboola_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/taboola/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/taboola/disconnect")
def taboola_disconnect():
    connector_disconnect("taboola")
    return redirect("/connectors/taboola")

# ================= OUTBRAIN ========================

@app.route("/connectors/outbrain")
@require_login
def outbrain_page():
    return render_ui_template("connectors/outbrain.html")

@app.route("/connectors/outbrain/connect")
def outbrain_connect():
    r = proxy_get("/connectors/outbrain/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/outbrain/sync")
def outbrain_sync():
    return jsonify(connector_sync("outbrain").json())

@app.route("/api/status/outbrain")
def outbrain_status_proxy():
    return jsonify(connector_status("outbrain").json())

@app.route("/connectors/outbrain/job/get")
def outbrain_job_get_proxy():
    return jsonify(connector_job_get("outbrain").json())

@app.route("/connectors/outbrain/job/save", methods=["POST"])
def outbrain_job_save_proxy():
    return jsonify(connector_job_save("outbrain").json())

@app.route("/connectors/outbrain/save_app", methods=["POST"])
def outbrain_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/outbrain/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/outbrain/disconnect")
def outbrain_disconnect():
    connector_disconnect("outbrain")
    return redirect("/connectors/outbrain")

# ================= SIMILARWEB ========================

@app.route("/connectors/similarweb")
@require_login
def similarweb_page():
    return render_ui_template("connectors/similarweb.html")

@app.route("/connectors/similarweb/connect")
def similarweb_connect():
    r = proxy_get("/connectors/similarweb/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/similarweb/sync")
def similarweb_sync():
    return jsonify(connector_sync("similarweb").json())

@app.route("/api/status/similarweb")
def similarweb_status_proxy():
    return jsonify(connector_status("similarweb").json())

@app.route("/connectors/similarweb/job/get")
def similarweb_job_get_proxy():
    return jsonify(connector_job_get("similarweb").json())

@app.route("/connectors/similarweb/job/save", methods=["POST"])
def similarweb_job_save_proxy():
    return jsonify(connector_job_save("similarweb").json())

@app.route("/connectors/similarweb/save_app", methods=["POST"])
def similarweb_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/similarweb/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/similarweb/disconnect")
def similarweb_disconnect():
    connector_disconnect("similarweb")
    return redirect("/connectors/similarweb")

# ================= X ========================

@app.route("/connectors/x")
@require_login
def x_page():
    return render_ui_template("connectors/x.html")

@app.route("/connectors/x/connect")
def x_connect():
    return redirect("/_backend/connectors/x/connect")

@app.route("/connectors/x/sync")
def x_sync():
    return jsonify(connector_sync("x").json())

@app.route("/api/status/x")
def x_status_proxy():
    return jsonify(connector_status("x").json())

@app.route("/connectors/x/job/get")
def x_job_get_proxy():
    return jsonify(connector_job_get("x").json())

@app.route("/connectors/x/job/save", methods=["POST"])
def x_job_save_proxy():
    return jsonify(connector_job_save("x").json())

@app.route("/connectors/x/save_app", methods=["POST"])
def x_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/x/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/x/disconnect")
def x_disconnect():
    connector_disconnect("x")
    return redirect("/connectors/x")

# ================= LINKEDIN ========================

@app.route("/connectors/linkedin")
@require_login
def linkedin_page():
    return render_ui_template("connectors/linkedin.html")

@app.route("/connectors/linkedin/connect")
def linkedin_connect():
    return redirect("/_backend/connectors/linkedin/connect")

@app.route("/connectors/linkedin/sync")
def linkedin_sync():
    return jsonify(connector_sync("linkedin").json())

@app.route("/api/status/linkedin")
def linkedin_status_proxy():
    return jsonify(connector_status("linkedin").json())

@app.route("/connectors/linkedin/job/get")
def linkedin_job_get_proxy():
    return jsonify(connector_job_get("linkedin").json())

@app.route("/connectors/linkedin/job/save", methods=["POST"])
def linkedin_job_save_proxy():
    return jsonify(connector_job_save("linkedin").json())

@app.route("/connectors/linkedin/save_app", methods=["POST"])
def linkedin_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/linkedin/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/linkedin/disconnect")
def linkedin_disconnect():
    connector_disconnect("linkedin")
    return redirect("/connectors/linkedin")

# ================= SLACK ========================

@app.route("/connectors/slack")
@require_login
def slack_page():
    return render_ui_template("connectors/slack.html")

@app.route("/connectors/slack/connect")
def slack_connect():
    r = proxy_get("/connectors/slack/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/slack/sync")
def slack_sync():
    return jsonify(connector_sync("slack").json())

@app.route("/api/status/slack")
def slack_status_proxy():
    return jsonify(connector_status("slack").json())

@app.route("/connectors/slack/job/get")
def slack_job_get_proxy():

    r = connector_job_get("slack")

    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({
            "error": "invalid_response",
            "status_code": r.status_code,
            "body": r.text[:300]
        }), r.status_code
        
@app.route("/connectors/slack/job/save", methods=["POST"])
def slack_job_save_proxy():
    return jsonify(connector_job_save("slack").json())

@app.route("/connectors/slack/save_app", methods=["POST"])
def slack_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/slack/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/slack/disconnect")
def slack_disconnect():
    connector_disconnect("slack")
    return redirect("/connectors/slack")

# ================= WHATSAPP ========================

@app.route("/connectors/whatsapp")
@require_login
def whatsapp_page():
    return render_ui_template("connectors/whatsapp.html")

@app.route("/connectors/whatsapp/connect")
def whatsapp_connect():
    return jsonify({"status": "manual_credentials"})

@app.route("/connectors/whatsapp/disconnect")
def whatsapp_disconnect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/whatsapp/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/connectors/whatsapp/save_app", methods=["POST"])
def whatsapp_save_config_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/whatsapp/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/whatsapp/sync")
def whatsapp_sync():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/whatsapp/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

# ================= REDDIT ========================

@app.route("/connectors/reddit")
@require_login
def reddit_page():
    return render_ui_template("connectors/reddit.html")

@app.route("/connectors/reddit/connect")
def reddit_connect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/reddit/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/reddit/disconnect")
def reddit_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/reddit/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/reddit/save_config", methods=["POST"])
def reddit_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/reddit/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/reddit/sync")
def reddit_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/reddit/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# ---------- Reddit Dashboard ----------

@app.route("/dashboard/reddit")
def reddit_dashboard():
    return render_ui_template("dashboards/reddit.html")

@app.route("/api/status/reddit")
def reddit_status():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/reddit",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/reddit/job/get")
def reddit_job_get():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/reddit/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/reddit/job/save", methods=["POST"])
def reddit_job_save():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/reddit/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r)

# ---------- Reddit Data API ----------

@app.route("/api/reddit/data/<table>")
def reddit_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "posts":
        cur.execute("SELECT * FROM reddit_posts")

    elif table == "messages":
        cur.execute("SELECT * FROM reddit_messages")

    elif table == "profile":
        cur.execute("SELECT * FROM reddit_profiles")

    else:
        return jsonify([])

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= MEDIUM =================

@app.route("/connectors/medium")
@require_login
def medium_page():
    return render_ui_template("connectors/medium.html")

@app.route("/connectors/medium/connect")
def medium_connect_proxy():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/medium/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/medium")

@app.route("/connectors/medium/save_config", methods=["POST"])
def medium_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/medium/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "error": "identity_server_error",
            "raw": r.text
        }), r.status_code

@app.route("/connectors/medium/sync")
def medium_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/medium/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/medium")
def medium_dashboard():
    return render_ui_template("dashboards/medium.html")



@app.route("/api/status/medium")
def medium_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/medium",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/api/medium/data/posts")
def medium_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM medium_posts ORDER BY published DESC")

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GITLAB =================

@app.route("/connectors/gitlab")
@require_login
def gitlab_page():
    return render_ui_template("connectors/gitlab.html")

@app.route("/connectors/gitlab/save_app", methods=["POST"])
def gitlab_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/gitlab/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/gitlab")
def gitlab_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/gitlab",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/gitlab/job/get")
def gitlab_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/gitlab/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/gitlab/job/save", methods=["POST"])
def gitlab_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/gitlab/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/gitlab/connect")
def gitlab_connect():
    return redirect("/_backend/gitlab/connect")


@app.route("/connectors/gitlab/sync")
def gitlab_sync():

    try:
        base = request.host_url.rstrip("/")
        r = requests.get(
            f"{base}/_backend/connectors/gitlab/sync",
            cookies=request.cookies,
            headers={"Cookie": request.headers.get("Cookie", "")}, timeout=300
        )

        return safe_backend_json_response(r, include_status=True)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/dashboard/gitlab")
def gitlab_dashboard():
    return render_ui_template("dashboards/gitlab.html")

@app.route("/api/gitlab/<table>")
def gitlab_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    if table=="projects":
        cur.execute("SELECT * FROM gitlab_projects")

    elif table=="commits":
        cur.execute("SELECT * FROM gitlab_commits")

    elif table=="issues":
        cur.execute("SELECT * FROM gitlab_issues")

    elif table=="mrs":
        cur.execute("SELECT * FROM gitlab_merge_requests")

    else:
        return jsonify([])


    rows=cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= DEVTO =================

@app.route("/connectors/devto")
@require_login
def devto_page():
    return render_ui_template("connectors/devto.html")

@app.route("/api/status/devto")
def devto_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/devto",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/devto/connect")
def devto_connect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/devto/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/devto")


@app.route("/connectors/devto/disconnect")
def devto_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/devto/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/devto/sync")
def devto_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/devto/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/dashboard/devto")
def devto_dashboard():
    return render_ui_template("dashboards/devto.html")

@app.route("/connectors/devto/job/get")
def devto_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/devto/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/devto/job/save", methods=["POST"])
def devto_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/devto/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r)

# ================= STACKOVERFLOW =================

@app.route("/connectors/stackoverflow")
@require_login
def stackoverflow_page():
    return render_ui_template("connectors/stackoverflow.html")

@app.route("/connectors/stackoverflow/save_config", methods=["POST"])
def stackoverflow_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/stackoverflow/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

# CONNECT
@app.route("/connectors/stackoverflow/connect")
def stackoverflow_connect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/stackoverflow/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/stackoverflow")


# DISCONNECT
@app.route("/connectors/stackoverflow/disconnect")
def stackoverflow_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/stackoverflow/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


# MANUAL SYNC
@app.route("/connectors/stackoverflow/sync")
def stackoverflow_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/stackoverflow/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/dashboard/stackoverflow")
def stackoverflow_dashboard():
    return render_ui_template("dashboards/stackoverflow.html")


# ---------- STATUS ----------

@app.route("/api/status/stackoverflow")
def stackoverflow_status():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/stackoverflow",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


# ---------- DATA APIs ----------

@app.route("/api/stackoverflow/data/questions")
def stack_questions():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM stack_questions
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/stackoverflow/data/answers")
def stack_answers():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM stack_answers
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/stackoverflow/data/users")
def stack_users():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM stack_users
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= HACKERNEWS =================

@app.route("/connectors/hackernews")
@require_login
def hackernews_page():
    return render_ui_template("connectors/hackernews.html")


@app.route("/connectors/hackernews/connect")
def hackernews_connect():
    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/hackernews/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return redirect("/connectors/hackernews")

@app.route("/connectors/hackernews/sync")
def hackernews_sync():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/hackernews/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/dashboard/hackernews")
def hackernews_dashboard():
    return render_ui_template("dashboards/hackernews.html")

# ---------- STATUS ----------

@app.route("/api/status/hackernews")
def hackernews_status():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM hackernews_stories")

    count = cur.fetchone()[0]

    conn.close()

    return jsonify({"connected": count > 0})


# ---------- DATA API ----------

@app.route("/api/hackernews/data/stories")
def hackernews_stories():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM hackernews_stories
        ORDER BY time DESC
    """)

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= NVD =================

@app.route("/connectors/nvd")
@require_login
def nvd_page():
    return render_ui_template("connectors/nvd.html")

@app.route("/connectors/nvd/save_config", methods=["POST"])
def nvd_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/nvd/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "error": "identity_server returned non-json",
            "status": r.status_code,
            "body": r.text
        }), r.status_code

# CONNECT = FIRST SYNC
@app.route("/connectors/nvd/connect")
def nvd_connect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/nvd/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/nvd")

# MANUAL SYNC
@app.route("/connectors/nvd/sync")
def nvd_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/nvd/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/nvd")
def nvd_dashboard():
    return render_ui_template("dashboards/nvd.html")


# ---------- STATUS ----------

@app.route("/api/status/nvd")
def nvd_status():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/nvd",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# ---------- DATA API ----------

@app.route("/api/nvd/data/cves")
def nvd_cves():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM nvd_cves
        ORDER BY published DESC
    """)

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= DISCORD =================

@app.route("/connectors/discord")
@require_login
def discord_page():
    return render_ui_template("connectors/discord.html")

@app.route("/connectors/discord/save_config", methods=["POST"])
def discord_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/discord/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/discord/connect")
def discord_connect_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/discord/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/discord")

@app.route("/connectors/discord/disconnect")
def discord_disconnect_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/discord/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/discord/sync")
def discord_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/discord/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/api/status/discord")
def discord_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/discord",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# ================= TELEGRAM =================

@app.route("/connectors/telegram")
@require_login
def telegram_page():
    return render_ui_template("connectors/telegram.html")

@app.route("/connectors/telegram/connect")
def telegram_connect_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/telegram/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    if r.status_code != 200:
        return r.text, 400

    return redirect("/connectors/telegram")

@app.route("/connectors/telegram/disconnect")
def telegram_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/telegram/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/telegram/sync")
def telegram_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/telegram/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return safe_backend_json_response(r)
    except:
        return jsonify({"error": "sync failed"}), 500

@app.route("/dashboard/telegram")
def telegram_dashboard():
    return render_ui_template("dashboards/telegram.html")


# -------- STATUS --------

@app.route("/api/status/telegram")
def telegram_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/telegram",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- DATA APIs --------

@app.route("/api/telegram/channels")
def telegram_channels():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM telegram_channels")

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/telegram/messages/<cid>")
def telegram_messages(cid):

    # Trigger sync before fetch
    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/telegram/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM telegram_messages
        WHERE channel_id=?
        ORDER BY date DESC
        LIMIT 200
    """, (cid,))

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/telegram/save_config", methods=["POST"])
def telegram_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/telegram/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

# ================= TUMBLR =================

@app.route("/connectors/tumblr")
@require_login
def tumblr_page():
    return render_ui_template("connectors/tumblr.html")

@app.route("/connectors/tumblr/connect")
def tumblr_connect_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/tumblr/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/tumblr")

@app.route("/connectors/tumblr/save_config", methods=["POST"])
def tumblr_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/tumblr/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "error": "identity_server error",
            "raw": r.text
        }), r.status_code

@app.route("/connectors/tumblr/sync")
def tumblr_sync_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/tumblr/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/tumblr")
def tumblr_dashboard():
    return render_ui_template("dashboards/tumblr.html")

@app.route("/connectors/tumblr/disconnect")
def tumblr_disconnect_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/tumblr/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- STATUS --------

@app.route("/api/status/tumblr")
def tumblr_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/tumblr",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- DATA APIs --------

@app.route("/api/tumblr/blogs")
def tumblr_blogs():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM tumblr_blogs")

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/tumblr/posts/<blog>")
def tumblr_posts(blog):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM tumblr_posts
        WHERE blog_name=?
        ORDER BY timestamp DESC
        LIMIT 200
    """, (blog,))

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= MASTODON =================
@app.route("/connectors/mastodon")
@require_login
def mastodon_page():
    return render_ui_template("connectors/mastodon.html")

@app.route("/connectors/mastodon/connect")
def mastodon_connect_proxy():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/mastodon/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/mastodon")

@app.route("/connectors/mastodon/disconnect")
def mastodon_disconnect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/mastodon/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/connectors/mastodon/sync")
def mastodon_sync():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/mastodon/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/dashboard/mastodon")
def mastodon_dashboard():
    return render_ui_template("dashboards/mastodon.html")


# -------- STATUS --------

@app.route("/api/status/mastodon")
def mastodon_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/mastodon",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "error": "identity_server failure",
            "raw": r.text
        }), r.status_code

# -------- DATA --------

@app.route("/api/mastodon/statuses")
def mastodon_statuses():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM mastodon_statuses
    ORDER BY fetched_at DESC
    LIMIT 500
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/mastodon/tags")
def mastodon_tags():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM mastodon_tags
    ORDER BY fetched_at DESC
    LIMIT 200
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

@app.route("/connectors/mastodon/save_config",methods=["POST"])
def mastodon_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/mastodon/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r),r.status_code

# ================= LEMMY =================

@app.route("/connectors/lemmy")
@require_login
def lemmy_page():
    return render_ui_template("connectors/lemmy.html")

@app.route("/connectors/lemmy/connect")
def lemmy_connect_proxy():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/lemmy/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/lemmy")

@app.route("/connectors/lemmy/sync")
def lemmy_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/lemmy/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/lemmy")
def lemmy_dashboard():
    return render_ui_template("dashboards/lemmy.html")


# -------- STATUS --------

@app.route("/api/status/lemmy")
def lemmy_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/lemmy",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return safe_backend_json_response(r)
    except:
        return jsonify({
            "connected": False,
            "has_credentials": False
        }), 500
    
# -------- DATA --------

@app.route("/api/lemmy/posts")
def lemmy_posts():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM lemmy_posts
    ORDER BY fetched_at DESC
    LIMIT 500
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/lemmy/communities")
def lemmy_communities():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM lemmy_communities
    ORDER BY fetched_at DESC
    LIMIT 300
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/lemmy/users")
def lemmy_users():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM lemmy_users
    ORDER BY fetched_at DESC
    LIMIT 300
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

@app.route("/connectors/lemmy/save_config",methods=["POST"])
def lemmy_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/lemmy/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r),r.status_code

# ================= PINTEREST =================

@app.route("/connectors/pinterest")
@require_login
def pinterest_page():
    return render_ui_template("connectors/pinterest.html")


@app.route("/connectors/pinterest/connect")
def pinterest_connect():
    return redirect("/_backend/connectors/pinterest/connect")

@app.route("/connectors/pinterest/sync")
def pinterest_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/pinterest/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/pinterest/disconnect")
def pinterest_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/pinterest/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/pinterest")
def pinterest_dashboard():
    return render_ui_template("dashboards/pinterest.html")


# -------- STATUS --------

@app.route("/api/status/pinterest")
def pinterest_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/pinterest",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- DATA --------

@app.route("/api/pinterest/boards")
def pinterest_boards():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM pinterest_boards
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/pinterest/pins")
def pinterest_pins():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM pinterest_pins
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

@app.route("/connectors/pinterest/save_app", methods=["POST"])
def pinterest_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/pinterest/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)


@app.route("/pinterest/callback")
def pinterest_callback_proxy():
    """
    Pinterest OAuth redirects here. Must match the redirect_uri registered
    in the Pinterest Developer app (e.g. http://localhost:5000/pinterest/callback).
    Proxies to api_server carrying the session cookie so get_uid() resolves,
    then redirects the browser back to the connector page.
    """
    code  = request.args.get("code", "")
    state = request.args.get("state", "")

    if not code:
        return "Authorization failed: no code returned from Pinterest.", 400

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/pinterest/callback?code={code}&state={state}",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    print(f"[PINTEREST CALLBACK PROXY] api_server responded {r.status_code}", flush=True)

    if r.status_code not in (200, 302):
        return f"OAuth error ({r.status_code}): {r.text}", 400

    return redirect("/connectors/pinterest")

# ================= TWITCH =================

@app.route("/connectors/twitch")
@require_login
def twitch_page():
    return render_ui_template("connectors/twitch.html")

@app.route("/connectors/twitch/connect")
def twitch_connect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/twitch/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    # CRITICAL
    return redirect("/connectors/twitch")

@app.route("/connectors/twitch/disconnect")
def twitch_disconnect():
    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/twitch/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return redirect("/connectors/twitch")

@app.route("/connectors/twitch/sync")
def twitch_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/twitch/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/api/status/twitch")
def twitch_status():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/twitch",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/twitch/save_config",methods=["POST"])
def twitch_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/twitch/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r),r.status_code

# ================= PEERTUBE =================

@app.route("/connectors/peertube")
@require_login
def peertube_page():
    return render_ui_template("connectors/peertube.html")

@app.route("/connectors/peertube/connect")
def peertube_connect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/peertube/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/peertube")

@app.route("/connectors/peertube/disconnect")
def peertube_disconnect_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/peertube/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/peertube/sync")
def peertube_sync_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/peertube/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r, include_status=True)


@app.route("/dashboard/peertube")
def peertube_dashboard():
    return render_ui_template("dashboards/peertube.html")


# -------- STATUS --------

@app.route("/api/status/peertube")
def peertube_status():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/peertube",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- DATA --------

@app.route("/api/peertube/videos")
def peertube_videos():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM peertube_videos
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/peertube/channels")
def peertube_channels():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM peertube_channels
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

@app.route("/connectors/peertube/save_config",methods=["POST"])
def peertube_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/peertube/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r),r.status_code

# ================= OPENSTREETMAP =================

@app.route("/connectors/openstreetmap")
@require_login
def osm_page():
    return render_ui_template("connectors/openstreetmap.html")

@app.route("/connectors/openstreetmap/connect")
def ui_osm_connect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/openstreetmap/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/openstreetmap")

@app.route("/connectors/openstreetmap/disconnect")
def ui_osm_disconnect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/openstreetmap/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/openstreetmap")

@app.route("/connectors/openstreetmap/sync")
def ui_osm_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/openstreetmap/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/openstreetmap")
def osm_dashboard():
    return render_ui_template("dashboards/openstreetmap.html")


# -------- STATUS --------

@app.route("/api/status/openstreetmap")
def osm_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/openstreetmap",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- DATA --------

@app.route("/api/osm/changesets")
def osm_changesets():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM osm_changesets
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)


@app.route("/api/osm/notes")
def osm_notes():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
    SELECT * FROM osm_notes
    ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]

    con.close()

    return jsonify(rows)

# ================= WIKIPEDIA =================

@app.route("/connectors/wikipedia")
@require_login
def wikipedia_page():
    return render_ui_template("connectors/wikipedia.html")

# -------- CONNECT --------

@app.route("/connectors/wikipedia/connect")
def ui_wikipedia_connect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/wikipedia/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/wikipedia")

# -------- DISCONNECT --------

@app.route("/connectors/wikipedia/disconnect")
def ui_wikipedia_disconnect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/wikipedia/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/wikipedia")

# -------- SYNC --------

@app.route("/connectors/wikipedia/sync")
def ui_wikipedia_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/wikipedia/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- STATUS (Unified Pattern) --------

@app.route("/api/status/wikipedia")
def wikipedia_status_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/wikipedia",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- DASHBOARD --------

@app.route("/dashboard/wikipedia")
def wikipedia_dashboard():
    return render_ui_template("dashboards/wikipedia.html")


# -------- DATA --------

@app.route("/api/wiki/recent")
def wiki_recent():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM wikipedia_recent_changes
        ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    return jsonify(rows)


@app.route("/api/wiki/newpages")
def wiki_new():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM wikipedia_new_pages
        ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    return jsonify(rows)


@app.route("/api/wiki/viewed")
def wiki_viewed():

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT *
        FROM wikipedia_most_viewed
        ORDER BY fetched_at DESC
    """)

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    return jsonify(rows)

# ================= PRODUCTHUNT =================

@app.route("/connectors/producthunt")
@require_login
def producthunt_page():
    return render_ui_template("connectors/producthunt.html")


# -------- CONNECT --------

@app.route("/connectors/producthunt/connect")
def ui_producthunt_connect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/producthunt/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/producthunt")

# -------- DISCONNECT --------

@app.route("/connectors/producthunt/disconnect")
def ui_producthunt_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/producthunt/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r, include_status=True)


# -------- SYNC --------

@app.route("/connectors/producthunt/sync")
def ui_producthunt_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/producthunt/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r, include_status=True)


# -------- STATUS (STANDARDIZED) --------

@app.route("/api/status/producthunt")
def ui_producthunt_status():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/producthunt",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# -------- DASHBOARD --------

@app.route("/dashboard/producthunt")
def producthunt_dashboard():
    return render_ui_template("dashboards/producthunt.html")


# -------- DATA APIs --------

@app.route("/api/producthunt/posts")
def ui_producthunt_posts():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/producthunt/data/posts",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/api/producthunt/topics")
def ui_producthunt_topics():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/producthunt/data/topics",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/producthunt/save_config",methods=["POST"])
def ui_producthunt_save():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/producthunt/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r)

# ================= DISCOURSE =================

@app.route("/connectors/discourse")
@require_login
def discourse_page():
    return render_ui_template("connectors/discourse.html")

@app.route("/connectors/discourse/connect")
def ui_discourse_connect():

    base = request.host_url.rstrip("/")
    requests.get(
        f"{base}/_backend/connectors/discourse/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return redirect("/connectors/discourse")

@app.route("/api/status/discourse")
def discourse_status():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/discourse",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/discourse/disconnect")
def ui_discourse_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/discourse/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/discourse/sync")
def ui_discourse_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/discourse/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/dashboard/discourse")
def discourse_dashboard():
    return render_ui_template("dashboards/discourse.html")

@app.route("/api/discourse/topics")
def ui_discourse_topics():

    r = requests.get(
        "http://127.0.0.1:4000/discourse/data/topics",
        headers={
            "Cookie": request.headers.get("Cookie", "")
        }
    )

    try:
        return safe_backend_json_response(r)
    except:
        return jsonify([])


@app.route("/api/discourse/categories")
def ui_discourse_categories():

    r = requests.get(
        "http://127.0.0.1:4000/discourse/data/categories",
        headers={
            "Cookie": request.headers.get("Cookie", "")
        }
    )

    try:
        return safe_backend_json_response(r)
    except:
        return jsonify([])

# ================= GMAIL ========================

@app.route("/connectors/gmail")
@require_login
def gmail_page():
    return render_ui_template("connectors/gmail.html")


# Redirect to Identity Server OAuth
@app.route("/connectors/gmail/connect")
def gmail_connect():
    return redirect("/_backend/google/connect?source=gmail")


# After OAuth redirect comes back here
@app.route("/connectors/gmail/callback")
def gmail_callback():

    code = request.args.get("code")

    if not code:
        return "Authorization failed", 400

    # Forward to identity server
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/callback?code={code}&source=gmail",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    if r.status_code != 200:
        return r.text, 400

    # NO AUTO SYNC HERE

    return redirect("/connectors/gmail")


@app.route("/connectors/gmail/sync")
def gmail_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/gmail",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=120
    )

    try:
        return safe_backend_json_response(r)
    except:
        return jsonify({"status": "error"}), 500


@app.route("/connectors/gmail/disconnect")
def gmail_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/gmail",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/api/status/gmail")
def gmail_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "gmail"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials saved
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Check connection enabled
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/gmail/save_app", methods=["POST"])
def gmail_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/gmail/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/gmail/data/<table>")
def gmail_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "profile":
        cur.execute("SELECT * FROM google_gmail_profile")

    elif table == "labels":
        cur.execute("SELECT * FROM google_gmail_labels")

    elif table == "messages":
        cur.execute("SELECT * FROM google_gmail_messages")

    elif table == "details":
        cur.execute("SELECT * FROM google_gmail_message_details")

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/dashboard/gmail")
def gmail_dashboard():
    return render_ui_template("dashboards/gmail.html")


# ================= GOOGLE DRIVE ========================

@app.route("/connectors/drive")
@require_login
def drive_page():
    return render_ui_template("connectors/drive.html")

@app.route("/connectors/drive/connect")
def drive_connect():
    return redirect("/_backend/google/connect?source=drive")

@app.route("/connectors/drive/sync")
def drive_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/drive",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=120
    )

    # Safe handling
    try:
        return safe_backend_json_response(r)
    except:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text
        }), 500


@app.route("/dashboard/drive")
def drive_dashboard():
    return render_ui_template("dashboards/drive.html")


@app.route("/api/status/drive")
def drive_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "drive"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/drive/save_app", methods=["POST"])
def drive_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/drive/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/drive/disconnect")
def drive_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/drive",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/api/drive/data/files")
def drive_files_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM drive_files
        ORDER BY fetched_at DESC
        LIMIT 500
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/drive/job/get")
def drive_job_get_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/drive/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/drive/job/save", methods=["POST"])
def drive_job_save_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/drive/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

# ================= GOOGLE CALENDAR ========================

@app.route("/connectors/calendar")
@require_login
def calendar_page():
    return render_ui_template("connectors/calendar.html")

@app.route("/connectors/calendar/connect")
def calendar_connect():
    return redirect("/_backend/google/connect?source=calendar")

@app.route("/connectors/calendar/save_app", methods=["POST"])
def calendar_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/calendar/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/calendar/disconnect")
def calendar_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/calendar",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/calendar/sync")
def calendar_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/calendar",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=180
    )

    # Safe JSON handling
    try:
        return safe_backend_json_response(r)
    except Exception as e:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text,
            "exception": str(e)
        }), 500


@app.route("/dashboard/calendar")
def calendar_dashboard():
    return render_ui_template("dashboards/calendar.html")

@app.route("/api/status/calendar")
def calendar_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "calendar"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/calendar/job/get")
def calendar_job_get_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/calendar/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/calendar/job/save", methods=["POST"])
def calendar_job_save_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/calendar/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/calendar/data/<table>")
def calendar_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "colors":
        cur.execute("SELECT * FROM google_calendar_colors")

    elif table == "settings":
        cur.execute("SELECT * FROM google_calendar_settings")

    elif table == "calendars":
        cur.execute("SELECT * FROM google_calendar_list")

    elif table == "events":
        cur.execute("""
            SELECT *
            FROM google_calendar_events
            ORDER BY start DESC
            LIMIT 1000
        """)

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE SHEETS ========================

@app.route("/connectors/sheets")
@require_login
def sheets_page():
    return render_ui_template("connectors/sheets.html")

@app.route("/connectors/sheets/connect")
def sheets_connect():
    return redirect("/_backend/google/connect?source=sheets")

@app.route("/connectors/sheets/save_app", methods=["POST"])
def sheets_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/sheets/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sheets/disconnect")
def sheets_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/sheets",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/sheets/job/get")
def sheets_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/sheets/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/sheets/job/save", methods=["POST"])
def sheets_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/sheets/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sheets/sync")
def sheets_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/sheets",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=120
    )

    # Safe JSON handling
    try:
        return safe_backend_json_response(r)
    except Exception as e:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text,
            "exception": str(e)
        }), 500


@app.route("/dashboard/sheets")
def sheets_dashboard():
    return render_ui_template("dashboards/sheets.html")


@app.route("/api/status/sheets")
def sheets_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "sheets"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/api/sheets/data")
def sheets_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM sheets_data
        ORDER BY fetched_at DESC
        LIMIT 500
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE FORMS ========================

@app.route("/connectors/forms")
@require_login
def forms_page():
    return render_ui_template("connectors/forms.html")

@app.route("/connectors/forms/sync")
def forms_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/forms",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=180
    )

    return safe_backend_json_response(r)

@app.route("/connectors/forms/connect")
def forms_connect():
    return redirect("/_backend/google/connect?source=forms")

@app.route("/connectors/forms/save_app", methods=["POST"])
def forms_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/forms/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/forms/disconnect")
def forms_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/forms",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/forms")
def forms_dashboard():
    return render_ui_template("dashboards/forms.html")

@app.route("/api/status/forms")
def forms_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "forms"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/forms/job/get")
def forms_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/forms/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/forms/job/save", methods=["POST"])
def forms_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/forms/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/api/forms/data/<table>")
def forms_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "forms":

        cur.execute("""
            SELECT *
            FROM google_forms
            ORDER BY fetched_at DESC
        """)

    elif table == "responses":

        cur.execute("""
            SELECT *
            FROM google_form_responses
            ORDER BY fetched_at DESC
            LIMIT 1000
        """)

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


# ================= GOOGLE CONTACTS ========================

@app.route("/connectors/contacts")
@require_login
def contacts_page():
    return render_ui_template("connectors/contacts.html")

@app.route("/connectors/contacts/connect")
def contacts_connect():
    return redirect("/_backend/google/connect?source=contacts")

@app.route("/connectors/contacts/save_app", methods=["POST"])
def contacts_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/contacts/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/contacts/disconnect")
def contacts_disconnect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/contacts",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/connectors/contacts/sync")
def contacts_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/contacts",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=180
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/contacts")
def contacts_dashboard():
    return render_ui_template("dashboards/contacts.html")

@app.route("/api/status/contacts")
def contacts_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "contacts"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/api/contacts/data")
def contacts_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_contacts_persons
        ORDER BY fetched_at DESC
        LIMIT 1000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/contacts/job/get")
def contacts_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/contacts/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/contacts/job/save", methods=["POST"])
def contacts_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/contacts/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

# ================= GOOGLE TASKS ========================

@app.route("/connectors/tasks")
@require_login
def tasks_page():
    return render_ui_template("connectors/tasks.html")

@app.route("/connectors/tasks/connect")
def tasks_connect():
    return redirect("/_backend/google/connect?source=tasks")

@app.route("/connectors/tasks/save_app", methods=["POST"])
def tasks_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/tasks/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/tasks/disconnect")
def tasks_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/tasks",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/tasks/sync")
def tasks_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/tasks",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=180
    )

    try:
        return safe_backend_json_response(r)
    except Exception as e:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text,
            "exception": str(e)
        }), 500


@app.route("/dashboard/tasks")
def tasks_dashboard():
    return render_ui_template("dashboards/tasks.html")


@app.route("/api/status/tasks")
def tasks_status():

    uid = request.cookies.get("uid") or "demo_user"
    source = "tasks"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, source))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source=?
        LIMIT 1
    """, (uid, source))
    row = cur.fetchone()

    conn.close()

    connected = False
    if row and row[0] == 1:
        connected = True

    return jsonify({
        "connected": connected,
        "has_credentials": bool(creds)
    })

@app.route("/connectors/tasks/job/get")
def tasks_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/tasks/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/tasks/job/save", methods=["POST"])
def tasks_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/tasks/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/api/tasks/data/<table>")
def tasks_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if table == "lists":

        cur.execute("""
            SELECT *
            FROM google_tasks_lists
            ORDER BY fetched_at DESC
        """)

    elif table == "items":

        cur.execute("""
            SELECT *
            FROM google_tasks_items
            ORDER BY fetched_at DESC
            LIMIT 1000
        """)

    else:
        return jsonify([])

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE GA4 ========================

@app.route("/connectors/ga4")
@require_login
def ga4_page():
    return render_ui_template("connectors/ga4.html")

@app.route("/connectors/ga4/connect")
def ga4_connect():
    return redirect("/_backend/google/connect?source=ga4")

@app.route("/connectors/ga4/save_app", methods=["POST"])
def ga4_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/ga4/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/ga4/disconnect")
def ga4_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/ga4",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/ga4/job/get")
def ga4_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/ga4/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/ga4/job/save", methods=["POST"])
def ga4_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/ga4/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/ga4/sync")
def ga4_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/ga4",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=180
    )

    try:
        return safe_backend_json_response(r)
    except:
        return jsonify({
            "status": "error",
            "http_code": r.status_code,
            "raw": r.text
        }), 500

@app.route("/dashboard/ga4")
def ga4_dashboard():
    return render_ui_template("dashboards/ga4.html")


@app.route("/api/status/ga4")
def ga4_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='ga4'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Check connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='ga4'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0] == 1),
        "has_credentials": bool(creds)
    })

@app.route("/api/ga4/data/<table>")
def ga4_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    tables = {
        "overview": "ga4_website_overview",
        "devices": "ga4_devices",
        "locations": "ga4_locations",
        "traffic": "ga4_traffic_sources",
        "events": "ga4_events"
    }

    if table not in tables:
        return jsonify([])


    cur.execute(f"""
        SELECT *
        FROM {tables[table]}
        ORDER BY date DESC
        LIMIT 2000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE SEARCH CONSOLE ========================

@app.route("/connectors/search-console")
@require_login
def gsc_page():
    return render_ui_template("connectors/search_console.html")

@app.route("/connectors/search-console/connect")
def search_console_connect():
    return redirect("/_backend/google/connect?source=search-console")

@app.route("/connectors/search-console/save_app", methods=["POST"])
def search_console_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/search-console/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/search-console/disconnect")
def search_console_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/search-console",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/search-console/sync")
def ui_gsc_sync():

    site = request.args.get("site")
    sync_type = request.args.get("sync_type", "incremental")

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/search-console/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/search-console")
def gsc_dashboard():
    return render_ui_template("dashboards/search_console.html")


@app.route("/api/status/search-console")
def search_console_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='search-console'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='search-console'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0] == 1),
        "has_credentials": bool(creds)
    })

@app.route("/api/search-console/data")
def gsc_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_search_console
        ORDER BY fetched_at DESC
        LIMIT 3000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE YOUTUBE ========================

@app.route("/connectors/youtube")
@require_login
def youtube_page():
    return render_ui_template("connectors/youtube.html")

@app.route("/connectors/youtube/connect")
def youtube_connect():
    return redirect("/_backend/google/connect?source=youtube")

@app.route("/connectors/youtube/save_app", methods=["POST"])
def youtube_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/youtube/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/youtube/disconnect")
def youtube_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/youtube",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/api/status/youtube")
def youtube_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Credentials
    cur.execute("""
        SELECT 1
        FROM connector_configs
        WHERE uid=? AND connector='youtube'
        LIMIT 1
    """, (uid,))
    creds = cur.fetchone()

    # Connection
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='youtube'
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0] == 1),
        "has_credentials": bool(creds)
    })

@app.route("/connectors/youtube/job/get")
def youtube_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/youtube/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/youtube/job/save", methods=["POST"])
def youtube_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/youtube/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/youtube/sync")
def ui_youtube_sync():

    sync_type = request.args.get("sync_type", "incremental")

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/youtube/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/youtube")
def youtube_dashboard():
    return render_ui_template("dashboards/youtube.html")

@app.route("/api/youtube/data/<table>")
def youtube_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    if table == "channels":

        cur.execute("""
            SELECT *
            FROM google_youtube_channels
            ORDER BY fetched_at DESC
        """)

    elif table == "videos":

        cur.execute("""
            SELECT *
            FROM google_youtube_videos
            ORDER BY published_at DESC
            LIMIT 2000
        """)

    else:
        return jsonify([])


    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE TRENDS ========================

@app.route("/connectors/trends")
@require_login
def trends_page():
    return render_ui_template("connectors/trends.html")

@app.route("/connectors/trends/disconnect")
def trends_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/trends",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/trends/sync")
def ui_trends_sync():

    keyword = request.args.get("keyword")
    sync_type = request.args.get("sync_type", "daily")

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/trends/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/trends/connect", methods=["POST"])
def ui_trends_connect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/trends/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/dashboard/trends")
def trends_dashboard():
    return render_ui_template("dashboards/trends.html")

@app.route("/api/trends/data/<table>")
def trends_data(table):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()


    if table == "interest":

        cur.execute("""
            SELECT *
            FROM google_trends_interest
            ORDER BY date DESC
            LIMIT 2000
        """)

    elif table == "related":

        cur.execute("""
            SELECT *
            FROM google_trends_related
            ORDER BY fetched_at DESC
            LIMIT 2000
        """)

    else:
        return jsonify([])


    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/trends/job/get")
def trends_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/trends/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/trends/job/save", methods=["POST"])
def trends_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/trends/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r)

# ================= GOOGLE NEWS ========================

@app.route("/connectors/news")
@require_login
def news_page():
    return render_ui_template("connectors/news.html")


@app.route("/connectors/news/connect", methods=["POST"])
def news_connect():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/news/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/news/disconnect", methods=["POST"])
def news_disconnect():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/news/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/news/sync")
def news_sync():

    keyword = request.args.get("keyword")
    sync_type = request.args.get("sync_type", "incremental")

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/news/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/news/job/get")
def news_job_get_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/news/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/news/job/save", methods=["POST"])
def news_job_save_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/news/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r)


@app.route("/api/status/news")
def news_status_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/news",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

# ================= GOOGLE BOOKS ========================

@app.route("/connectors/books")
@require_login
def books_page():
    return render_ui_template("connectors/books.html")


@app.route("/connectors/books/connect")
def ui_books_connect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/books/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/books/disconnect")
def ui_books_disconnect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/books/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/books/sync")
def ui_books_sync():

    query = request.args.get("query")
    sync_type = request.args.get("sync_type", "incremental")

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/books/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


@app.route("/connectors/books/job/get")
def ui_books_job_get():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/books/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/books/job/save", methods=["POST"])
def ui_books_job_save():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/books/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r)


@app.route("/dashboard/books")
def books_dashboard():
    return render_ui_template("dashboards/books.html")


@app.route("/api/status/books")
def books_status():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/books",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/api/books/data")
def books_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_books_volumes
        ORDER BY fetched_at DESC
        LIMIT 2000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE WEBFONTS ========================

@app.route("/connectors/webfonts")
@require_login
def webfonts_page():
    return render_ui_template("connectors/webfonts.html")


@app.route("/connectors/webfonts/connect")
def webfonts_connect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/webfonts/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/webfonts/disconnect")
def webfonts_disconnect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/webfonts/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/webfonts/sync")
def webfonts_sync():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/webfonts/sync",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=180
    )
    return safe_backend_json_response(r)


@app.route("/connectors/webfonts/job/get")
def webfonts_job_get_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/webfonts/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/webfonts/job/save", methods=["POST"])
def webfonts_job_save_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/webfonts/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r)


@app.route("/api/status/webfonts")
def webfonts_status():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/api/status/webfonts",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/api/webfonts/data")
def webfonts_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_webfonts
        ORDER BY fetched_at DESC
        LIMIT 1000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/webfonts/save_config", methods=["POST"])
def webfonts_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/webfonts/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

# ================= GOOGLE PAGESPEED ========================

@app.route("/connectors/pagespeed")
@require_login
def pagespeed_page():
    return render_ui_template("connectors/pagespeed.html")


@app.route("/connectors/pagespeed/sync", methods=["POST"])
def pagespeed_sync():

    data = request.get_json()

    if not data:
        return jsonify({"error": "JSON body required"}), 400

    url = data.get("url")

    if not url:
        return jsonify({"error": "URL required"}), 400

    try:

        base = request.host_url.rstrip("/")
        r = requests.post(
            f"{base}/_backend/google/sync/pagespeed",
            cookies=request.cookies,
            headers={"Cookie": request.headers.get("Cookie", "")}, timeout=600, json=request.get_json(silent=True) or request.json or {}
        )

        return safe_backend_json_response(r)

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/dashboard/pagespeed")
def pagespeed_dashboard():
    return render_ui_template("dashboards/pagespeed.html")


@app.route("/api/status/pagespeed")
def pagespeed_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ---------- connection ----------
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='pagespeed'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    connected = bool(row and row[0] == 1)

    # ---------- api key ----------
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='pagespeed'
        LIMIT 1
    """, (uid,))

    key_row = cur.fetchone()
    api_key_saved = key_row is not None

    conn.close()

    return jsonify({
        "connected": connected,
        "api_key_saved": api_key_saved
    })

@app.route("/api/pagespeed/data")
def pagespeed_data():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_pagespeed
        ORDER BY fetched_at DESC
        LIMIT 1000
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/pagespeed/connect")
def pagespeed_connect_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/pagespeed/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/connectors/pagespeed/disconnect")
def pagespeed_disconnect_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/pagespeed/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)


@app.route("/connectors/pagespeed/save_config", methods=["POST"])
def pagespeed_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/pagespeed/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/pagespeed/job/get")
def pagespeed_job_get_proxy():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/pagespeed/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/connectors/pagespeed/job/save", methods=["POST"])
def pagespeed_job_save_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/pagespeed/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r)

# ================= GOOGLE CLOUD STORAGE =================

@app.route("/connectors/gcs")
@require_login
def gcs_page():
    return render_ui_template("connectors/gcs.html")


# ---- CONNECT (Google OAuth) ----
@app.route("/connectors/gcs/connect")
def gcs_connect():
    return redirect(
        "/_backend/google/connect?source=gcs"
    )

# ---- SYNC ----
@app.route("/connectors/gcs/sync")
def gcs_sync():

    sync_type = request.args.get("sync_type","incremental")

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/gcs",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

# ---- DASHBOARD ----
@app.route("/dashboard/gcs")
def gcs_dashboard():
    return render_ui_template("dashboards/gcs.html")

# ---- DATA APIs ----
@app.route("/api/gcs/data/buckets")
def gcs_buckets():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_gcs_buckets
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/gcs/data/objects")
def gcs_objects():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_gcs_objects
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/api/status/gcs")
def gcs_status():

    uid=request.cookies.get("uid") or "demo_user"
    conn=sqlite3.connect(DB_PATH)
    cur=conn.cursor()

    # credentials saved?
    cur.execute("""
        SELECT 1 FROM connector_configs
        WHERE uid=? AND connector='gcs'
        LIMIT 1
    """,(uid,))
    creds=cur.fetchone()

    # connected?
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='gcs'
        LIMIT 1
    """,(uid,))
    row=cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0]==1),
        "has_credentials": bool(creds)
    })

@app.route("/connectors/gcs/disconnect")
def gcs_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/gcs",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/gcs/job/get")
def gcs_job_get_proxy():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/gcs/job/get",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/gcs/job/save", methods=["POST"])
def gcs_job_save_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/gcs/job/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r)

@app.route("/connectors/gcs/save_app",methods=["POST"])
def gcs_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/gcs/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r),r.status_code

# ================= GOOGLE CLASSROOM =================

@app.route("/connectors/classroom")
@require_login
def classroom_page():
    return render_ui_template("connectors/classroom.html")


@app.route("/connectors/classroom/connect")
def classroom_connect():
    return redirect(
        "/_backend/google/connect?source=classroom"
    )

@app.route("/connectors/classroom/save_app", methods=["POST"])
def classroom_save_app_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/classroom/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/classroom/disconnect")
def classroom_disconnect():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/disconnect/classroom",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )

    return safe_backend_json_response(r)


# ---- SYNC ----
@app.route("/connectors/classroom/sync")
def classroom_sync():

    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/google/sync/classroom",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, timeout=300
    )

    if r.status_code != 200:
        return r.text, 400

    return r.json()


# ---- DASHBOARD ----
@app.route("/dashboard/classroom")
def classroom_dashboard():
    return render_ui_template("dashboards/classroom.html")

@app.route("/api/status/classroom")
def classroom_status():

    uid = request.cookies.get("uid") or "demo_user"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # credentials saved?
    cur.execute("""
        SELECT 1 FROM connector_configs
        WHERE uid=? AND connector='classroom'
        LIMIT 1
    """,(uid,))
    creds = cur.fetchone()

    # oauth connected?
    cur.execute("""
        SELECT enabled
        FROM google_connections
        WHERE uid=? AND source='classroom'
        LIMIT 1
    """,(uid,))
    row = cur.fetchone()

    conn.close()

    return jsonify({
        "connected": bool(row and row[0]==1),
        "has_credentials": bool(creds)
    })

# ---- DATA APIs ----

@app.route("/api/classroom/courses")
def classroom_courses():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_courses
        ORDER BY fetched_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/teachers")
def classroom_teachers():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_teachers
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/students")
def classroom_students():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_students
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/announcements")
def classroom_announcements():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_announcements
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/coursework")
def classroom_coursework():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_coursework
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@app.route("/api/classroom/submissions")
def classroom_submissions():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_classroom_submissions
    """)

    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

# ================= GOOGLE FACT CHECK =================

@app.route("/connectors/factcheck")
@require_login
def factcheck_page():
    return render_ui_template("connectors/factcheck.html")

@app.route("/connectors/factcheck/connect")
def factcheck_connect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/factcheck/connect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/connectors/factcheck/disconnect")
def factcheck_disconnect():
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/connectors/factcheck/disconnect",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r)

@app.route("/connectors/factcheck/sync")
def factcheck_sync():

    query = request.args.get("query")

    if not query:
        return jsonify({
            "status": "error",
            "message": "Query required"
        })


    try:

        r = requests.get(
            "http://127.0.0.1:4000/googlefactcheck/sync/claims",
            params={
                "q": query,
                "limit": 200
            },
            timeout=60
        )


        if r.status_code != 200:
            return jsonify({
                "status": "error",
                "message": r.text
            })


        data = r.json()


        return jsonify({
            "status": "ok",
            "data": data
        })


    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        })

# ---------- DASHBOARD ----------
@app.route("/dashboard/factcheck")
def factcheck_dashboard():
    return render_ui_template("dashboards/factcheck.html")


# ---------- STATUS ----------
@app.route("/api/status/factcheck")
def factcheck_status():
    return jsonify(connector_status("factcheck").json())

@app.route("/connectors/factcheck/job/get")
def factcheck_job_get_proxy():
    return jsonify(connector_job_get("factcheck").json())

@app.route("/connectors/factcheck/job/save", methods=["POST"])
def factcheck_job_save_proxy():
    return jsonify(connector_job_save("factcheck").json())

# ---------- DATA ----------
@app.route("/api/factcheck/claims")
def factcheck_claims():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM google_factcheck_claims
        ORDER BY fetched_at DESC
        LIMIT 500
    """)

    rows = cur.fetchall()

    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/connectors/factcheck/save_config", methods=["POST"])
def factcheck_save_config_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/factcheck/save_config",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

# ================= FACEBOOK PAGES=================

@app.route("/connectors/facebook")
@require_login
def facebook_page():
    return render_ui_template("connectors/facebookpages.html")


@app.route("/connectors/facebook/connect")
def facebook_connect():
    return redirect("/_backend/connectors/facebook/connect")


@app.route("/connectors/facebook/disconnect")
def facebook_disconnect():
    connector_disconnect("facebook")
    return redirect("/connectors/facebook")


@app.route("/connectors/facebook/sync")
def facebook_sync():
    return jsonify(connector_sync("facebook").json())

@app.route("/api/status/facebook")
def facebook_status():
    return jsonify(connector_status("facebook").json())

@app.route("/connectors/facebook/save_app", methods=["POST"])
def facebook_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/facebook/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/facebook/job/get")
def facebook_job_get_proxy():
    r = connector_job_get("facebook")

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/facebook/job/save", methods=["POST"])
def facebook_job_save_proxy():
    r = connector_job_save("facebook")
    return safe_backend_json_response(r, include_status=True)

# ================= FACEBOOK ADS =================

@app.route("/connectors/facebook_ads")
@require_login
def facebook_ads_page():
    return render_ui_template("connectors/facebook_ads.html")

@app.route("/connectors/facebook_ads/connect")
def facebook_ads_connect():
    return redirect("/_backend/connectors/facebook_ads/connect")

@app.route("/connectors/facebook_ads/disconnect")
def facebook_ads_disconnect():
    connector_disconnect("facebook_ads")
    return redirect("/connectors/facebook_ads")

@app.route("/connectors/facebook_ads/sync")
def facebook_ads_sync():
    return jsonify(connector_sync("facebook_ads").json())

@app.route("/api/status/facebook_ads")
def facebook_ads_status():
    return jsonify(connector_status("facebook_ads").json())

@app.route("/connectors/facebook_ads/job/get")
def facebook_ads_job_get_proxy():
    r = connector_job_get("facebook_ads")

    try:
        return safe_backend_json_response(r, include_status=True)
    except:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None
        }), 200

@app.route("/connectors/facebook_ads/job/save", methods=["POST"])
def facebook_ads_job_save_proxy():
    r = connector_job_save("facebook_ads")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/facebook_ads/save_app", methods=["POST"])
def facebook_ads_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/facebook_ads/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

# ================= CHARTBEAT =================

@app.route("/connectors/chartbeat")
@require_login
def chartbeat_page():
    return render_ui_template("connectors/chartbeat.html")


@app.route("/connectors/chartbeat/save_app", methods=["POST"])
def chartbeat_save_app_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/connectors/chartbeat/save_app",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/chartbeat/connect")
def chartbeat_connect():
    r = proxy_get("/connectors/chartbeat/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/chartbeat/disconnect")
def chartbeat_disconnect():
    r = connector_disconnect("chartbeat")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/chartbeat/sync")
def chartbeat_sync():
    return jsonify(connector_sync("chartbeat").json())


@app.route("/api/status/chartbeat")
def chartbeat_status():
    return jsonify(connector_status("chartbeat").json())


@app.route("/connectors/chartbeat/job/get")
def chartbeat_job_get_proxy():
    r = connector_job_get("chartbeat")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({
            "exists": False,
            "sync_type": "incremental",
            "schedule_time": None,
        }), 200


@app.route("/connectors/chartbeat/job/save", methods=["POST"])
def chartbeat_job_save_proxy():
    r = connector_job_save("chartbeat")
    return safe_backend_json_response(r, include_status=True)

# ================= STRIPE =================

@app.route("/connectors/stripe")
@require_login
def stripe_page():
    return render_ui_template("connectors/stripe.html")


@app.route("/connectors/stripe/save_app", methods=["POST"])
@require_login
def stripe_save_app_proxy():
    r = proxy_post("/connectors/stripe/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/stripe/connect")
@require_login
def stripe_connect_proxy():
    r = proxy_get("/connectors/stripe/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/stripe/disconnect")
@require_login
def stripe_disconnect_proxy():
    r = connector_disconnect("stripe")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/stripe/sync")
@require_login
def stripe_sync_proxy():
    r = connector_sync("stripe")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/stripe/status")
@require_login
def stripe_status_proxy():
    r = connector_status("stripe")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/stripe/job/get")
@require_login
def stripe_job_get_proxy():
    r = connector_job_get("stripe")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/stripe/job/save", methods=["POST"])
@require_login
def stripe_job_save_proxy():
    r = connector_job_save("stripe")
    return safe_backend_json_response(r, include_status=True)

# ================= DESTINATION =================

@app.route("/destination/save", methods=["POST"])
def destination_save_proxy():
    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/destination/save",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/destination/list/<source>")
def destination_list_proxy(source):
    base = request.host_url.rstrip("/")
    r = requests.get(
        f"{base}/_backend/destination/list/{source}",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}
    )
    return safe_backend_json_response(r, include_status=True)

@app.route("/destination/activate", methods=["POST"])
def activate_destination_proxy():

    base = request.host_url.rstrip("/")
    r = requests.post(
        f"{base}/_backend/destination/activate",
        cookies=request.cookies,
        headers={"Cookie": request.headers.get("Cookie", "")}, json=request.get_json(silent=True) or request.json or {}
    )

    return safe_backend_json_response(r, include_status=True)

# ================= BIGQUERY DESTINATION ========================

@app.route("/connectors/bigquery")
@require_login
def bigquery_page():
    return render_ui_template("connectors/bigquery.html")


@app.route("/connectors/bigquery/connect")
@require_login
def bigquery_connect_proxy():
    r = proxy_get("/connectors/bigquery/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bigquery/disconnect")
@require_login
def bigquery_disconnect_proxy():
    r = connector_disconnect("bigquery")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bigquery/sync")
@require_login
def bigquery_sync_proxy():
    r = connector_sync("bigquery")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bigquery/status")
@require_login
def bigquery_status_proxy():
    r = connector_status("bigquery")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bigquery/job/get")
@require_login
def bigquery_job_get_proxy():
    r = connector_job_get("bigquery")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bigquery/job/save", methods=["POST"])
@require_login
def bigquery_job_save_proxy():
    r = connector_job_save("bigquery")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bigquery/save_app", methods=["POST"])
@require_login
def bigquery_save_app_proxy():
    r = proxy_post("/connectors/bigquery/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

# ================= AWS RDS =================

@app.route("/connectors/aws_rds")
@require_login
def aws_rds_page():
    return render_ui_template("connectors/aws_rds.html")


@app.route("/connectors/aws_rds/save_app", methods=["POST"])
@require_login
def aws_rds_save_app_proxy():
    r = proxy_post("/connectors/aws_rds/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/aws_rds/connect")
@require_login
def aws_rds_connect_proxy():
    r = proxy_get("/connectors/aws_rds/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/aws_rds/disconnect")
@require_login
def aws_rds_disconnect_proxy():
    r = connector_disconnect("aws_rds")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/aws_rds/sync")
@require_login
def aws_rds_sync_proxy():
    r = connector_sync("aws_rds")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/aws_rds/status")
@require_login
def aws_rds_status_proxy():
    r = connector_status("aws_rds")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/aws_rds/job/get")
@require_login
def aws_rds_job_get_proxy():
    r = connector_job_get("aws_rds")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/aws_rds/job/save", methods=["POST"])
@require_login
def aws_rds_job_save_proxy():
    r = connector_job_save("aws_rds")
    return safe_backend_json_response(r, include_status=True)

# ================= AWS DYNAMODB =================

@app.route("/connectors/dynamodb")
@require_login
def dynamodb_page():
    return render_ui_template("connectors/dynamodb.html")


@app.route("/connectors/dynamodb/save_app", methods=["POST"])
@require_login
def dynamodb_save_app_proxy():
    r = proxy_post("/connectors/dynamodb/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dynamodb/connect")
@require_login
def dynamodb_connect_proxy():
    r = proxy_get("/connectors/dynamodb/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dynamodb/disconnect")
@require_login
def dynamodb_disconnect_proxy():
    r = connector_disconnect("dynamodb")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dynamodb/sync")
@require_login
def dynamodb_sync_proxy():
    r = connector_sync("dynamodb")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dynamodb/status")
@require_login
def dynamodb_status_proxy():
    r = connector_status("dynamodb")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dynamodb/job/get")
@require_login
def dynamodb_job_get_proxy():
    r = connector_job_get("dynamodb")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/dynamodb/job/save", methods=["POST"])
@require_login
def dynamodb_job_save_proxy():
    r = connector_job_save("dynamodb")
    return safe_backend_json_response(r, include_status=True)

# ================= NOTION ========================

@app.route("/connectors/notion")
@require_login
def notion_page():
    return render_ui_template("connectors/notion.html")


@app.route("/connectors/notion/connect")
@require_login
def notion_connect():
    r = proxy_get("/connectors/notion/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/notion/sync")
@require_login
def notion_sync():
    r = connector_sync("notion")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/notion")
@require_login
def notion_status_proxy():
    r = connector_status("notion")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/notion/job/get")
@require_login
def notion_job_get_proxy():
    r = connector_job_get("notion")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/notion/job/save", methods=["POST"])
@require_login
def notion_job_save_proxy():
    r = connector_job_save("notion")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/notion/save_app", methods=["POST"])
@require_login
def notion_save_app_proxy():
    r = proxy_post("/connectors/notion/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/notion/disconnect")
@require_login
def notion_disconnect():
    r = connector_disconnect("notion")
    return safe_backend_json_response(r, include_status=True)


# ================= HUBSPOT ========================

@app.route("/connectors/hubspot")
@require_login
def hubspot_page():
    return render_ui_template("connectors/hubspot.html")


@app.route("/connectors/hubspot/connect")
@require_login
def hubspot_connect_proxy():
    r = proxy_get("/connectors/hubspot/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/hubspot/sync")
@require_login
def hubspot_sync_proxy():
    r = connector_sync("hubspot")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/hubspot")
@require_login
def hubspot_status_proxy():
    r = connector_status("hubspot")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/hubspot/job/get")
@require_login
def hubspot_job_get_proxy():
    r = connector_job_get("hubspot")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/hubspot/job/save", methods=["POST"])
@require_login
def hubspot_job_save_proxy():
    r = connector_job_save("hubspot")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/hubspot/save_app", methods=["POST"])
@require_login
def hubspot_save_app_proxy():
    r = proxy_post("/connectors/hubspot/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/hubspot/disconnect")
@require_login
def hubspot_disconnect_proxy():
    r = connector_disconnect("hubspot")
    return safe_backend_json_response(r, include_status=True)


# ================= AIRTABLE ========================

@app.route("/connectors/airtable")
@require_login
def airtable_page():
    return render_ui_template("connectors/airtable.html")


@app.route("/connectors/airtable/connect")
@require_login
def airtable_connect_proxy():
    r = proxy_get("/connectors/airtable/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/airtable/sync")
@require_login
def airtable_sync_proxy():
    r = connector_sync("airtable")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/airtable")
@require_login
def airtable_status_proxy():
    r = connector_status("airtable")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/airtable/job/get")
@require_login
def airtable_job_get_proxy():
    r = connector_job_get("airtable")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/airtable/job/save", methods=["POST"])
@require_login
def airtable_job_save_proxy():
    r = connector_job_save("airtable")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/airtable/save_app", methods=["POST"])
@require_login
def airtable_save_app_proxy():
    r = proxy_post("/connectors/airtable/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/airtable/disconnect")
@require_login
def airtable_disconnect_proxy():
    r = connector_disconnect("airtable")
    return jsonify(res)


# ================= ZENDESK ========================

@app.route("/connectors/zendesk")
@require_login
def zendesk_page():
    return render_ui_template("connectors/zendesk.html")


@app.route("/connectors/zendesk/connect")
@require_login
def zendesk_connect():
    r = proxy_get("/connectors/zendesk/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zendesk/sync")
@require_login
def zendesk_sync():
    r = connector_sync("zendesk")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/zendesk")
@require_login
def zendesk_status_proxy():
    r = connector_status("zendesk")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zendesk/job/get")
@require_login
def zendesk_job_get_proxy():
    r = connector_job_get("zendesk")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/zendesk/job/save", methods=["POST"])
@require_login
def zendesk_job_save_proxy():
    r = connector_job_save("zendesk")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zendesk/save_app", methods=["POST"])
@require_login
def zendesk_save_app_proxy():
    r = proxy_post("/connectors/zendesk/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zendesk/disconnect")
@require_login
def zendesk_disconnect():
    r = connector_disconnect("zendesk")
    return safe_backend_json_response(r, include_status=True)


# ================= INTERCOM ========================

@app.route("/connectors/intercom")
@require_login
def intercom_page():
    return render_ui_template("connectors/intercom.html")


@app.route("/connectors/intercom/connect")
@require_login
def intercom_connect():
    r = proxy_get("/connectors/intercom/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/intercom/sync")
@require_login
def intercom_sync():
    r = connector_sync("intercom")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/intercom")
@require_login
def intercom_status_proxy():
    r = connector_status("intercom")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/intercom/job/get")
@require_login
def intercom_job_get_proxy():
    r = connector_job_get("intercom")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/intercom/job/save", methods=["POST"])
@require_login
def intercom_job_save_proxy():
    r = connector_job_save("intercom")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/intercom/save_app", methods=["POST"])
@require_login
def intercom_save_app_proxy():
    r = proxy_post("/connectors/intercom/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/intercom/disconnect")
@require_login
def intercom_disconnect():
    r = connector_disconnect("intercom")
    return safe_backend_json_response(r, include_status=True)


# ================= MAILCHIMP ========================

@app.route("/connectors/mailchimp")
@require_login
def mailchimp_page():
    return render_ui_template("connectors/mailchimp.html")


@app.route("/connectors/mailchimp/connect")
@require_login
def mailchimp_connect():
    r = proxy_get("/connectors/mailchimp/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mailchimp/sync")
@require_login
def mailchimp_sync():
    r = connector_sync("mailchimp")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/mailchimp")
@require_login
def mailchimp_status_proxy():
    r = connector_status("mailchimp")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mailchimp/job/get")
@require_login
def mailchimp_job_get_proxy():
    r = connector_job_get("mailchimp")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/mailchimp/job/save", methods=["POST"])
@require_login
def mailchimp_job_save_proxy():
    r = connector_job_save("mailchimp")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mailchimp/save_app", methods=["POST"])
@require_login
def mailchimp_save_app_proxy():
    r = proxy_post("/connectors/mailchimp/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mailchimp/disconnect")
@require_login
def mailchimp_disconnect():
    r = connector_disconnect("mailchimp")
    return safe_backend_json_response(r, include_status=True)


# ================= TWILIO ========================

@app.route("/connectors/twilio")
@require_login
def twilio_page():
    return render_ui_template("connectors/twilio.html")


@app.route("/connectors/twilio/connect")
@require_login
def twilio_connect():
    r = proxy_get("/connectors/twilio/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/twilio/sync")
@require_login
def twilio_sync():
    r = connector_sync("twilio")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/twilio")
@require_login
def twilio_status_proxy():
    r = connector_status("twilio")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/twilio/job/get")
@require_login
def twilio_job_get_proxy():
    r = connector_job_get("twilio")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/twilio/job/save", methods=["POST"])
@require_login
def twilio_job_save_proxy():
    r = connector_job_save("twilio")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/twilio/save_app", methods=["POST"])
@require_login
def twilio_save_app_proxy():
    r = proxy_post("/connectors/twilio/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/twilio/disconnect")
@require_login
def twilio_disconnect():
    r = connector_disconnect("twilio")
    return safe_backend_json_response(r, include_status=True)


# ================= SHOPIFY ========================

@app.route("/connectors/shopify")
@require_login
def shopify_page():
    return render_ui_template("connectors/shopify.html")


@app.route("/connectors/shopify/connect")
@require_login
def shopify_connect():
    r = proxy_get("/connectors/shopify/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/shopify/sync")
@require_login
def shopify_sync():
    r = connector_sync("shopify")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/shopify/status")
@require_login
def shopify_status_proxy():
    r = connector_status("shopify")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/shopify/job/get")
@require_login
def shopify_job_get_proxy():
    r = connector_job_get("shopify")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/shopify/job/save", methods=["POST"])
@require_login
def shopify_job_save_proxy():
    r = connector_job_save("shopify")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/shopify/save_app", methods=["POST"])
@require_login
def shopify_save_app_proxy():
    r = proxy_post("/connectors/shopify/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/shopify/disconnect")
@require_login
def shopify_disconnect():
    r = connector_disconnect("shopify")
    return safe_backend_json_response(r, include_status=True)

# ---------------- PIPEDRIVE ----------------

@app.route("/connectors/pipedrive")
@require_login
def pipedrive_page():
    return render_ui_template("connectors/pipedrive.html")


@app.route("/connectors/pipedrive/connect")
@require_login
def pipedrive_connect_proxy():
    r = proxy_get("/connectors/pipedrive/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pipedrive/sync")
@require_login
def pipedrive_sync_proxy():
    r = connector_sync("pipedrive")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/pipedrive")
@require_login
def pipedrive_status_proxy():
    r = connector_status("pipedrive")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pipedrive/job/get")
@require_login
def pipedrive_job_get_proxy():
    r = connector_job_get("pipedrive")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/pipedrive/job/save", methods=["POST"])
@require_login
def pipedrive_job_save_proxy():
    r = connector_job_save("pipedrive")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pipedrive/save_app", methods=["POST"])
@require_login
def pipedrive_save_app_proxy():
    r = proxy_post("/connectors/pipedrive/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pipedrive/disconnect")
@require_login
def pipedrive_disconnect_proxy():
    r = proxy_get("/connectors/pipedrive/disconnect")
    return safe_backend_json_response(r, include_status=True)


# ---------------- FRESHDESK ----------------

@app.route("/connectors/freshdesk")
@require_login
def freshdesk_page():
    return render_ui_template("connectors/freshdesk.html")


@app.route("/connectors/freshdesk/connect")
@require_login
def freshdesk_connect_proxy():
    r = proxy_get("/connectors/freshdesk/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/freshdesk/sync")
@require_login
def freshdesk_sync_proxy():
    r = connector_sync("freshdesk")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/freshdesk")
@require_login
def freshdesk_status_proxy():
    r = connector_status("freshdesk")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/freshdesk/job/get")
@require_login
def freshdesk_job_get_proxy():
    r = connector_job_get("freshdesk")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/freshdesk/job/save", methods=["POST"])
@require_login
def freshdesk_job_save_proxy():
    r = connector_job_save("freshdesk")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/freshdesk/save_app", methods=["POST"])
@require_login
def freshdesk_save_app_proxy():
    r = proxy_post("/connectors/freshdesk/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/freshdesk/disconnect")
@require_login
def freshdesk_disconnect_proxy():
    r = proxy_get("/connectors/freshdesk/disconnect")
    return safe_backend_json_response(r, include_status=True)


# ---------------- KLAVIYO ----------------

@app.route("/connectors/klaviyo")
@require_login
def klaviyo_page():
    return render_ui_template("connectors/klaviyo.html")


@app.route("/connectors/klaviyo/connect")
@require_login
def klaviyo_connect_proxy():
    r = proxy_get("/connectors/klaviyo/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/klaviyo/sync")
@require_login
def klaviyo_sync_proxy():
    r = connector_sync("klaviyo")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/klaviyo")
@require_login
def klaviyo_status_proxy():
    r = connector_status("klaviyo")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/klaviyo/job/get")
@require_login
def klaviyo_job_get_proxy():
    r = connector_job_get("klaviyo")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/klaviyo/job/save", methods=["POST"])
@require_login
def klaviyo_job_save_proxy():
    r = connector_job_save("klaviyo")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/klaviyo/save_app", methods=["POST"])
@require_login
def klaviyo_save_app_proxy():
    r = proxy_post("/connectors/klaviyo/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/klaviyo/disconnect")
@require_login
def klaviyo_disconnect_proxy():
    r = proxy_get("/connectors/klaviyo/disconnect")
    return safe_backend_json_response(r, include_status=True)


# ---------------- AMPLITUDE ----------------

@app.route("/connectors/amplitude")
@require_login
def amplitude_page():
    return render_ui_template("connectors/amplitude.html")


@app.route("/connectors/amplitude/connect")
@require_login
def amplitude_connect_proxy():
    r = proxy_get("/connectors/amplitude/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/amplitude/sync")
@require_login
def amplitude_sync_proxy():
    r = connector_sync("amplitude")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/amplitude")
@require_login
def amplitude_status_proxy():
    r = connector_status("amplitude")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/amplitude/job/get")
@require_login
def amplitude_job_get_proxy():
    r = connector_job_get("amplitude")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/amplitude/job/save", methods=["POST"])
@require_login
def amplitude_job_save_proxy():
    r = connector_job_save("amplitude")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/amplitude/save_app", methods=["POST"])
@require_login
def amplitude_save_app_proxy():
    r = proxy_post("/connectors/amplitude/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/amplitude/disconnect")
@require_login
def amplitude_disconnect_proxy():
    r = proxy_get("/connectors/amplitude/disconnect")
    return safe_backend_json_response(r, include_status=True)

# SALESFORCE ROUTES
@app.route("/connectors/salesforce")
@require_login
def salesforce_page():
    return render_ui_template("connectors/salesforce.html")


@app.route("/connectors/salesforce/connect")
@require_login
def salesforce_connect_proxy():
    r = proxy_get("/connectors/salesforce/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/salesforce/sync")
@require_login
def salesforce_sync_proxy():
    r = connector_sync("salesforce")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/salesforce")
@require_login
def salesforce_status_proxy():
    r = connector_status("salesforce")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/salesforce/job/get")
@require_login
def salesforce_job_get_proxy():
    r = connector_job_get("salesforce")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/salesforce/job/save", methods=["POST"])
@require_login
def salesforce_job_save_proxy():
    r = connector_job_save("salesforce")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/salesforce/save_app", methods=["POST"])
@require_login
def salesforce_save_app_proxy():
    r = proxy_post("/connectors/salesforce/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/salesforce/disconnect")
@require_login
def salesforce_disconnect_proxy():
    r = proxy_get("/connectors/salesforce/disconnect")
    return safe_backend_json_response(r, include_status=True)

# JIRA ROUTES

@app.route("/connectors/jira")
@require_login
def jira_page():
    return render_ui_template("connectors/jira.html")


@app.route("/connectors/jira/connect")
@require_login
def jira_connect_proxy():
    r = proxy_get("/connectors/jira/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/jira/sync")
@require_login
def jira_sync_proxy():
    r = connector_sync("jira")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/jira")
@require_login
def jira_status_proxy():
    r = connector_status("jira")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/jira/job/get")
@require_login
def jira_job_get_proxy():
    r = connector_job_get("jira")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/jira/job/save", methods=["POST"])
@require_login
def jira_job_save_proxy():
    r = connector_job_save("jira")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/jira/save_app", methods=["POST"])
@require_login
def jira_save_app_proxy():
    r = proxy_post("/connectors/jira/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/jira/disconnect")
@require_login
def jira_disconnect_proxy():
    r = proxy_get("/connectors/jira/disconnect")
    return safe_backend_json_response(r, include_status=True)

# ZOHO CRM ROUTES
@app.route("/connectors/zoho_crm")
@require_login
def zoho_crm_page():
    return render_ui_template("connectors/zoho_crm.html")


@app.route("/connectors/zoho_crm/connect")
@require_login
def zoho_crm_connect_proxy():
    r = proxy_get("/connectors/zoho_crm/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zoho_crm/sync")
@require_login
def zoho_crm_sync_proxy():
    r = connector_sync("zoho_crm")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/zoho_crm")
@require_login
def zoho_crm_status_proxy():
    r = connector_status("zoho_crm")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zoho_crm/job/get")
@require_login
def zoho_crm_job_get_proxy():
    r = connector_job_get("zoho_crm")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/zoho_crm/job/save", methods=["POST"])
@require_login
def zoho_crm_job_save_proxy():
    r = connector_job_save("zoho_crm")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zoho_crm/save_app", methods=["POST"])
@require_login
def zoho_crm_save_app_proxy():
    r = proxy_post("/connectors/zoho_crm/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/zoho_crm/disconnect")
@require_login
def zoho_crm_disconnect_proxy():
    r = proxy_get("/connectors/zoho_crm/disconnect")
    return safe_backend_json_response(r, include_status=True)

# PAYPAL ROUTES

@app.route("/connectors/paypal")
@require_login
def paypal_page():
    return render_ui_template("connectors/paypal.html")


@app.route("/connectors/paypal/connect")
@require_login
def paypal_connect_proxy():
    r = proxy_get("/connectors/paypal/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/paypal/sync")
@require_login
def paypal_sync_proxy():
    r = connector_sync("paypal")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/paypal")
@require_login
def paypal_status_proxy():
    r = connector_status("paypal")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/paypal/job/get")
@require_login
def paypal_job_get_proxy():
    r = connector_job_get("paypal")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/paypal/job/save", methods=["POST"])
@require_login
def paypal_job_save_proxy():
    r = connector_job_save("paypal")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/paypal/save_app", methods=["POST"])
@require_login
def paypal_save_app_proxy():
    r = proxy_post("/connectors/paypal/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/paypal/disconnect")
@require_login
def paypal_disconnect_proxy():
    r = proxy_get("/connectors/paypal/disconnect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/asana")
@require_login
def asana_page():
    return render_ui_template("connectors/asana.html")


@app.route("/connectors/asana/connect")
@require_login
def asana_connect_proxy():
    r = proxy_get("/connectors/asana/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/asana/sync")
@require_login
def asana_sync_proxy():
    r = connector_sync("asana")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/asana")
@require_login
def asana_status_proxy():
    r = connector_status("asana")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/asana/job/get")
@require_login
def asana_job_get_proxy():
    r = connector_job_get("asana")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/asana/job/save", methods=["POST"])
@require_login
def asana_job_save_proxy():
    r = connector_job_save("asana")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/asana/save_app", methods=["POST"])
@require_login
def asana_save_app_proxy():
    r = proxy_post("/connectors/asana/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/asana/disconnect")
@require_login
def asana_disconnect_proxy():
    r = proxy_get("/connectors/asana/disconnect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/tableau")
@require_login
def tableau_page():
    return render_ui_template("connectors/tableau.html")


@app.route("/connectors/tableau/connect")
@require_login
def tableau_connect_proxy():
    r = proxy_get("/connectors/tableau/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/tableau/sync")
@require_login
def tableau_sync_proxy():
    r = connector_sync("tableau")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/tableau")
@require_login
def tableau_status_proxy():
    r = connector_status("tableau")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/tableau/job/get")
@require_login
def tableau_job_get_proxy():
    r = connector_job_get("tableau")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/tableau/job/save", methods=["POST"])
@require_login
def tableau_job_save_proxy():
    r = connector_job_save("tableau")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/tableau/save_app", methods=["POST"])
@require_login
def tableau_save_app_proxy():
    r = proxy_post("/connectors/tableau/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/tableau/disconnect")
@require_login
def tableau_disconnect_proxy():
    r = proxy_get("/connectors/tableau/disconnect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/power_bi")
@require_login
def power_bi_page():
    return render_ui_template("connectors/power_bi.html")


@app.route("/connectors/power_bi/connect")
@require_login
def power_bi_connect_proxy():
    r = proxy_get("/connectors/power_bi/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/power_bi/sync")
@require_login
def power_bi_sync_proxy():
    r = connector_sync("power_bi")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/power_bi")
@require_login
def power_bi_status_proxy():
    r = connector_status("power_bi")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/power_bi/job/get")
@require_login
def power_bi_job_get_proxy():
    r = connector_job_get("power_bi")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/power_bi/job/save", methods=["POST"])
@require_login
def power_bi_job_save_proxy():
    r = connector_job_save("power_bi")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/power_bi/save_app", methods=["POST"])
@require_login
def power_bi_save_app_proxy():
    r = proxy_post("/connectors/power_bi/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/power_bi/disconnect")
@require_login
def power_bi_disconnect_proxy():
    r = proxy_get("/connectors/power_bi/disconnect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/workday")
@require_login
def workday_page():
    return render_ui_template("connectors/workday.html")


@app.route("/connectors/workday/connect")
@require_login
def workday_connect_proxy():
    r = proxy_get("/connectors/workday/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/workday/sync")
@require_login
def workday_sync_proxy():
    r = connector_sync("workday")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/workday")
@require_login
def workday_status_proxy():
    r = connector_status("workday")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/workday/job/get")
@require_login
def workday_job_get_proxy():
    r = connector_job_get("workday")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/workday/job/save", methods=["POST"])
@require_login
def workday_job_save_proxy():
    r = connector_job_save("workday")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/workday/save_app", methods=["POST"])
@require_login
def workday_save_app_proxy():
    r = proxy_post("/connectors/workday/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/workday/disconnect")
@require_login
def workday_disconnect_proxy():
    r = proxy_get("/connectors/workday/disconnect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/ebay")
@require_login
def ebay_page():
    return render_ui_template("connectors/ebay.html")


@app.route("/connectors/ebay/connect")
@require_login
def ebay_connect_proxy():
    r = proxy_get("/connectors/ebay/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/ebay/sync")
@require_login
def ebay_sync_proxy():
    r = connector_sync("ebay")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/ebay")
@require_login
def ebay_status_proxy():
    r = connector_status("ebay")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/ebay/job/get")
@require_login
def ebay_job_get_proxy():
    r = connector_job_get("ebay")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/ebay/job/save", methods=["POST"])
@require_login
def ebay_job_save_proxy():
    r = connector_job_save("ebay")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/ebay/save_app", methods=["POST"])
@require_login
def ebay_save_app_proxy():
    r = proxy_post("/connectors/ebay/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/ebay/disconnect")
@require_login
def ebay_disconnect_proxy():
    r = proxy_get("/connectors/ebay/disconnect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sendgrid")
@require_login
def sendgrid_page():
    return render_ui_template("connectors/sendgrid.html")


@app.route("/connectors/sendgrid/connect")
@require_login
def sendgrid_connect_proxy():
    r = proxy_get("/connectors/sendgrid/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/sendgrid/sync")
@require_login
def sendgrid_sync_proxy():
    r = connector_sync("sendgrid")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/sendgrid")
@require_login
def sendgrid_status_proxy():
    r = connector_status("sendgrid")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/sendgrid/job/get")
@require_login
def sendgrid_job_get_proxy():
    r = connector_job_get("sendgrid")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/sendgrid/job/save", methods=["POST"])
@require_login
def sendgrid_job_save_proxy():
    r = connector_job_save("sendgrid")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/sendgrid/save_app", methods=["POST"])
@require_login
def sendgrid_save_app_proxy():
    r = proxy_post("/connectors/sendgrid/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/sendgrid/disconnect")
@require_login
def sendgrid_disconnect_proxy():
    r = proxy_get("/connectors/sendgrid/disconnect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/mixpanel")
@require_login
def mixpanel_page():
    return render_ui_template("connectors/mixpanel.html")

@app.route("/connectors/mixpanel/connect")
@require_login
def mixpanel_connect_proxy():
    r = proxy_get("/connectors/mixpanel/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mixpanel/sync")
@require_login
def mixpanel_sync_proxy():
    r = connector_sync("mixpanel")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/mixpanel")
@require_login
def mixpanel_status_proxy():
    r = connector_status("mixpanel")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mixpanel/job/get")
@require_login
def mixpanel_job_get_proxy():
    r = connector_job_get("mixpanel")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/mixpanel/job/save", methods=["POST"])
@require_login
def mixpanel_job_save_proxy():
    r = connector_job_save("mixpanel")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mixpanel/save_app", methods=["POST"])
@require_login
def mixpanel_save_app_proxy():
    r = proxy_post("/connectors/mixpanel/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/mixpanel/disconnect")
@require_login
def mixpanel_disconnect_proxy():
    r = proxy_get("/connectors/mixpanel/disconnect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/monday")
@require_login
def monday_page():
    return render_ui_template("connectors/monday.html")

@app.route("/connectors/monday/connect")
@require_login
def monday_connect_proxy():
    r = proxy_get("/connectors/monday/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/monday/sync")
@require_login
def monday_sync_proxy():
    r = connector_sync("monday")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/monday")
@require_login
def monday_status_proxy():
    r = connector_status("monday")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/monday/job/get")
@require_login
def monday_job_get_proxy():
    r = connector_job_get("monday")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/monday/job/save", methods=["POST"])
@require_login
def monday_job_save_proxy():
    r = connector_job_save("monday")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/monday/save_app", methods=["POST"])
@require_login
def monday_save_app_proxy():
    r = proxy_post("/connectors/monday/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/monday/disconnect")
@require_login
def monday_disconnect_proxy():
    r = proxy_get("/connectors/monday/disconnect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/clickup")
@require_login
def clickup_page():
    return render_ui_template("connectors/clickup.html")

@app.route("/connectors/clickup/connect")
@require_login
def clickup_connect_proxy():
    r = proxy_get("/connectors/clickup/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/clickup/sync")
@require_login
def clickup_sync_proxy():
    r = connector_sync("clickup")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/clickup")
@require_login
def clickup_status_proxy():
    r = connector_status("clickup")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/clickup/job/get")
@require_login
def clickup_job_get_proxy():
    r = connector_job_get("clickup")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/clickup/job/save", methods=["POST"])
@require_login
def clickup_job_save_proxy():
    r = connector_job_save("clickup")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/clickup/save_app", methods=["POST"])
@require_login
def clickup_save_app_proxy():
    r = proxy_post("/connectors/clickup/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/clickup/disconnect")
@require_login
def clickup_disconnect_proxy():
    r = proxy_get("/connectors/clickup/disconnect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/helpscout")
@require_login
def helpscout_page():
    return render_ui_template("connectors/helpscout.html")

@app.route("/connectors/helpscout/connect")
@require_login
def helpscout_connect_proxy():
    r = proxy_get("/connectors/helpscout/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/helpscout/sync")
@require_login
def helpscout_sync_proxy():
    r = connector_sync("helpscout")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/helpscout")
@require_login
def helpscout_status_proxy():
    r = connector_status("helpscout")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/helpscout/job/get")
@require_login
def helpscout_job_get_proxy():
    r = connector_job_get("helpscout")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/helpscout/job/save", methods=["POST"])
@require_login
def helpscout_job_save_proxy():
    r = connector_job_save("helpscout")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/helpscout/save_app", methods=["POST"])
@require_login
def helpscout_save_app_proxy():
    r = proxy_post("/connectors/helpscout/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/helpscout/disconnect")
@require_login
def helpscout_disconnect_proxy():
    r = proxy_get("/connectors/helpscout/disconnect")
    return safe_backend_json_response(r, include_status=True)

# ================= LOOKER =================

@app.route("/connectors/looker")
@require_login
def looker_page():
    return render_ui_template("connectors/looker.html")

@app.route("/connectors/looker/connect")
@require_login
def looker_connect():
    r = proxy_get("/connectors/looker/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/looker/sync")
@require_login
def looker_sync():
    r = connector_sync("looker")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/looker")
@require_login
def looker_status_proxy():
    r = connector_status("looker")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/looker/job/get")
@require_login
def looker_job_get_proxy():
    r = connector_job_get("looker")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/looker/job/save", methods=["POST"])
@require_login
def looker_job_save_proxy():
    r = connector_job_save("looker")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/looker/save_app", methods=["POST"])
@require_login
def looker_save_app_proxy():
    r = proxy_post("/connectors/looker/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/looker/disconnect")
@require_login
def looker_disconnect():
    r = connector_disconnect("looker")
    return safe_backend_json_response(r, include_status=True)


# ================= SUPERSET =================

@app.route("/connectors/superset")
@require_login
def superset_page():
    return render_ui_template("connectors/superset.html")

@app.route("/connectors/superset/connect")
@require_login
def superset_connect():
    r = proxy_get("/connectors/superset/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/superset/sync")
@require_login
def superset_sync():
    r = connector_sync("superset")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/superset")
@require_login
def superset_status_proxy():
    r = connector_status("superset")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/superset/job/get")
@require_login
def superset_job_get_proxy():
    r = connector_job_get("superset")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/superset/job/save", methods=["POST"])
@require_login
def superset_job_save_proxy():
    r = connector_job_save("superset")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/superset/save_app", methods=["POST"])
@require_login
def superset_save_app_proxy():
    r = proxy_post("/connectors/superset/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/superset/disconnect")
@require_login
def superset_disconnect():
    r = connector_disconnect("superset")
    return safe_backend_json_response(r, include_status=True)


# ================= AZURE_BLOB =================

@app.route("/connectors/azure_blob")
@require_login
def azure_blob_page():
    return render_ui_template("connectors/azure_blob.html")

@app.route("/connectors/azure_blob/connect")
@require_login
def azure_blob_connect():
    r = proxy_get("/connectors/azure_blob/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/azure_blob/sync")
@require_login
def azure_blob_sync():
    r = connector_sync("azure_blob")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/azure_blob")
@require_login
def azure_blob_status_proxy():
    r = connector_status("azure_blob")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/azure_blob/job/get")
@require_login
def azure_blob_job_get_proxy():
    r = connector_job_get("azure_blob")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/azure_blob/job/save", methods=["POST"])
@require_login
def azure_blob_job_save_proxy():
    r = connector_job_save("azure_blob")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/azure_blob/save_app", methods=["POST"])
@require_login
def azure_blob_save_app_proxy():
    r = proxy_post("/connectors/azure_blob/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/azure_blob/disconnect")
@require_login
def azure_blob_disconnect():
    r = connector_disconnect("azure_blob")
    return safe_backend_json_response(r, include_status=True)


# ================= DATADOG =================

@app.route("/connectors/datadog")
@require_login
def datadog_page():
    return render_ui_template("connectors/datadog.html")

@app.route("/connectors/datadog/connect")
@require_login
def datadog_connect():
    r = proxy_get("/connectors/datadog/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/datadog/sync")
@require_login
def datadog_sync():
    r = connector_sync("datadog")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/datadog")
@require_login
def datadog_status_proxy():
    r = connector_status("datadog")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/datadog/job/get")
@require_login
def datadog_job_get_proxy():
    r = connector_job_get("datadog")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/datadog/job/save", methods=["POST"])
@require_login
def datadog_job_save_proxy():
    r = connector_job_save("datadog")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/datadog/save_app", methods=["POST"])
@require_login
def datadog_save_app_proxy():
    r = proxy_post("/connectors/datadog/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/datadog/disconnect")
@require_login
def datadog_disconnect():
    r = connector_disconnect("datadog")
    return safe_backend_json_response(r, include_status=True)



# ================= OKTA =================

@app.route("/connectors/okta")
@require_login
def okta_page():
    return render_ui_template("connectors/okta.html")

@app.route("/connectors/okta/connect")
@require_login
def okta_connect():
    r = proxy_get("/connectors/okta/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/okta/sync")
@require_login
def okta_sync():
    r = connector_sync("okta")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/okta")
@require_login
def okta_status_proxy():
    r = connector_status("okta")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/okta/job/get")
@require_login
def okta_job_get_proxy():
    r = connector_job_get("okta")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/okta/job/save", methods=["POST"])
@require_login
def okta_job_save_proxy():
    r = connector_job_save("okta")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/okta/save_app", methods=["POST"])
@require_login
def okta_save_app_proxy():
    r = proxy_post("/connectors/okta/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/okta/disconnect")
@require_login
def okta_disconnect():
    r = connector_disconnect("okta")
    return safe_backend_json_response(r, include_status=True)


# ================= AUTH0 =================

@app.route("/connectors/auth0")
@require_login
def auth0_page():
    return render_ui_template("connectors/auth0.html")

@app.route("/connectors/auth0/connect")
@require_login
def auth0_connect():
    r = proxy_get("/connectors/auth0/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/auth0/sync")
@require_login
def auth0_sync():
    r = connector_sync("auth0")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/auth0")
@require_login
def auth0_status_proxy():
    r = connector_status("auth0")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/auth0/job/get")
@require_login
def auth0_job_get_proxy():
    r = connector_job_get("auth0")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/auth0/job/save", methods=["POST"])
@require_login
def auth0_job_save_proxy():
    r = connector_job_save("auth0")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/auth0/save_app", methods=["POST"])
@require_login
def auth0_save_app_proxy():
    r = proxy_post("/connectors/auth0/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/auth0/disconnect")
@require_login
def auth0_disconnect():
    r = connector_disconnect("auth0")
    return safe_backend_json_response(r, include_status=True)


# ================= CLOUDFLARE =================

@app.route("/connectors/cloudflare")
@require_login
def cloudflare_page():
    return render_ui_template("connectors/cloudflare.html")

@app.route("/connectors/cloudflare/connect")
@require_login
def cloudflare_connect():
    r = proxy_get("/connectors/cloudflare/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/cloudflare/sync")
@require_login
def cloudflare_sync():
    r = connector_sync("cloudflare")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/cloudflare")
@require_login
def cloudflare_status_proxy():
    r = connector_status("cloudflare")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/cloudflare/job/get")
@require_login
def cloudflare_job_get_proxy():
    r = connector_job_get("cloudflare")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/cloudflare/job/save", methods=["POST"])
@require_login
def cloudflare_job_save_proxy():
    r = connector_job_save("cloudflare")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/cloudflare/save_app", methods=["POST"])
@require_login
def cloudflare_save_app_proxy():
    r = proxy_post("/connectors/cloudflare/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/cloudflare/disconnect")
@require_login
def cloudflare_disconnect():
    r = connector_disconnect("cloudflare")
    return safe_backend_json_response(r, include_status=True)


# ================= SENTRY =================

@app.route("/connectors/sentry")
@require_login
def sentry_page():
    return render_ui_template("connectors/sentry.html")

@app.route("/connectors/sentry/connect")
@require_login
def sentry_connect():
    r = proxy_get("/connectors/sentry/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sentry/sync")
@require_login
def sentry_sync():
    r = connector_sync("sentry")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/sentry")
@require_login
def sentry_status_proxy():
    r = connector_status("sentry")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sentry/job/get")
@require_login
def sentry_job_get_proxy():
    r = connector_job_get("sentry")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sentry/job/save", methods=["POST"])
@require_login
def sentry_job_save_proxy():
    r = connector_job_save("sentry")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sentry/save_app", methods=["POST"])
@require_login
def sentry_save_app_proxy():
    r = proxy_post("/connectors/sentry/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/sentry/disconnect")
@require_login
def sentry_disconnect():
    r = connector_disconnect("sentry")
    return safe_backend_json_response(r, include_status=True)


# ================= QUICKBOOKS =================

@app.route("/connectors/quickbooks")
@require_login
def quickbooks_page():
    return render_ui_template("connectors/quickbooks.html")

@app.route("/connectors/quickbooks/connect")
@require_login
def quickbooks_connect():
    r = proxy_get("/connectors/quickbooks/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/quickbooks/callback")
def quickbooks_callback():
    r = proxy_get(f"/connectors/quickbooks/callback?{request.query_string.decode()}")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/quickbooks/sync")
@require_login
def quickbooks_sync():
    r = connector_sync("quickbooks")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/quickbooks")
@require_login
def quickbooks_status_proxy():
    r = connector_status("quickbooks")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/quickbooks/save_app", methods=["POST"])
@require_login
def quickbooks_save_app_proxy():
    r = proxy_post("/connectors/quickbooks/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/quickbooks/disconnect")
@require_login
def quickbooks_disconnect():
    r = connector_disconnect("quickbooks")
    return safe_backend_json_response(r, include_status=True)

# ================= XERO =================

@app.route("/connectors/xero")
@require_login
def xero_page():
    return render_ui_template("connectors/xero.html")

@app.route("/connectors/xero/connect")
@require_login
def xero_connect():
    r = proxy_get("/connectors/xero/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/xero/callback")
def xero_callback():
    r = proxy_get(f"/connectors/xero/callback?{request.query_string.decode()}")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/xero/sync")
@require_login
def xero_sync():
    r = connector_sync("xero")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/xero")
@require_login
def xero_status_proxy():
    r = connector_status("xero")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/xero/save_app", methods=["POST"])
@require_login
def xero_save_app_proxy():
    r = proxy_post("/connectors/xero/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/xero/disconnect")
@require_login
def xero_disconnect():
    r = connector_disconnect("xero")
    return safe_backend_json_response(r, include_status=True)

# ================= AMAZON SELLER =================

@app.route("/connectors/amazon_seller")
@require_login
def amazon_seller_page():
    return render_ui_template("connectors/amazon_seller.html")

@app.route("/connectors/amazon_seller/connect")
@require_login
def amazon_seller_connect():
    r = proxy_get("/connectors/amazon_seller/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/amazon_seller/callback")
def amazon_seller_callback():
    r = proxy_get(f"/connectors/amazon_seller/callback?{request.query_string.decode()}")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/amazon_seller/sync")
@require_login
def amazon_seller_sync():
    r = connector_sync("amazon_seller")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/amazon_seller")
@require_login
def amazon_seller_status_proxy():
    r = connector_status("amazon_seller")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/amazon_seller/save_app", methods=["POST"])
@require_login
def amazon_seller_save_app_proxy():
    r = proxy_post("/connectors/amazon_seller/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/amazon_seller/disconnect")
@require_login
def amazon_seller_disconnect():
    r = connector_disconnect("amazon_seller")
    return safe_backend_json_response(r, include_status=True)

# ================= NEW RELIC =================

@app.route("/connectors/newrelic")
@require_login
def newrelic_page():
    return render_ui_template("connectors/newrelic.html")

@app.route("/connectors/newrelic/sync")
@require_login
def newrelic_sync():
    r = connector_sync("newrelic")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/newrelic")
@require_login
def newrelic_status_proxy():
    r = connector_status("newrelic")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/newrelic/save_app", methods=["POST"])
@require_login
def newrelic_save_app_proxy():
    r = proxy_post("/connectors/newrelic/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/newrelic/disconnect")
@require_login
def newrelic_disconnect():
    r = connector_disconnect("newrelic")
    return safe_backend_json_response(r, include_status=True)

# OPENAI
@app.route("/connectors/openai")
@require_login
def openai_page():
    return render_ui_template("connectors/openai.html")

@app.route("/connectors/openai/connect")
@require_login
def openai_connect_proxy():
    r = proxy_get("/connectors/openai/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/openai/sync")
@require_login
def openai_sync_proxy():
    r = connector_sync("openai")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/openai")
@require_login
def openai_status_proxy():
    r = connector_status("openai")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/openai/job/get")
@require_login
def openai_job_get_proxy():
    r = connector_job_get("openai")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/openai/job/save", methods=["POST"])
@require_login
def openai_job_save_proxy():
    r = connector_job_save("openai")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/openai/save_app", methods=["POST"])
@require_login
def openai_save_app_proxy():
    r = proxy_post("/connectors/openai/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/openai/disconnect")
@require_login
def openai_disconnect_proxy():
    r = proxy_get("/connectors/openai/disconnect")
    return safe_backend_json_response(r, include_status=True)


# HUGGINGFACE
@app.route("/connectors/huggingface")
@require_login
def huggingface_page():
    return render_ui_template("connectors/huggingface.html")

@app.route("/connectors/huggingface/connect")
@require_login
def huggingface_connect_proxy():
    r = proxy_get("/connectors/huggingface/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/huggingface/sync")
@require_login
def huggingface_sync_proxy():
    r = connector_sync("huggingface")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/huggingface")
@require_login
def huggingface_status_proxy():
    r = connector_status("huggingface")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/huggingface/job/get")
@require_login
def huggingface_job_get_proxy():
    r = connector_job_get("huggingface")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/huggingface/job/save", methods=["POST"])
@require_login
def huggingface_job_save_proxy():
    r = connector_job_save("huggingface")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/huggingface/save_app", methods=["POST"])
@require_login
def huggingface_save_app_proxy():
    r = proxy_post("/connectors/huggingface/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/huggingface/disconnect")
@require_login
def huggingface_disconnect_proxy():
    r = proxy_get("/connectors/huggingface/disconnect")
    return safe_backend_json_response(r, include_status=True)


# AIRFLOW
@app.route("/connectors/airflow")
@require_login
def airflow_page():
    return render_ui_template("connectors/airflow.html")

@app.route("/connectors/airflow/connect")
@require_login
def airflow_connect_proxy():
    r = proxy_get("/connectors/airflow/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/airflow/sync")
@require_login
def airflow_sync_proxy():
    r = connector_sync("airflow")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/airflow")
@require_login
def airflow_status_proxy():
    r = connector_status("airflow")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/airflow/job/get")
@require_login
def airflow_job_get_proxy():
    r = connector_job_get("airflow")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/airflow/job/save", methods=["POST"])
@require_login
def airflow_job_save_proxy():
    r = connector_job_save("airflow")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/airflow/save_app", methods=["POST"])
@require_login
def airflow_save_app_proxy():
    r = proxy_post("/connectors/airflow/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/airflow/disconnect")
@require_login
def airflow_disconnect_proxy():
    r = proxy_get("/connectors/airflow/disconnect")
    return safe_backend_json_response(r, include_status=True)


# KAFKA
@app.route("/connectors/kafka")
@require_login
def kafka_page():
    return render_ui_template("connectors/kafka.html")

@app.route("/connectors/kafka/connect")
@require_login
def kafka_connect_proxy():
    r = proxy_get("/connectors/kafka/connect")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/kafka/sync")
@require_login
def kafka_sync_proxy():
    r = connector_sync("kafka")
    return safe_backend_json_response(r, include_status=True)

@app.route("/api/status/kafka")
@require_login
def kafka_status_proxy():
    r = connector_status("kafka")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/kafka/job/get")
@require_login
def kafka_job_get_proxy():
    r = connector_job_get("kafka")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200

@app.route("/connectors/kafka/job/save", methods=["POST"])
@require_login
def kafka_job_save_proxy():
    r = connector_job_save("kafka")
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/kafka/save_app", methods=["POST"])
@require_login
def kafka_save_app_proxy():
    r = proxy_post("/connectors/kafka/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)

@app.route("/connectors/kafka/disconnect")
@require_login
def kafka_disconnect_proxy():
    r = proxy_get("/connectors/kafka/disconnect")
    return safe_backend_json_response(r, include_status=True)

# ================= DBT ========================

@app.route("/connectors/dbt")
@require_login
def dbt_page():
    return render_ui_template("connectors/dbt.html")


@app.route("/connectors/dbt/connect")
@require_login
def dbt_connect_proxy():
    r = proxy_get("/connectors/dbt/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dbt/sync")
@require_login
def dbt_sync_proxy():
    r = connector_sync("dbt")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/dbt")
@require_login
def dbt_status_proxy():
    r = connector_status("dbt")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dbt/job/get")
@require_login
def dbt_job_get_proxy():
    r = connector_job_get("dbt")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/dbt/job/save", methods=["POST"])
@require_login
def dbt_job_save_proxy():
    r = connector_job_save("dbt")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dbt/save_app", methods=["POST"])
@require_login
def dbt_save_app_proxy():
    r = proxy_post("/connectors/dbt/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/dbt/disconnect")
@require_login
def dbt_disconnect_proxy():
    r = proxy_get("/connectors/dbt/disconnect")
    return safe_backend_json_response(r, include_status=True)


# ================= TYPEFORM ========================

@app.route("/connectors/typeform")
@require_login
def typeform_page():
    return render_ui_template("connectors/typeform.html")


@app.route("/connectors/typeform/connect")
@require_login
def typeform_connect_proxy():
    r = proxy_get("/connectors/typeform/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/typeform/sync")
@require_login
def typeform_sync_proxy():
    r = connector_sync("typeform")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/typeform")
@require_login
def typeform_status_proxy():
    r = connector_status("typeform")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/typeform/job/get")
@require_login
def typeform_job_get_proxy():
    r = connector_job_get("typeform")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/typeform/job/save", methods=["POST"])
@require_login
def typeform_job_save_proxy():
    r = connector_job_save("typeform")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/typeform/save_app", methods=["POST"])
@require_login
def typeform_save_app_proxy():
    r = proxy_post("/connectors/typeform/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/typeform/disconnect")
@require_login
def typeform_disconnect_proxy():
    r = proxy_get("/connectors/typeform/disconnect")
    return safe_backend_json_response(r, include_status=True)


# ================= SURVEYMONKEY ========================

@app.route("/connectors/surveymonkey")
@require_login
def surveymonkey_page():
    return render_ui_template("connectors/surveymonkey.html")


@app.route("/connectors/surveymonkey/connect")
@require_login
def surveymonkey_connect_proxy():
    r = proxy_get("/connectors/surveymonkey/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/surveymonkey/sync")
@require_login
def surveymonkey_sync_proxy():
    r = connector_sync("surveymonkey")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/surveymonkey")
@require_login
def surveymonkey_status_proxy():
    r = connector_status("surveymonkey")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/surveymonkey/job/get")
@require_login
def surveymonkey_job_get_proxy():
    r = connector_job_get("surveymonkey")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/surveymonkey/job/save", methods=["POST"])
@require_login
def surveymonkey_job_save_proxy():
    r = connector_job_save("surveymonkey")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/surveymonkey/save_app", methods=["POST"])
@require_login
def surveymonkey_save_app_proxy():
    r = proxy_post("/connectors/surveymonkey/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/surveymonkey/disconnect")
@require_login
def surveymonkey_disconnect_proxy():
    r = proxy_get("/connectors/surveymonkey/disconnect")
    return safe_backend_json_response(r, include_status=True)


# ================= PINECONE ========================

@app.route("/connectors/pinecone")
@require_login
def pinecone_page():
    return render_ui_template("connectors/pinecone.html")


@app.route("/connectors/pinecone/connect")
@require_login
def pinecone_connect_proxy():
    r = proxy_get("/connectors/pinecone/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pinecone/sync")
@require_login
def pinecone_sync_proxy():
    r = connector_sync("pinecone")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/pinecone")
@require_login
def pinecone_status_proxy():
    r = connector_status("pinecone")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pinecone/job/get")
@require_login
def pinecone_job_get_proxy():
    r = connector_job_get("pinecone")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/pinecone/job/save", methods=["POST"])
@require_login
def pinecone_job_save_proxy():
    r = connector_job_save("pinecone")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pinecone/save_app", methods=["POST"])
@require_login
def pinecone_save_app_proxy():
    r = proxy_post("/connectors/pinecone/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/pinecone/disconnect")
@require_login
def pinecone_disconnect_proxy():
    r = proxy_get("/connectors/pinecone/disconnect")
    return safe_backend_json_response(r, include_status=True)

# ================= NETLIFY ========================

@app.route("/connectors/netlify")
@require_login
def netlify_page():
    return render_ui_template("connectors/netlify.html")


@app.route("/connectors/netlify/connect")
@require_login
def netlify_connect_proxy():
    r = proxy_get("/connectors/netlify/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/netlify/sync")
@require_login
def netlify_sync_proxy():
    r = connector_sync("netlify")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/netlify")
@require_login
def netlify_status_proxy():
    r = connector_status("netlify")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/netlify/job/get")
@require_login
def netlify_job_get_proxy():
    r = connector_job_get("netlify")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/netlify/job/save", methods=["POST"])
@require_login
def netlify_job_save_proxy():
    r = connector_job_save("netlify")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/netlify/save_app", methods=["POST"])
@require_login
def netlify_save_app_proxy():
    r = proxy_post("/connectors/netlify/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/netlify/disconnect")
@require_login
def netlify_disconnect_proxy():
    r = proxy_get("/connectors/netlify/disconnect")
    return safe_backend_json_response(r, include_status=True)


# ================= LINEAR ========================

@app.route("/connectors/linear")
@require_login
def linear_page():
    return render_ui_template("connectors/linear.html")


@app.route("/connectors/linear/connect")
@require_login
def linear_connect_proxy():
    r = proxy_get("/connectors/linear/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/linear/sync")
@require_login
def linear_sync_proxy():
    r = connector_sync("linear")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/linear")
@require_login
def linear_status_proxy():
    r = connector_status("linear")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/linear/job/get")
@require_login
def linear_job_get_proxy():
    r = connector_job_get("linear")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/linear/job/save", methods=["POST"])
@require_login
def linear_job_save_proxy():
    r = connector_job_save("linear")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/linear/save_app", methods=["POST"])
@require_login
def linear_save_app_proxy():
    r = proxy_post("/connectors/linear/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/linear/disconnect")
@require_login
def linear_disconnect_proxy():
    r = proxy_get("/connectors/linear/disconnect")
    return safe_backend_json_response(r, include_status=True)

# ================= BITBUCKET ========================

@app.route("/connectors/bitbucket")
@require_login
def bitbucket_page():
    return render_ui_template("connectors/bitbucket.html")


@app.route("/connectors/bitbucket/connect")
@require_login
def bitbucket_connect_proxy():
    r = proxy_get("/connectors/bitbucket/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bitbucket/sync")
@require_login
def bitbucket_sync_proxy():
    r = connector_sync("bitbucket")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/bitbucket")
@require_login
def bitbucket_status_proxy():
    r = connector_status("bitbucket")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bitbucket/job/get")
@require_login
def bitbucket_job_get_proxy():
    r = connector_job_get("bitbucket")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/bitbucket/job/save", methods=["POST"])
@require_login
def bitbucket_job_save_proxy():
    r = connector_job_save("bitbucket")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bitbucket/save_app", methods=["POST"])
@require_login
def bitbucket_save_app_proxy():
    r = proxy_post("/connectors/bitbucket/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/bitbucket/disconnect")
@require_login
def bitbucket_disconnect_proxy():
    r = proxy_get("/connectors/bitbucket/disconnect")
    return safe_backend_json_response(r, include_status=True)

# ================= VERCEL ========================

@app.route("/connectors/vercel")
@require_login
def vercel_page():
    return render_ui_template("connectors/vercel.html")


@app.route("/connectors/vercel/connect")
@require_login
def vercel_connect_proxy():
    r = proxy_get("/connectors/vercel/connect")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/vercel/sync")
@require_login
def vercel_sync_proxy():
    r = connector_sync("vercel")
    return safe_backend_json_response(r, include_status=True)


@app.route("/api/status/vercel")
@require_login
def vercel_status_proxy():
    r = connector_status("vercel")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/vercel/job/get")
@require_login
def vercel_job_get_proxy():
    r = connector_job_get("vercel")
    try:
        return safe_backend_json_response(r, include_status=True)
    except Exception:
        return jsonify({"exists": False, "sync_type": "incremental", "schedule_time": None}), 200


@app.route("/connectors/vercel/job/save", methods=["POST"])
@require_login
def vercel_job_save_proxy():
    r = connector_job_save("vercel")
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/vercel/save_app", methods=["POST"])
@require_login
def vercel_save_app_proxy():
    r = proxy_post("/connectors/vercel/save_app", json=request.get_json())
    return safe_backend_json_response(r, include_status=True)


@app.route("/connectors/vercel/disconnect")
@require_login
def vercel_disconnect_proxy():
    r = proxy_get("/connectors/vercel/disconnect")
    return safe_backend_json_response(r, include_status=True)

# ================= AI COMPANION ==========================

@app.route("/ai/chats", methods=["GET"])
@require_login
def ai_chats():
    r = proxy_get("/ai/chats")
    return safe_backend_json_response(r, include_status=True)

@app.route("/ai/chat/<chat_id>", methods=["GET"])
@require_login
def ai_chat_history(chat_id):
    r = proxy_get(f"/ai/chat/{chat_id}")
    return safe_backend_json_response(r, include_status=True)

@app.route("/ai/chat", methods=["POST"])
@require_login
def ai_chat_message():
    r = proxy_post("/ai/chat", json=request.get_json(silent=True) or {})
    return safe_backend_json_response(r, include_status=True)

# ================= MAIN ==========================
if __name__ == "__main__":
    app.run(port=3000, debug=True)
