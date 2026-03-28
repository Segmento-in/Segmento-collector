from flask import current_app

def call_connector_route(path, uid, method="GET", json_data=None):
    try:
        with current_app.test_client() as client:

            if method == "POST":
                res = client.post(
                    path,
                    json=json_data or {},
                    headers={"X-Internal-UID": uid},
                    follow_redirects=False
                )
            else:
                res = client.get(
                    path,
                    headers={"X-Internal-UID": uid},
                    follow_redirects=False
                )

            data = res.get_json(silent=True) or {}

            # Strict success: only standard OK codes
            ok = res.status_code in (200, 201)

            connected = isinstance(data, dict) and data.get("connected") is True

            return {
                "ok": ok,
                "connected": connected,
                "http_status": res.status_code,
                "data": data
            }
            
    except Exception as e:
        return {"ok": False, "error": str(e)}
