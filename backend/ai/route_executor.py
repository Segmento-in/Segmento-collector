from flask import current_app

def call_connector_route(path, uid, method="GET", json_data=None):
    try:
        with current_app.test_client() as client:

            if method == "POST":
                res = client.post(
                    path,
                    json=json_data or {},
                    headers={"X-Internal-UID": uid},
                    follow_redirects=True
                )
            else:
                res = client.get(
                    path,
                    headers={"X-Internal-UID": uid},
                    follow_redirects=True
                )

            data = res.get_json(silent=True) or {}

            # Treat more cases as success
            success = res.status_code in (200, 201, 302)

            # Also treat known "non-failure" responses as success
            if isinstance(data, dict):
                if data.get("status") in ["success", "ok", "already_connected"]:
                    success = True
                if "redirect" in data or "auth" in str(data).lower():
                    success = True

            return {
                "ok": success,
                "status": res.status_code,
                "data": data
            }
            
    except Exception as e:
        return {"ok": False, "error": str(e)}