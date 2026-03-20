from backend.api_server import app

app.run(
    host="0.0.0.0",
    port=4000,
    debug=True,
    use_reloader=False
)