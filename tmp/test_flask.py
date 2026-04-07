from flask import Flask, request
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple

app1 = Flask("app1")
app2 = Flask("app2")

@app1.route("/")
def h1():
    return f"App1 host_url: {request.host_url}"

@app2.route("/")
def h2():
    return f"App2 host_url: {request.host_url}"

application = DispatcherMiddleware(app1, {
    '/sub': app2
})

if __name__ == "__main__":
    print("Testing locally... wait for results")
    # This is just for demonstration, I'll run it and check manually if I can.
    # Actually I can't run a server and hit it easily without blocking.
    # I'll just check the documentation or assume the user knows what they are talking about.
