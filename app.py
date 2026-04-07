from werkzeug.middleware.dispatcher import DispatcherMiddleware
from frontend.ui_server import app as frontend_app
from backend.api_server import app as backend_app

application = DispatcherMiddleware(frontend_app, {
    '/_backend': backend_app
})

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    run_simple('0.0.0.0', 7860, application, threaded=True)
