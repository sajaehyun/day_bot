import os, json, pathlib, threading, secrets, logging
from flask import Flask, render_template, request, session, jsonify, abort
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))

from surge_app import surge_bp, init_surge_scheduler
app.register_blueprint(surge_bp)
init_surge_scheduler()

RESULT_DIR = pathlib.Path("results")
RESULT_DIR.mkdir(exist_ok=True)
_analysing = False

def _generate_csrf():
    if "_csrf" not in session: session["_csrf"] = secrets.token_hex(32)
    return session["_csrf"]

@app.context_processor
def inject_csrf():
    return {"csrf_token": _generate_csrf}

def _validate_csrf():
    token = request.headers.get("X-CSRF-Token") or request.form.get("csrf_token")
    if not token or token != session.get("_csrf"): abort(403)

def _load_latest():
    files = sorted(RESULT_DIR.glob("*.json"))
    if not files: return None
    return json.loads(files[-1].read_text(encoding="utf-8"))

@app.route("/")
def index():
    data = _load_latest()
    return render_template("dashboard.html", data=data, analyzing=_analysing)

@app.route("/refresh", methods=["POST"])
def refresh():
    global _analysing
    _validate_csrf()
    if _analysing: return jsonify(status="already_running")
    _analysing = True
    def _run():
        global _analysing
        try:
            from scanner import scan_all
            scan_all()
        finally:
            _analysing = False
    threading.Thread(target=_run, daemon=True).start()
    return jsonify(status="started")

@app.route("/status")
def status():
    return jsonify(analyzing=_analysing)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
