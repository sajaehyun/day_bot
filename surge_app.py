import os
import threading
import logging
from datetime import datetime
import pytz
from flask import Blueprint, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler
from surge_scanner import run_surge_scan, send_telegram_surge_alert, SESSION_LABEL, get_market_session

log = logging.getLogger(__name__)

surge_bp = Blueprint("surge", __name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_surge_cache = {"results": [], "last_updated": None, "status": "idle", "session": "closed", "session_label": ""}
_lock = threading.Lock()

def _run_and_cache(session=None):
    with _lock:
        _surge_cache["status"] = "running"
    try:
        results, sess = run_surge_scan(session)
        et_now = datetime.now(pytz.timezone("America/New_York"))
        with _lock:
            _surge_cache["results"] = results
            _surge_cache["last_updated"] = et_now.strftime("%Y-%m-%d %H:%M ET")
            _surge_cache["status"] = "done"
            _surge_cache["session"] = sess
            _surge_cache["session_label"] = SESSION_LABEL.get(sess, "")
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            send_telegram_surge_alert(results, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sess)
            log.info("텔레그램 발송 완료")
    except Exception as e:
        log.error(f"surge 스캔 오류: {e}")
        with _lock:
            _surge_cache["status"] = "error"

@surge_bp.route("/surge")
def surge_dashboard():
    with _lock:
        data = dict(_surge_cache)
    data["current_session"] = SESSION_LABEL.get(get_market_session(), "")
    return render_template("surge_dashboard.html", **data)

@surge_bp.route("/surge/refresh", methods=["POST"])
def surge_refresh():
    session = request.json.get("session") if request.json else None
    threading.Thread(target=_run_and_cache, args=(session,), daemon=True).start()
    return jsonify({"status": "started"})

@surge_bp.route("/surge/status")
def surge_status():
    with _lock:
        return jsonify(_surge_cache)

def init_surge_scheduler():
    scheduler = BackgroundScheduler(timezone="America/New_York")
    scheduler.add_job(_run_and_cache, "cron", hour=7, minute=0,
                      id="surge_pre", args=["pre"])
    scheduler.add_job(_run_and_cache, "cron", hour=9, minute=35,
                      id="surge_day_open", args=["day"])
    scheduler.add_job(_run_and_cache, "cron", hour=13, minute=0,
                      id="surge_day_mid", args=["day"])
    scheduler.add_job(_run_and_cache, "cron", hour=16, minute=5,
                      id="surge_after", args=["after"])
    scheduler.start()
    log.info("Surge 스케줄러 시작: 프리/데이/애프터 자동 실행")
    return scheduler
