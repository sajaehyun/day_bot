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

_surge_cache = {
    "results": [],
    "last_updated": None,
    "status": "idle",
    "session": "closed",
    "session_label": "🔒 장 마감 (ET 20:00~24:00)"
}
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

@surge_bp.route("/surge/refresh", methods=["POST"])
def surge_refresh():
    session = request.json.get("session") if request.json else None
    threading.Thread(target=_run_and_cache, args=(session,), daemon=True).start()
    return jsonify({"status": "started"})

@surge_bp.route("/surge/status")
def surge_status():
    with _lock:
        return jsonify(_surge_cache)

@surge_bp.route("/surge")
def surge_page():
    with _lock:
        data = _surge_cache.copy()
    return render_template(
        "surge.html",
        results=data["results"],
        last_updated=data["last_updated"],
        session_label=data["session_label"],
        current_session=data["session_label"]
    )
def init_surge_scheduler():
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")

    scheduler.add_job(_run_and_cache, "cron",
                      hour=17, minute=0,
                      day_of_week="mon-fri",
                      id="surge_pre", args=["pre"])

    scheduler.add_job(_run_and_cache, "cron",
                      hour=22, minute=35,
                      day_of_week="mon-fri",
                      id="surge_day_open", args=["day"])

    scheduler.add_job(_run_and_cache, "cron",
                      hour=5, minute=5,
                      day_of_week="tue-sat",
                      id="surge_after", args=["after"])

    scheduler.add_job(_run_and_cache, "cron",
                      hour=10, minute=0,
                      day_of_week="mon-fri",
                      id="surge_daytrade", args=["daytrade"])

    scheduler.start()
    log.info("Surge 스케줄러 시작 (KST 기준)")
    return scheduler

