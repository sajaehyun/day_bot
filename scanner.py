import os, json, logging, pathlib, numpy as np, pandas as pd, requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scanner")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
KST = timezone(timedelta(hours=9))
RESULT_DIR = pathlib.Path("results")
RESULT_DIR.mkdir(exist_ok=True)
try:
    import yfinance as yf; _YF = True
except:
    _YF = False
SP500 = ["AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","JPM","V","UNH","MA","JNJ","PG","HD","ABBV","MRK","WMT","NFLX","CRM","AMD","KO","PEP","TMO","ACN","MCD","ADBE","LIN","CSCO","TXN","NEE","AMGN","RTX","HON","GE","CAT","ISRG","BLK","VRTX","GILD","NOW","PANW","COIN","PLTR","ARM","SMCI","DELL","MU","QCOM","COST","AVGO"]
SOX = ["AMD","INTC","MU","AMAT","LRCX","KLAC","MRVL","ADI","NXPI","ON","SWKS","MCHP","ARM","MPWR","TER","ENTG","SNPS","CDNS","COHR","RMBS"]
ALL = list(dict.fromkeys(SP500 + SOX))
def safe_float(v, d=0.0):
    try:
        if v is None: return d
        if isinstance(v, (pd.Series, pd.DataFrame)):
            v = v.squeeze()
            if isinstance(v, pd.Series): v = v.iloc[-1] if len(v) else d
        f = float(v)
        return d if (np.isnan(f) or np.isinf(f)) else f
    except: return d
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id":CHAT_ID,"text":msg[:4000],"parse_mode":"HTML"}, timeout=10)
    except: pass
def analyze_next_day(ticker):
    if not _YF: return None
    try:
        t = yf.Ticker(ticker)
        df = t.history(period="3mo", interval="1d", auto_adjust=True)
        if df is None or len(df) < 30: return None
        c, h, l, v = df["Close"].astype(float), df["High"].astype(float), df["Low"].astype(float), df["Volume"].astype(float)
        price = round(float(c.iloc[-1]), 2)
        prev = float(c.iloc[-2]) if len(c) >= 2 else price
        chg = round((price - prev) / prev * 100, 2) if prev else 0
        raw = 0; signals = []
        delta = c.diff()
        gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
        loss = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
        lg, ll = safe_float(gain.iloc[-1]), safe_float(loss.iloc[-1])
        rsi = round(100 - 100/(1+lg/ll), 1) if ll > 0 else (100.0 if lg > 0 else 50.0)
        if 25 <= rsi < 30: raw += 20; signals.append(f"RSI ���ŵ��ݵ�({rsi})")
        elif 30 <= rsi <= 40: raw += 15; signals.append(f"RSI �ݵ��({rsi})")
        elif rsi > 75: raw -= 15; signals.append(f"RSI ����({rsi})")
        if len(c) >= 35:
            macd = c.ewm(span=12).mean() - c.ewm(span=26).mean()
            sig = macd.ewm(span=9).mean()
            hist = macd - sig
            if len(hist) >= 3:
                h1,h2,h3 = float(hist.iloc[-3]),float(hist.iloc[-2]),float(hist.iloc[-1])
                if h2 <= 0 and h3 > 0: raw += 20; signals.append("MACD ���ũ�ν�")
                elif h1 < h2 < 0 and h3 > h2: raw += 12; signals.append("MACD ������")
                elif h2 >= 0 and h3 < 0: raw -= 15; signals.append("MACD ����ũ�ν�")
        vol_ratio = 1.0
        if len(v) >= 21:
            avg_v = float(v.rolling(20).mean().iloc[-1])
            vol_ratio = round(float(v.iloc[-1])/avg_v, 2) if avg_v > 0 else 1.0
            if vol_ratio >= 2.0 and abs(chg) < 2: raw += 18; signals.append(f"�ŷ�������({vol_ratio}x)")
            elif vol_ratio >= 1.5: raw += 8; signals.append(f"�ŷ�������({vol_ratio}x)")
        ma5 = round(float(c.rolling(5).mean().iloc[-1]), 2) if len(c) >= 5 else price
        ma20 = round(float(c.rolling(20).mean().iloc[-1]), 2) if len(c) >= 20 else price
        if price > ma5 > ma20: raw += 12; signals.append("���迭")
        elif price < ma5 < ma20: raw -= 10; signals.append("���迭")
        if len(c) >= 20:
            bm = float(c.rolling(20).mean().iloc[-1])
            bs = float(c.rolling(20).std().iloc[-1])
            if price <= bm - 2*bs: raw += 12; signals.append("�������ϴ� �ݵ���")
        if len(c) >= 5:
            recent = c.tail(5).pct_change().dropna()
            dd = sum(1 for x in recent if x < 0)
            if dd >= 3 and chg > 0: raw += 15; signals.append(f"{dd}���϶��Ĺݵ�")
            elif dd >= 4: raw += 8; signals.append(f"{dd}�Ͽ����϶�")
        score = max(0, min(100, raw))
        if score >= 70: grade, gk = "���¸ż�", "strong_buy"
        elif score >= 55: grade, gk = "�ż�", "buy"
        elif score >= 40: grade, gk = "����", "watch"
        elif score >= 25: grade, gk = "�߸�", "neutral"
        else: grade, gk = "ȸ��", "avoid"
        return {"ticker":ticker,"price":price,"change_1d":chg,"score":score,"grade":grade,"grade_key":gk,"rsi":rsi,"volume_ratio":vol_ratio,"ma5":ma5,"ma20":ma20,"signals":signals}
    except Exception as e:
        log.error("[%s] %s", ticker, e); return None
def scan_all():
    at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S (KST)")
    log.info("=== scan start %d ===", len(ALL))
    results = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        fs = {ex.submit(analyze_next_day, s): s for s in ALL}
        done = 0
        for f in as_completed(fs):
            done += 1
            if done % 10 == 0: log.info("progress %d/%d", done, len(ALL))
            try:
                r = f.result()
                if r and r["score"] >= 20: results.append(r)
            except: pass
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:20]
    sb = sum(1 for r in results if r["grade_key"]=="strong_buy")
    bu = sum(1 for r in results if r["grade_key"]=="buy")
    wa = sum(1 for r in results if r["grade_key"]=="watch")
    data = {"analyzed_at":at,"total_scanned":len(ALL),"total_passed":len(results),"strong_buy":sb,"buy":bu,"watch":wa,"results":top}
    ts = datetime.now(KST).strftime("%Y-%m-%d_%H%M%S")
    (RESULT_DIR / f"{ts}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    lines = [f"<b>���ϻ�¿���</b> {at}",""]
    for i,r in enumerate(top[:10],1):
        sig = " | ".join(r["signals"][:3])
        lines.append(f'{i}. <b>{r["ticker"]}</b> {r["grade"]} {r["score"]}�� | \ ({r["change_1d"]:+.1f}%)\n  {sig}')
    send_telegram("\n".join(lines))
    log.info("=== done: %d sb:%d bu:%d wa:%d ===", len(results), sb, bu, wa)
    return data
if __name__ == "__main__":
    scan_all()
