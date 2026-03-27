import yfinance as yf
import requests
import logging
import time
from datetime import datetime
import pytz

log = logging.getLogger(__name__)

SOXL_SYMBOLS = [
    "MU", "AMAT", "NVDA", "AMD", "AVGO", "LRCX", "KLAC",
    "ASML", "TER", "ADI", "TXN", "MPWR", "TSM", "INTC",
    "MRVL", "NXPI", "MCHP", "QCOM", "SWKS", "ENTG",
    "ONTO", "WOLF", "OLED", "COHU", "ACLS"
]

SP500_FALLBACK = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","BRK-B","AVGO",
    "JPM","LLY","V","MA","UNH","XOM","COST","HD","PG","WMT","JNJ","ABBV",
    "BAC","NFLX","CRM","MRK","CVX","ORCL","ACN","AMD","TMO","ABT","KO",
    "PEP","CSCO","LIN","MCD","TXN","PM","INTC","ADBE","WFC","IBM","GE",
    "DHR","CAT","INTU","SPGI","AXP","RTX","NOW","ISRG","GS","BLK","AMGN",
    "ELV","VRTX","SYK","DE","ADI","REGN","MMC","GILD","ZTS","PANW","LRCX",
    "BSX","MU","PLD","CI","CB","AMAT","KLAC","MDLZ","PGR","ITW","TJX",
    "SNPS","EOG","CDNS","AON","CMG","MO","SHW","MRVL","NXPI","FTNT","APH",
    "CTAS","MCO","ICE","NOC","DUK","PSA","SO","HUM","WM","EMR","GD","NSC",
    "FCX","NKE","F","GM","USB","TFC","WELL","EW","ROP","IDXX","DXCM",
    "GEHC","MPWR","SBUX","STZ","DOW","DD","CARR","CCI","CTVA","BIIB","OKE"
]

SESSION_LABEL = {
    "night":  {"change": 1.0, "vol": 0.3},   # 야간: 조건 대폭 완화    
    "pre":    "🌅 프리마켓 (ET 04:00~09:30)",
    "day":    "📈 데이마켓 (ET 09:30~16:00)",
    "after":  "🌙 애프터마켓 (ET 16:00~20:00)",
    "closed": "🔒 장 마감 (ET 20:00~04:00)",
}

SESSION_FILTER = {
    "night":  {"change": 1.5, "vol": 1.5},
    "pre":    {"change": 2.0, "vol": 1.5},
    "day":    {"change": 1.5, "vol": 1.5},
    "after":  {"change": 1.5, "vol": 1.3},
    "closed": {"change": 2.0, "vol": 1.5},
}


def get_market_session():
    et = pytz.timezone("America/New_York")
    now = datetime.now(et)
    hour = now.hour + now.minute / 60
    if 0 <= hour < 4:
        return "night"
    elif 4 <= hour < 9.5:
        return "pre"
    elif 9.5 <= hour < 16:
        return "day"
    elif 16 <= hour < 20:
        return "after"
    else:
        return "closed"


def get_sp500_symbols():
    try:
        import pandas as pd
        table = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        symbols = [s.replace(".", "-") for s in table[0]["Symbol"].tolist()]
        log.info(f"S&P 500 수집 완료: {len(symbols)}개")
        return symbols
    except Exception as e:
        log.warning(f"S&P 500 수집 실패, fallback 사용: {e}")
        return SP500_FALLBACK

def get_universe():
    sp500 = get_sp500_symbols()
    combined = list(set(sp500 + SOXL_SYMBOLS))
    log.info(f"전체 유니버스: {len(combined)}개")
    return combined

def get_market_data(symbol, session):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")
        if not prev_close or prev_close == 0:
            return None

        # 실시간 현재가
        cur_price = info.get("regularMarketPrice") or info.get("currentPrice")
        # 프리/야간/애프터는 해당 가격도 체크
        if session in ("pre", "night"):
            cur_price = info.get("preMarketPrice") or cur_price
        elif session == "after":
            cur_price = info.get("postMarketPrice") or cur_price

        if not cur_price:
            return None

        change_pct = (cur_price - prev_close) / prev_close * 100

        # 오늘 실시간 거래량: 1분봉으로 오늘치 합산
        hist_1d = ticker.history(period="2d", interval="1m")
        hist_20d = ticker.history(period="1mo")

        if hist_20d.empty:
            return None

        avg_volume = hist_20d["Volume"].mean()

        # 오늘 거래량 합산
        if not hist_1d.empty:
            today_str = datetime.now(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
            today_vol = hist_1d[hist_1d.index.strftime("%Y-%m-%d") == today_str]["Volume"].sum()
        else:
            today_vol = hist_20d["Volume"].iloc[-1]

        vol_ratio = today_vol / avg_volume if avg_volume > 0 else 0

        target = info.get("targetMeanPrice")
        upside = ((target - cur_price) / cur_price * 100) if target else None

        news_list = ticker.news or []
        latest_news = ""
        if news_list:
            first = news_list[0]
            latest_news = (
                first.get("content", {}).get("title", "")
                or first.get("title", "")
            )

        return {
            "symbol": symbol,
            "name": info.get("shortName", symbol),
            "cur_price": round(cur_price, 2),
            "prev_close": round(prev_close, 2),
            "change_pct": round(change_pct, 2),
            "cur_volume": int(today_vol),
            "avg_volume": int(avg_volume),
            "vol_ratio": round(vol_ratio, 2),
            "target_price": round(target, 2) if target else None,
            "upside": round(upside, 1) if upside else None,
            "latest_news": latest_news,
            "market_cap": info.get("marketCap"),
            "sector": info.get("sector", ""),
            "session": session,
        }
    except Exception as e:
        log.debug(f"{symbol} 실패: {e}")
        return None

def compute_surge_score(data):
    score = 0
    cp = data["change_pct"]
    vr = data["vol_ratio"]
    up = data.get("upside") or 0
    if cp >= 10:    score += 50
    elif cp >= 7:   score += 40
    elif cp >= 5:   score += 30
    elif cp >= 3:   score += 20
    elif cp >= 2:   score += 10
    if vr >= 5:     score += 30
    elif vr >= 3:   score += 20
    elif vr >= 2:   score += 10
    elif vr >= 1.3: score += 5
    if up >= 30:    score += 20
    elif up >= 15:  score += 10
    elif up >= 5:   score += 5
    return score

def send_telegram_surge_alert(results, token, chat_id, session="day"):
    label = SESSION_LABEL.get(session, "")
    if not results:
        msg = f"🔍 [{label}] 폭등 종목 없음"
    else:
        et_now = datetime.now(pytz.timezone("America/New_York"))
        lines = [f"🚀 폭등 알림 [{label}]\n{et_now.strftime('%m/%d %H:%M ET')}\n"]
        for i, r in enumerate(results[:10], 1):
            upside_str = f"+{r['upside']}%" if r.get("upside") else "N/A"
            lines.append(
                f"{i}. {r['symbol']} +{r['change_pct']}% | 거래량 {r['vol_ratio']}배\n"
                f"   💬 {r['latest_news'][:60] if r['latest_news'] else '뉴스 없음'}\n"
                f"   🎯 업사이드 {upside_str} | 점수 {r['score']}\n"
            )
        lines.append(f"\n총 {len(results)}종목 감지")
        msg = "\n".join(lines)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)

def run_surge_scan(session=None):
    if session is None:
        session = get_market_session()
    filters = SESSION_FILTER[session]
    symbols = get_universe()
    results = []
    total = len(symbols)
    log.info(f"[{SESSION_LABEL[session]}] 스캔 시작: {total}개")
    for i, sym in enumerate(symbols):
        data = get_market_data(sym, session)
        if data and data["change_pct"] >= filters["change"] and data["vol_ratio"] >= filters["vol"]:
            data["score"] = compute_surge_score(data)
            results.append(data)
            log.info(f"🚀 {sym} +{data['change_pct']}% 거래량{data['vol_ratio']}배 점수{data['score']}")
        if (i + 1) % 50 == 0:
            log.info(f"진행: {i+1}/{total}")
        time.sleep(0.05)
    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"스캔 완료: {len(results)}개 감지")
    return results[:20], session
