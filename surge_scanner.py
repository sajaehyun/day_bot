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
    "pre":      "🌅 프리마켓 (KST 17:00~22:30)",
    "day":      "📈 정규장 (KST 22:30~05:00)",
    "after":    "🌙 애프터마켓 (KST 05:00~06:00)",
    "daytrade": "📊 주간거래 (KST 10:00~18:00)",
    "closed":   "🔒 장 마감",
}

SESSION_FILTER = {
    "pre":      {"change": 1.0, "vol": 0.3},
    "day":      {"change": 1.5, "vol": 0.5},
    "after":    {"change": 1.0, "vol": 0.3},
    "daytrade": {"change": 0.5, "vol": 0.1},  # 주간거래는 필터 최대한 완화
    "closed":   {"change": 1.0, "vol": 0.1},
}

def get_market_session():
    kst = pytz.timezone("Asia/Seoul")
    now = datetime.now(kst)
    hour = now.hour + now.minute / 60

    et = pytz.timezone("America/New_York")
    is_dst = bool(datetime.now(et).dst())

    if is_dst:
        if 17.0 <= hour < 22.5:         return "pre"
        if hour >= 22.5 or hour < 5.0:  return "day"
        if 5.0 <= hour < 6.0:           return "after"
        if 10.0 <= hour < 18.0:         return "daytrade"
        return "closed"
    else:
        if 18.0 <= hour < 23.5:         return "pre"
        if hour >= 23.5 or hour < 6.0:  return "day"
        if 6.0 <= hour < 7.0:           return "after"
        if 10.0 <= hour < 18.0:         return "daytrade"
        return "closed"

def get_sp500_symbols():
    try:
        import pandas as pd
        headers = {"User-Agent": "Mozilla/5.0"}
        table = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            storage_options={"headers": headers}
        )
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
        info = ticker.fast_info

        # 전일 종가
        prev_close = getattr(info, "previous_close", None) or \
                     ticker.info.get("regularMarketPreviousClose")
        if not prev_close or prev_close == 0:
            return None

        # 현재가: 세션별 분기
        full_info = ticker.info
        if session == "day":
            cur_price = (full_info.get("regularMarketPrice")
                         or full_info.get("currentPrice"))
        elif session in ("pre",):
            cur_price = (full_info.get("preMarketPrice")
                         or full_info.get("regularMarketPrice"))
        elif session == "after":
            cur_price = (full_info.get("postMarketPrice")
                         or full_info.get("regularMarketPrice"))
        elif session == "daytrade":
            # 주간거래: 애프터 가격 우선, 없으면 정규장 가격
            cur_price = (full_info.get("postMarketPrice")
                         or full_info.get("regularMarketPrice")
                         or full_info.get("currentPrice"))
        else:
            # closed
            cur_price = (full_info.get("regularMarketPrice")
                         or full_info.get("currentPrice"))

        if not cur_price:
            return None

        change_pct = (cur_price - prev_close) / prev_close * 100

        # 거래량
        hist_1d  = ticker.history(period="2d", interval="1m")
        hist_20d = ticker.history(period="1mo")

        if hist_20d.empty:
            return None

        avg_volume = hist_20d["Volume"].mean()

        et = pytz.timezone("America/New_York")
        now_et = datetime.now(et)
        today_str = now_et.strftime("%Y-%m-%d")

        if not hist_1d.empty:
            today_mask = hist_1d.index.strftime("%Y-%m-%d") == today_str

            # ✅ 오늘 데이터 없으면 가장 최근 거래일 사용 (주말/공휴일 대비)
            if today_mask.sum() == 0:
                latest_date = hist_1d.index.strftime("%Y-%m-%d").max()
                today_mask = hist_1d.index.strftime("%Y-%m-%d") == latest_date
                log.debug(f"{symbol} 오늘 데이터 없음, 최근 거래일 {latest_date} 사용")

            today_vol = hist_1d[today_mask]["Volume"].sum()
        else:
            today_vol = int(avg_volume)

        # 세션별 거래량 비율 계산
        if session == "day":
            # 정규장: 경과 시간 비율로 하루치 환산
            market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            elapsed_min = max(1, (now_et - market_open).total_seconds() / 60)
            elapsed_ratio = min(elapsed_min / 390, 1.0)
            projected_vol = today_vol / elapsed_ratio if elapsed_ratio > 0 else today_vol
            vol_ratio = projected_vol / avg_volume if avg_volume > 0 else 0
        elif session == "pre":
            # 프리마켓: 경과 시간 비율로 환산
            pre_open  = now_et.replace(hour=4, minute=0, second=0, microsecond=0)
            elapsed   = max(1, (now_et - pre_open).total_seconds() / 60)
            ratio     = min(elapsed / 330, 1.0)
            projected_vol = today_vol / ratio if ratio > 0 else today_vol
            vol_ratio = projected_vol / avg_volume if avg_volume > 0 else 0
        else:
            # daytrade, after, closed: 그냥 당일 거래량 / 평균
            vol_ratio = today_vol / avg_volume if avg_volume > 0 else 0

        vol_ratio = round(vol_ratio, 2)

        target = full_info.get("targetMeanPrice")
        upside = ((target - cur_price) / cur_price * 100) if target else None

        news_list   = ticker.news or []
        latest_news = ""
        if news_list:
            first       = news_list[0]
            latest_news = (first.get("content", {}).get("title", "")
                           or first.get("title", ""))

        return {
            "symbol":       symbol,
            "name":         full_info.get("shortName", symbol),
            "cur_price":    round(cur_price, 2),
            "prev_close":   round(prev_close, 2),
            "change_pct":   round(change_pct, 2),
            "cur_volume":   int(today_vol),
            "avg_volume":   int(avg_volume),
            "vol_ratio":    vol_ratio,
            "target_price": round(target, 2) if target else None,
            "upside":       round(upside, 1) if upside else None,
            "latest_news":  latest_news,
            "market_cap":   full_info.get("marketCap"),
            "sector":       full_info.get("sector", ""),
            "session":      session,
        }
    except Exception as e:
        log.debug(f"{symbol} 실패: {e}")
        return None

def compute_surge_score(data):
    score = 0
    cp = abs(data["change_pct"])
    vr = data["vol_ratio"]
    up = data.get("upside") or 0
    if cp >= 10:    score += 50
    elif cp >= 7:   score += 40
    elif cp >= 5:   score += 30
    elif cp >= 3:   score += 20
    elif cp >= 2:   score += 10
    elif cp >= 1:   score += 5
    if vr >= 5:     score += 30
    elif vr >= 3:   score += 20
    elif vr >= 2:   score += 10
    elif vr >= 1.3: score += 5
    elif vr >= 0.5: score += 2
    if up >= 30:    score += 20
    elif up >= 15:  score += 10
    elif up >= 5:   score += 5
    return score

def send_telegram_surge_alert(results, token, chat_id, session="day"):
    label = SESSION_LABEL.get(session, "")
    if not results:
        msg = f"🔍 [{label}] 급등 종목 없음"
    else:
        et_now = datetime.now(pytz.timezone("America/New_York"))
        lines = [f"🚀 급등 알림 [{label}]\n{et_now.strftime('%m/%d %H:%M ET')}\n"]
        for i, r in enumerate(results[:10], 1):
            upside_str = f"+{r['upside']}%" if r.get("upside") else "N/A"
            lines.append(
                f"{i}. {r['symbol']} {r['change_pct']:+.2f}% | 거래량 {r['vol_ratio']}배\n"
                f"   💬 {r['latest_news'][:60] if r['latest_news'] else '뉴스 없음'}\n"
                f"   🎯 업사이드 {upside_str} | 점수 {r['score']}\n"
            )
        lines.append(f"\n총 {len(results)}종목 감지")
        msg = "\n".join(lines)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_surge_scan(session=None):
    if session is None:
        session = get_market_session()
    filters = SESSION_FILTER[session]
    symbols = get_universe()
    results = []
    total = len(symbols)
    log.info(f"[{SESSION_LABEL[session]}] 스캔 시작: {total}개")

    def process(sym):
        data = get_market_data(sym, session)
        if data and abs(data["change_pct"]) >= filters["change"] and data["vol_ratio"] >= filters["vol"]:
            data["direction"] = "급등 🚀" if data["change_pct"] > 0 else "급락 📉"
            data["score"] = compute_surge_score(data)
            log.info(f"{data['direction']} {sym} {data['change_pct']:+.2f}% 거래량{data['vol_ratio']}배 점수{data['score']}")
            return data
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process, sym): sym for sym in symbols}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 30 == 0:
                log.info(f"진행: {done}/{total}")
            try:
                r = future.result()
                if r:
                    results.append(r)
            except Exception as e:
                log.debug(f"오류: {e}")

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"스캔 완료: {len(results)}개 감지")
    return results[:20], session
