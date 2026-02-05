#!/usr/bin/env python3
"""
Chartink → Telegram Signal Bot (Single File)

✔ GitHub-friendly
✔ PythonAnywhere compatible
✔ No hardcoded secrets
✔ IST logging to file + stdout
✔ BUY / SELL signals with dedupe
✔ Runs until notify-until time

Environment variables required:
- SIGNAL_AMOUNT
- CHARTINK_COOKIE
- CHARTINK_CSRF_TOKEN
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
"""

# ===================== IMPORTS =====================
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
import json
import urllib.parse
import time
import requests
import pytz
import logging
import sys
import os
import ast
import random

# ===================== CONFIG =====================
SIGNAL_AMOUNT = float(os.getenv("SIGNAL_AMOUNT"))

CHARTINK_COOKIE_RAW = os.getenv("CHARTINK_COOKIE")
CHARTINK_CSRF_TOKEN = os.getenv("CHARTINK_CSRF_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

POLL_INTERVAL_S = 14

HOME = Path.home()
CACHE_FILE = HOME / "notified_cache.json"
LOG_FILE = HOME / "stock_bot.log"
SIGNAL_LOG_FILE = HOME / "signals.log"

INDIA_TZ = pytz.timezone("Asia/Kolkata")
NOTIFY_UNTIL = dtime(hour=15, minute=15)

# ===================== LOGGING =====================
def ist_time(*_):
    return datetime.now(INDIA_TZ).timetuple()

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.Formatter.converter = ist_time
logger = logging.getLogger("chartink")
logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S"
    ))
    sh.formatter.converter = ist_time
    logger.addHandler(sh)

signal_logger = logging.getLogger("signals")
signal_logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.FileHandler) for h in signal_logger.handlers):
    fh = logging.FileHandler(SIGNAL_LOG_FILE, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    fh.formatter.converter = ist_time
    signal_logger.addHandler(fh)

def log(msg: str):
    logger.info(msg)

# ===================== PAYLOADS =====================
# buy_payload  = {"scan_clause": '''( {1339018} (  [0] 5 minute close >  [0] 5 minute open *  1.0045 and( {cash} (  abs(  [-1] 5 minute close -  [-1] 5 minute open ) <  abs(  [-1] 5 minute high -  [-1] 5 minute low ) *  0.4 or  [-1] 5 minute close -  [-1] 5 minute open <  [-1] 5 minute open *  1.003 ) ) and  [0] 5 minute close >  [0] 5 minute supertrend( 18 , 1.1 ) and( {cash} ( ( {cash} (  daily close >  25 and  daily close <  80 and  [0] 5 minute volume >  4000000 ) ) or( {cash} (  daily close >  80 and  daily close <  150 and  [0] 5 minute volume >  3000000 ) ) or( {cash} (  daily close >  150 and  daily close <  500 and  [0] 5 minute volume >  2000000 ) ) or( {cash} (  daily close >  500 and  daily close <  1500 and  [0] 5 minute volume >  1000000 ) ) or( {cash} (  daily close >  1500 and  [0] 5 minute volume >  400000 ) ) ) ) ) )'''}
sell_payload = {"scan_clause": '''( {1339018} (  [0] 5 minute close <  [0] 5 minute open *  0.9955 and( {cash} (  abs(  [-1] 5 minute close -  [-1] 5 minute open ) <  abs(  [-1] 5 minute high -  [-1] 5 minute low ) *  0.4 or  [-1] 5 minute open -  [-1] 5 minute close <  [-1] 5 minute close *  0.996 ) ) and  [0] 5 minute close <  [0] 5 minute supertrend( 18 , 1.1 ) and( {cash} ( ( {cash} (  daily close >  25 and  daily close <  80 and  [0] 5 minute volume >  4000000 ) ) or( {cash} (  daily close >  80 and  daily close <  150 and  [0] 5 minute volume >  3000000 ) ) or( {cash} (  daily close >  150 and  daily close <  500 and  [0] 5 minute volume >  2000000 ) ) or( {cash} (  daily close >  500 and  daily close <  1500 and  [0] 5 minute volume >  1000000 ) ) or( {cash} (  daily close >  1500 and  [0] 5 minute volume >  400000 ) ) ) ) ) )'''}
buy_payload  = {"scan_clause": '''( {1339018} ( ( {cash} ( ( {cash} (  daily close >  25 and  daily close <  80 and( {cash} (  [0] 10 minute volume >  50000 or( {cash} (  [0] 5 minute volume >  30000 and  [-1] 5 minute volume >  20000 ) ) ) ) ) ) or( {cash} (  daily close >  80 and  daily close <  150 and( {cash} (  [0] 10 minute volume >  45000 or( {cash} (  [0] 5 minute volume >  25000 and  [-1] 5 minute volume >  20000 ) ) ) ) ) ) or( {cash} (  daily close >  150 and  daily close <  500 and( {cash} (  [0] 10 minute volume >  35000 or( {cash} (  [0] 5 minute volume >  14000 and  [-1] 5 minute volume >  14000 ) ) ) ) ) ) or( {cash} (  daily close >  500 and( {cash} (  [0] 10 minute volume >  30000 or( {cash} (  [-1] 5 minute volume >  10000 and  [0] 5 minute volume >  25000 ) ) ) ) ) ) ) ) and  daily volume >  600000 and( {cash} (  [0] 5 minute close >  1 day ago close *  1.015 or  [0] 5 minute close <  1 day ago close *  0.985 ) ) and  [0] 5 minute adx( 14 ) >  [-1] 5 minute adx( 14 ) +  2.2 and  abs(  [0] 5 minute {custom_indicator_185281_start}"{custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100"{custom_indicator_185281_end} -  [0] 5 minute {custom_indicator_185282_start}"ema(  {custom_indicator_185278_start}"ema(  {custom_indicator_185277_start}"ema(  close - 1 candle ago close , 10 )"{custom_indicator_185277_end} , 26 )"{custom_indicator_185278_end} /  {custom_indicator_185280_start}"ema(  {custom_indicator_185279_start}"ema( abs(  close - 1 candle ago close ) , 10 )"{custom_indicator_185279_end} , 26 )"{custom_indicator_185280_end} * 100 , 20 )"{custom_indicator_185282_end} ) >  10 and  abs(  [0] 5 minute adx di positive( 14 ) -  [0] 5 minute adx di negative( 14 ) ) >  10 ) )'''}

# ===================== HELPERS =====================
def parse_cookie(blob: str) -> dict:
    if not blob:
        return {}
    try:
        return ast.literal_eval(blob)
    except Exception:
        out = {}
        for part in blob.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                out[k.strip()] = v.strip()
        return out

def fetch_chartink_signals(side: str, payload: dict):
    log(f"[chartink {side}] fetch start")

    cookies = parse_cookie(CHARTINK_COOKIE_RAW)
    token = CHARTINK_CSRF_TOKEN or cookies.get("XSRF-TOKEN")
    if token:
        token = urllib.parse.unquote(token)

    headers = {
        "Content-Type": "application/json",
        "Referer": "https://chartink.com/",
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["X-XSRF-TOKEN"] = token

    try:
        r = requests.post(
            "https://chartink.com/screener/process",
            headers=headers,
            json=payload,
            cookies=cookies,
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()

        if data.get("scan_error"):
            log(f"[chartink {side}] scan_error: {data['scan_error']}")
            return []

        out = []
        for d in data.get("data", []):
            sym = (d.get("nsecode") or "").upper()
            close = next(
                (float(d[k]) for k in ("close", "ltp", "last_price") if k in d and d[k]),
                None,
            )
            if not sym or not close:
                continue

            qty = int((SIGNAL_AMOUNT * 5) // close)
            signal_logger.info(f"{side} {sym} Qty={qty}")
            out.append({"symbol": sym, "side": side, "close": close})

        log(f"[chartink {side}] found {len(out)}")
        return out

    except Exception as e:
        log(f"[chartink {side}] failed: {e}")
        return []

def load_cache():
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE) as f:
            return {k: datetime.fromisoformat(v) for k, v in json.load(f).items()}
    except Exception:
        return {}

def save_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump({k: v.isoformat() for k, v in cache.items()}, f)

def send_telegram(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        log(f"[telegram] failed: {e}")
        return False

def create_github_issue(msg):
    try:
        token = os.getenv("GITHUB_TOKEN")
        repo = os.getenv("GITHUB_REPOSITORY")

        if not token or not repo:
            return False

        url = f"https://api.github.com/repos/{repo}/issues"

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json"
        }

        data = {
            "title": "🚨 Chartink Signal Alert",
            "body": msg
        }

        requests.post(url, headers=headers, json=data)
        return True

    except Exception as e:
        log(f"[github issue] failed: {e}")
        return False


# ===================== MAIN LOOP =====================
def main():
    log("[main] started")
    cache = load_cache()

    while datetime.now(INDIA_TZ).time() < NOTIFY_UNTIL:
        cutoff = datetime.now(pytz.utc) - timedelta(minutes=10)
        cache = {k: v for k, v in cache.items() if v >= cutoff}

        signals = (
            fetch_chartink_signals("BUY", buy_payload)
            + fetch_chartink_signals("SELL", sell_payload)
        )

        buy, sell, keys = [], [], []

        for s in signals:
            key = f"{s['symbol']}|{s['side']}"
            if key in cache:
                continue

            qty = int((SIGNAL_AMOUNT * 5) // s["close"])
            line = f"<b>{s['symbol']}</b> Qty={qty}"
            (buy if s["side"] == "BUY" else sell).append(line)
            keys.append(key)

        if keys:
            msg = ""
            if buy:
                msg += "\n🟢 Buy\n" + "\n".join(buy)
            if sell:
                msg += "\n🔴 Sell\n" + "\n".join(sell)
            final_msg = msg.strip()

            sent = send_telegram(final_msg)
            create_github_issue(final_msg)
            
            if sent:
                now = datetime.now(pytz.utc)
                for k in keys:
                    cache[k] = now
                save_cache(cache)

        time.sleep(min(random.uniform(8, 12), POLL_INTERVAL_S))

    save_cache(cache)
    log("[main] stopped")

if __name__ == "__main__":
    main()
