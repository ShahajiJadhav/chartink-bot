#!/usr/bin/env python3

from datetime import datetime, timedelta, time as dtime
from pathlib import Path
import json
import urllib.parse
import time
import requests
import pytz
import logging
import sys
import random
import signal
import threading
import ast,os

# ============================================================
# CONFIG
# ============================================================

SIGNAL_AMOUNT = 250.0

SIGNAL_AMOUNT = float(os.getenv("SIGNAL_AMOUNT"))

CHARTINK_COOKIE_RAW = os.getenv("CHARTINK_COOKIE")
CHARTINK_CSRF_TOKEN = os.getenv("CHARTINK_CSRF_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------------- SINGLE PAYLOAD ----------------
# signal_payload = {"scan_clause": '''( {1339018} (  abs(  [-1] 5 minute close -  [-1] 5 minute open ) >  [-1] 5 minute close *  0.01 and  abs(  [0] 5 minute close -  [0] 5 minute open ) >  [0] 5 minute close *  0.008 and  abs(  [-1] 5 minute close -  [-1] 5 minute open ) <  abs(  [-1] 5 minute high -  [-1] 5 minute low ) *  0.4 ) )'''}
signal_payload = {"scan_clause": '''( {1339018} ( ( {1339018} ( ( {cash} ( ( {cash} (  daily close >  20 and  daily close <  50 and  [0] 5 minute volume >  200000 ) ) or( {cash} (  daily close >  50 and  daily close <  100 and  [0] 5 minute volume >  150000 ) ) or( {cash} (  daily close >  100 and  daily close <  250 and  [0] 5 minute volume >  100000 ) ) or( {cash} (  daily close >  250 and  daily close <  500 and  [0] 5 minute volume >  50000 ) ) or( {cash} (  daily close >  500 and  daily close <  1000 and  [0] 5 minute volume >  30000 ) ) ) ) ) ) and  abs(  [0] 5 minute close -  [0] 5 minute open ) >  [0] 5 minute low *  0.012 and  [-1] 5 minute countstreak( 6, 1 where  abs(  [-1] 5 minute close -  [-1] 5 minute open ) <  [-1] 5 minute open *  0.003 ) >=  5 and  [0] 5 minute volume >  [-1] 5 minute sma(  [-1] 5 minute volume , 20 ) *  20 ) )'''}

HOME = Path.home()

CACHE_FILE = HOME / "notified_cache.json"
LOG_FILE   = HOME / "stock_bot.log"
SIGNAL_LOG_FILE = HOME / "signals.log"

INDIA_TZ = pytz.timezone("Asia/Kolkata")
NOTIFY_UNTIL = dtime(hour=15, minute=15)

RUN_INTERVAL_SECONDS = 90 * 60   # 1 hour 30 minutes


# ============================================================
# IST LOGGING
# ============================================================

def ist_time(*args):
    return datetime.now(INDIA_TZ).timetuple()

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.Formatter.converter = ist_time

logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    sh = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fmt.converter = ist_time
    sh.setFormatter(fmt)
    logger.addHandler(sh)

def _make_signal_logger(filepath):
    lg = logging.getLogger("SIGNAL_LOGGER")
    lg.setLevel(logging.INFO)

    if not any(isinstance(h, logging.FileHandler) for h in lg.handlers):
        fh = logging.FileHandler(filepath, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s - %(message)s")
        fmt.converter = ist_time
        fh.setFormatter(fmt)
        lg.addHandler(fh)

    return lg

signal_logger = _make_signal_logger(SIGNAL_LOG_FILE)

def log(msg):
    logger.info(msg)

# ============================================================
# SHUTDOWN HANDLING
# ============================================================

SHUTDOWN = threading.Event()

def handle_shutdown(signum, frame):
    log(f"[signal] received {signal.Signals(signum).name}, shutting down")
    SHUTDOWN.set()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# ============================================================
# CACHE
# ============================================================

def load_notified_cache():
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE) as f:
            return {k: datetime.fromisoformat(v) for k, v in json.load(f).items()}
    except Exception as e:
        log(f"[cache] load failed: {e}")
        return {}

def save_notified_cache(cache):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump({k: v.isoformat() for k, v in cache.items()}, f)
    except Exception as e:
        log(f"[cache] save failed: {e}")

# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(text):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML"
            },
            timeout=10,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        log(f"[telegram] send failed: {e}")
        return False

# ============================================================
# CHARTINK FETCH (SINGLE SIGNAL)
# ============================================================
def parse_cookie(raw):
    if not raw:
        return {}

    if isinstance(raw, dict):
        return raw

    if isinstance(raw, set):
        raw = next(iter(raw), "")

    cookies = {}
    for part in str(raw).split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


def fetch_chartink_signals(payload):
    cookies = cookies = parse_cookie(CHARTINK_COOKIE_RAW)
    token = CHARTINK_CSRF_TOKEN or cookies.get("XSRF-TOKEN")

    if token:
        token = urllib.parse.unquote(token)

    headers = {
        "Content-Type": "application/json",
        "Referer": "https://chartink.com/",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0",
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
            log(f"[chartink] scan_error: {data['scan_error']}")
            return []

        out = []
        for d in data.get("data", []):
            sym = (d.get("nsecode") or "").upper().strip()
            close = float(d.get("close", 0) or 0)

            if not sym or close <= 0:
                continue

            qty = int((SIGNAL_AMOUNT * 5) // close)
            msg = f"SIGNAL {sym} Qty={qty}"

            signal_logger.info(msg)
            print(msg)
            out.append({"symbol": sym, "close": close})
        return out

    except Exception as e:
        log(f"[chartink] request failed: {e}")
        return []

# ============================================================
# MAIN LOOP
# ============================================================

def main_loop():
    log("[main] loop started")
    notified = load_notified_cache()

    try:
        while not SHUTDOWN.is_set():
            now_ist = datetime.now(INDIA_TZ)
            if now_ist.time() >= NOTIFY_UNTIL:
                break

            cutoff = datetime.now(pytz.utc) - timedelta(minutes=20)
            notified = {k: v for k, v in notified.items() if v >= cutoff}

            signals = fetch_chartink_signals(signal_payload)

            msgs = []
            new_keys = []

            for s in signals:
                key = s["symbol"]
                if key in notified:
                    continue

                qty = int((SIGNAL_AMOUNT * 5) // s["close"])
                msgs.append(f"<b>{s['symbol']}</b> Qty={qty}")
                new_keys.append(key)

            if msgs:
                text = "📢 <u>Signal</u>\n" + "\n".join(msgs)

                if send_telegram(text):
                    now = datetime.now(pytz.utc)
                    for k in new_keys:
                        notified[k] = now
            else:
                log("[telegram] no new signals")

            save_notified_cache(notified)
            SHUTDOWN.wait(timeout=random.uniform(8, 12))

    finally:
        save_notified_cache(notified)
        log("[main] loop exited cleanly")

# ============================================================
# SCHEDULER
# ============================================================

if __name__ == "__main__":
    log("[boot] script started")
    
    while not SHUTDOWN.is_set():
        if datetime.now(INDIA_TZ).time() >= NOTIFY_UNTIL:
            break

        main_loop()
        SHUTDOWN.wait(timeout=RUN_INTERVAL_SECONDS)

    log("[boot] script finished cleanly")
