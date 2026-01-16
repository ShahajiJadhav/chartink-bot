#!/usr/bin/env python3
"""
PythonAnywhere-friendly Chartink scanner
- buy_sell + HAMMER strategies
- IST logging to file + stdout
- Separate raw signal log
- Telegram alerts with dedupe
"""

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
import os

# ---------------- CONFIG ----------------
SIGNAL_AMOUNT = float(os.getenv("SIGNAL_AMOUNT"))
LEVERAGE = 5
MIN_QTY = 1

cookie_str= os.getenv("CHARTINK_COOKIE")
CHARTINK_CSRF_TOKEN= os.getenv("CHARTINK_CSRF_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL_S = 14

HOME = Path.home()
CACHE_FILE = HOME / "notified_cache_pyany.json"
LOG_FILE = HOME / "stock_bot.txt"
SIGNAL_LOG_FILE = HOME / "signals.log"

INDIA_TZ = pytz.timezone("Asia/Kolkata")
NOTIFY_UNTIL = dtime(hour=15, minute=15)


def parse_cookie_string_to_dict(cookie_string):
    cookies_dict = {}
    if not cookie_string:
        return cookies_dict

    # Split the string by the standard delimiter '; '
    pairs = cookie_string.split('; ')

    for pair in pairs:
        if '=' in pair:
            # Split the pair by the first '='
            name, value = pair.split('=', 1)
            
            # URL-decode the value to handle characters like '%22'
            decoded_value = urllib.parse.unquote(value)
            
            # Add the key-value pair to the dictionary
            cookies_dict[name.strip()] = decoded_value

    return cookies_dict

CHARTINK_COOKIE = parse_cookie_string_to_dict(cookie_str)

# ---------------- LOGGING ----------------
def ist_time(*args):
    return datetime.now(INDIA_TZ).timetuple()

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.Formatter.converter = ist_time

logger = logging.getLogger("scanner")
logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S"
    ))
    sh.formatter.converter = ist_time
    logger.addHandler(sh)

signal_logger = logging.getLogger("signal_logger")
signal_logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.FileHandler) for h in signal_logger.handlers):
    fh = logging.FileHandler(SIGNAL_LOG_FILE, mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    fh.formatter.converter = ist_time
    signal_logger.addHandler(fh)

def log(msg):
    logger.info(msg)

# ---------------- PAYLOADS ----------------
buy_payload  = {"scan_clause": '''( {1339018} ( ( {cash} ( ( {cash} (  daily close >  20 and  daily close <  50 and  [0] 5 minute volume >  5000000 ) ) or( {cash} (  daily close >  50 and  daily close <  100 and  [0] 5 minute volume >  4000000 ) ) or( {cash} (  daily close >  100 and  daily close <  250 and  [0] 5 minute volume >  3500000 ) ) or( {cash} (  daily close >  250 and  daily close <  1000 and  [0] 5 minute volume >  2500000 ) ) ) ) and( {cash} (  abs(  [0] 5 minute adx di positive( 14 ) -  [0] 5 minute adx di negative( 14 ) ) >  20 or  [0] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} >  [0] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} and  [ -1 ] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} <=  [ -1 ] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} or  [0] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} <  [0] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} and  [ -1 ] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} >=  [ -1 ] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} ) ) and  [0] 5 minute close >  [0] 5 minute open ) )'''}
sell_payload = {"scan_clause": '''( {1339018} ( ( {cash} ( ( {cash} (  daily close >  20 and  daily close <  50 and  [0] 5 minute volume >  5000000 ) ) or( {cash} (  daily close >  50 and  daily close <  100 and  [0] 5 minute volume >  4000000 ) ) or( {cash} (  daily close >  100 and  daily close <  250 and  [0] 5 minute volume >  3500000 ) ) or( {cash} (  daily close >  250 and  daily close <  1000 and  [0] 5 minute volume >  2500000 ) ) ) ) and( {cash} (  abs(  [0] 5 minute adx di positive( 14 ) -  [0] 5 minute adx di negative( 14 ) ) >  20 or  [0] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} >  [0] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} and  [ -1 ] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} <=  [ -1 ] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} or  [0] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} <  [0] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} and  [ -1 ] 5 minute {custom_indicator_185281_start}\"{custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100\"{custom_indicator_185281_end} >=  [ -1 ] 5 minute {custom_indicator_185282_start}\"ema(  {custom_indicator_185278_start}\"ema(  {custom_indicator_185277_start}\"ema(  close - 1 candle ago close , 10 )\"{custom_indicator_185277_end} , 26 )\"{custom_indicator_185278_end} /  {custom_indicator_185280_start}\"ema(  {custom_indicator_185279_start}\"ema( abs(  close - 1 candle ago close ) , 10 )\"{custom_indicator_185279_end} , 26 )\"{custom_indicator_185280_end} * 100 , 20 )\"{custom_indicator_185282_end} ) ) and  [0] 5 minute close <  [0] 5 minute open ) )'''}
buy_hammer_payload  = {"scan_clause": '''( {1339018} (  [0] 5 minute close >  [0] 5 minute open *  1.0045 and( {cash} (  abs(  [-1] 5 minute close -  [-1] 5 minute open ) <  abs(  [-1] 5 minute high -  [-1] 5 minute low ) *  0.4 or  [-1] 5 minute close -  [-1] 5 minute open <  [-1] 5 minute open *  1.003 ) ) and  [0] 5 minute close >  [0] 5 minute supertrend( 18 , 1.1 ) and( {cash} ( ( {cash} (  daily close >  25 and  daily close <  80 and  [0] 5 minute volume >  4000000 ) ) or( {cash} (  daily close >  80 and  daily close <  150 and  [0] 5 minute volume >  3000000 ) ) or( {cash} (  daily close >  150 and  daily close <  500 and  [0] 5 minute volume >  2000000 ) ) or( {cash} (  daily close >  500 and  daily close <  1500 and  [0] 5 minute volume >  1000000 ) ) or( {cash} (  daily close >  1500 and  [0] 5 minute volume >  400000 ) ) ) ) ) )'''}
sell_hammer_payload = {"scan_clause": '''( {1339018} (  [0] 5 minute close <  [0] 5 minute open *  0.9955 and( {cash} (  abs(  [-1] 5 minute close -  [-1] 5 minute open ) <  abs(  [-1] 5 minute high -  [-1] 5 minute low ) *  0.4 or  [-1] 5 minute open -  [-1] 5 minute close <  [-1] 5 minute close *  0.996 ) ) and  [0] 5 minute close <  [0] 5 minute supertrend( 18 , 1.1 ) and( {cash} ( ( {cash} (  daily close >  25 and  daily close <  80 and  [0] 5 minute volume >  4000000 ) ) or( {cash} (  daily close >  80 and  daily close <  150 and  [0] 5 minute volume >  3000000 ) ) or( {cash} (  daily close >  150 and  daily close <  500 and  [0] 5 minute volume >  2000000 ) ) or( {cash} (  daily close >  500 and  daily close <  1500 and  [0] 5 minute volume >  1000000 ) ) or( {cash} (  daily close >  1500 and  [0] 5 minute volume >  400000 ) ) ) ) ) )'''}


# ---------------- CHARTINK ----------------
def fetch_chartink_signals(side, payload):
    log(f"[chartink {side}] fetch start")

    cookies = CHARTINK_COOKIE or {}
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
            sym = (d.get("nsecode") or "").upper().strip()
            close = float(d.get("close", d.get("ltp", 0)) or 0)

            if not sym or close <= 0:
                continue

            qty = int((SIGNAL_AMOUNT * LEVERAGE) // close)

            signal_logger.info(f"{side} {sym} Qty={qty}")
            out.append({"symbol": sym, "side": side, "close": close})

        # log(f"[chartink {side}] fetch success - {len(out)}")
        log(out)
        return out

    except Exception as e:
        log(f"[chartink {side}] failed: {e}")
        return []

# ---------------- CACHE ----------------
def load_cache():
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE) as f:
            return {k: datetime.fromisoformat(v) for k, v in json.load(f).items()}
    except:
        return {}

def save_cache(d):
    with open(CACHE_FILE, "w") as f:
        json.dump({k: v.isoformat() for k, v in d.items()}, f)

# ---------------- TELEGRAM ----------------
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10).raise_for_status()
        log("[telegram] sent")
        return True
    except Exception as e:
        log(f"[telegram] failed: {e}")
        return False

# ---------------- MAIN LOOP ----------------
def main_loop():
    log("[main] starting loop")
    notified = load_cache()  # key -> utc datetime

    while True:
        now_ist = datetime.now(INDIA_TZ)
        if now_ist.time() >= NOTIFY_UNTIL:
            log("[main] notify-until reached")
            break

        now_utc = datetime.now(pytz.utc)

        # ---------------- expire old cache (20 min TTL) ----------------
        ttl_cutoff = now_utc - timedelta(minutes=20)
        notified = {k: v for k, v in notified.items() if v >= ttl_cutoff}

        # ---------------- fetch signals ----------------
        vol_buy   = fetch_chartink_signals("BUY",  buy_payload)
        vol_sell  = fetch_chartink_signals("SELL", sell_payload)
        ham_buy   = fetch_chartink_signals("BUY",  buy_hammer_payload)
        ham_sell  = fetch_chartink_signals("SELL", sell_hammer_payload)

        # ---------------- containers ----------------
        volume_msgs = {"BUY": [], "SELL": []}
        hammer_msgs = {"BUY": [], "SELL": []}

        volume_keys = []
        hammer_keys = []

        seen_cycle = set()  # per-cycle hard dedupe

        # ===================== VOLUME =====================
        for s in vol_buy + vol_sell:
            key = f"{s['symbol']}|VOLUME|{s['side']}"

            if key in notified or key in seen_cycle:
                continue

            seen_cycle.add(key)

            close = s["close"]
            qty = max(MIN_QTY, int((SIGNAL_AMOUNT * LEVERAGE) // close))

            volume_msgs[s["side"]].append(
                f"<b>{s['symbol']}</b> Qty={qty}"
            )
            volume_keys.append(key)

        # ===================== HAMMER =====================
        for s in ham_buy + ham_sell:
            key = f"{s['symbol']}|HAMMER|{s['side']}"

            if key in notified or key in seen_cycle:
                continue

            seen_cycle.add(key)

            close = s["close"]
            qty = max(MIN_QTY, int((SIGNAL_AMOUNT * LEVERAGE) // close))

            hammer_msgs[s["side"]].append(
                f"<b>{s['symbol']}</b> Qty={qty}"
            )
            hammer_keys.append(key)

        # ---------------- send volume alerts ----------------
        if volume_msgs["BUY"] or volume_msgs["SELL"]:
            parts = []
            if volume_msgs["BUY"]:
                parts.append("🟢 <u>VOLUME BUY</u>\n" + "\n".join(volume_msgs["BUY"]))
            if volume_msgs["SELL"]:
                parts.append("🔴 <u>VOLUME SELL</u>\n" + "\n".join(volume_msgs["SELL"]))

            if send_telegram("\n\n".join(parts)):
                for k in volume_keys:
                    notified[k] = now_utc

        # ---------------- send hammer alerts ----------------
        if hammer_msgs["BUY"] or hammer_msgs["SELL"]:
            parts = []
            if hammer_msgs["BUY"]:
                parts.append("🔨 <u>HAMMER BUY</u>\n" + "\n".join(hammer_msgs["BUY"]))
            if hammer_msgs["SELL"]:
                parts.append("🔨 <u>HAMMER SELL</u>\n" + "\n".join(hammer_msgs["SELL"]))

            if send_telegram("\n\n".join(parts)):
                for k in hammer_keys:
                    notified[k] = now_utc

        save_cache(notified)
        time.sleep(min(random.uniform(8, 12), POLL_INTERVAL_S))

    save_cache(notified)
    log("[main] done")

if __name__ == "__main__":
    main_loop()
