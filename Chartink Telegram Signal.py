#!/usr/bin/env python3
"""
PythonAnywhere-friendly script that runs repeatedly until 15:10 IST and logs to a file (basicConfig) + stdout.

- Edit the top CONFIG block with your tokens, cookie dict, and Chartink payloads.
- Polls Chartink every POLL_INTERVAL_S seconds until 15:10 IST.
- Sends one Telegram message per poll (BUY / SELL sections) for new signals (30-minute dedupe).
- Saves notified cache under the user's home directory.
- Logs to both ~/stock_bot.log (via logging.basicConfig) and to stdout.
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
import os
import urllib.parse

# ----------------- CONFIG (edit these directly) -----------------
SIGNAL_AMOUNT = os.getenv("SIGNAL_AMOUNT")

cookie_str= os.getenv("CHARTINK_COOKIE")
CHARTINK_CSRF_TOKEN= os.getenv("CHARTINK_CSRF_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Chartink payloads - replace with your full triple-quoted clauses
buy_payload  = {"scan_clause": '''( {1339018} ( [0] 5 minute close > [0] 5 minute open * 1.01 and( {cash} ( ( {cash} ( daily close > 25 and daily close < 80 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 4000000 ) ) or( {cash} ( daily close > 80 and daily close < 150 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 3000000 ) ) or( {cash} ( daily close > 150 and daily close < 500 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 2000000 ) ) or( {cash} ( daily close > 500 and daily close < 1500 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 1000000 ) ) or( {cash} ( daily close > 1500 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 400000 ) ) ) ) and [0] 30 minute close > [0] 30 minute low * 1.02 and( {cash} ( abs( [-1] 5 minute close - [-1] 5 minute open ) < abs( [-1] 5 minute high - [-1] 5 minute low ) * 0.4 or [-1] 5 minute close - [-1] 5 minute open < [-1] 5 minute open * 1.003 ) ) ) )'''}
sell_payload = {"scan_clause": '''( {1339018} ( [0] 5 minute close < [0] 5 minute open * 0.99 and( {cash} ( ( {cash} ( daily close > 25 and daily close < 80 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 4000000 ) ) or( {cash} ( daily close > 80 and daily close < 150 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 3000000 ) ) or( {cash} ( daily close > 150 and daily close < 500 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 2000000 ) ) or( {cash} ( daily close > 500 and daily close < 1500 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 1000000 ) ) or( {cash} ( daily close > 1500 and [0] 5 minute sum( [0] 5 minute volume , 12 ) > 400000 ) ) ) ) and [0] 30 minute close < [0] 30 minute high * 0.98 and( {cash} ( abs( [-1] 5 minute close - [-1] 5 minute open ) < abs( [-1] 5 minute high - [-1] 5 minute low ) * 0.4 or [-1] 5 minute open - [-1] 5 minute close < [-1] 5 minute close * 0.996 ) ) ) )'''}

# Poll interval in seconds
POLL_INTERVAL_S = 14

# local cache file & log file (under home directory)
HOME = Path.home()
CACHE_FILE = HOME / 'notified_cache_pyany.json'
LOG_FILE = HOME / 'stock_bot.txt'  # per your requested filename

# notify-until (15:15 IST)
INDIA_TZ = pytz.timezone('Asia/Kolkata')
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
# ----------------- Logging setup (user requested basicConfig) -----------------
def ist_time(*args):
    """Converter for logging timestamps to IST (returns time.struct_time)."""
    return datetime.now(INDIA_TZ).timetuple()

# Configure basicConfig for file logging (will use Formatter.converter override below)
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Force all logging.Formatter timestamps to use IST
logging.Formatter.converter = ist_time

logger = logging.getLogger("5x_signals")
logger.setLevel(logging.INFO)

# avoid adding duplicate stream handlers if script is reloaded
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    # use same datefmt and ensure StreamHandler also uses IST converter
    sh_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    sh_formatter.converter = ist_time
    sh.setFormatter(sh_formatter)
    logger.addHandler(sh)

# --- new: separate signal log file (timestamped in IST) ---
SIGNAL_LOG_FILE = HOME / 'signals.log'

# create a dedicated logger for raw signals (avoid duplicate handlers on reload)
signal_logger = logging.getLogger("signal_logger")
signal_logger.setLevel(logging.INFO)

# only add a FileHandler if not already present to avoid duplicate entries on reload
if not any(
    isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(SIGNAL_LOG_FILE)
    for h in signal_logger.handlers
):
    sh = logging.FileHandler(filename=str(SIGNAL_LOG_FILE), mode='a', encoding='utf-8')
    sh.setLevel(logging.INFO)
    sh_formatter = logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    # use the same IST converter so times in this file are IST
    sh_formatter.converter = ist_time
    sh.setFormatter(sh_formatter)
    signal_logger.addHandler(sh)

def log(msg: str):
    """Unified logging helper: writes message to the configured logger."""
    logger.info(msg)

# ----------------- Chartink signals -----------------

def fetch_chartink_signals(scan_type: str, payload: dict):
    """
    Return list of dicts: {'symbol', 'side', 'close'}
    Logs each fetched signal immediately to signal_logger as:
      "YYYY-mm-dd HH:MM:SS - SIDE SYMBOL Qty=<n>"
    Expects CHARTINK_COOKIE to already be a dict of cookies (or empty).
    """
    log(f"[chartink {scan_type}] fetch start")
    cookies = CHARTINK_COOKIE or {}
    token = CHARTINK_CSRF_TOKEN or cookies.get('XSRF-TOKEN')
    if token:
        token = urllib.parse.unquote(token)

    headers = {
        'Content-Type': 'application/json',
        'Referer': 'https://chartink.com/',
        'User-Agent': 'Mozilla/5.0',
        'X-Requested-With': 'XMLHttpRequest',
    }
    if token:
        headers['X-XSRF-TOKEN'] = token

    try:
        r = requests.post('https://chartink.com/screener/process', headers=headers, json=payload, cookies=cookies, timeout=12)
        r.raise_for_status()
        data = r.json()
        if data.get('scan_error'):
            log(f"[chartink {scan_type}] scan_error: {data['scan_error']}")
            return []

        out = []
        for d in data.get('data', []) or []:
            if not isinstance(d, dict):
                continue

            sym = (d.get('nsecode') or d.get('symbol') or '').upper().strip()
            close = None

            # preferential keys
            for k in ('close', 'ltp', 'last', 'last_price', 'close_price'):
                if k in d and d[k] not in (None, ''):
                    try:
                        close = float(d[k])
                        break
                    except Exception:
                        pass

            # fallback: try any numeric-like value > 1
            if close is None:
                for v in d.values():
                    try:
                        f = float(v)
                        if f > 1:
                            close = f
                            break
                    except Exception:
                        continue

            if not sym or close is None:
                # still skip if we couldn't extract symbol or close
                continue

            # compute qty using the same SIGNAL_AMOUNT * 5 logic as main loop
            try:
                qty = int((SIGNAL_AMOUNT * 5) // close) if close and close > 0 else 0
            except Exception:
                qty = 0

            # log the raw signal to the separate signal log (IST timestamps via formatter)
            try:
                # e.g. "2025-09-16 14:05:03 - BUY ADANIPOWER Qty=10"
                signal_logger.info(f"{scan_type.upper()} {sym} Qty={qty}")
            except Exception as e:
                # defensive: don't fail the whole fetch on logging errors
                log(f"[signal-logger] failed to write signal for {sym}: {e}")

            out.append({'symbol': sym, 'side': scan_type.upper(), 'close': close})

        log(f"[chartink {scan_type}] fetch success - found {len(out)} items")
        return out

    except Exception as e:
        log(f"[chartink {scan_type}] request failed: {e}")
        return []

# ----------------- Notifications cache & telegram -----------------

def load_notified_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, 'r') as f:
            data = json.load(f)
        out = {k: datetime.fromisoformat(v) for k, v in data.items()}
        return out
    except Exception as e:
        log(f"[cache] load failed: {e}")
        return {}

def save_notified_cache(d: dict):
    try:
        serial = {k: v.isoformat() for k, v in d.items()}
        with open(CACHE_FILE, 'w') as f:
            json.dump(serial, f)
        log(f"[cache] saved {len(serial)} items to {CACHE_FILE}")
    except Exception as e:
        log(f"[cache] save failed: {e}")

def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log('[telegram] missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID')
        return False
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': text, 'parse_mode': 'HTML'}
    try:
        r = requests.post(url, json=payload, timeout=8)
        r.raise_for_status()
        log('[telegram] sent')
        return True
    except Exception as e:
        # try to log response body if available for debugging
        resp_text = ''
        try:
            resp_text = f" response={r.text[:1000]!r}"
        except Exception:
            resp_text = ''
        log(f'[telegram] send failed: {e}{resp_text}')
        return False

# ----------------- Helper: within notify window -----------------

def within_notify_window(now_dt: datetime) -> bool:
    t = now_dt.astimezone(INDIA_TZ).time()
    return t <= NOTIFY_UNTIL

# ----------------- Main loop (run until NOTIFY_UNTIL IST) -----------------
import random

def main_loop():
    log('[main] starting loop until notify-until IST (15:15)')
    notified = load_notified_cache()

    try:
        while True:
            # check IST end condition immediately (simplest & robust)
            now_ist = datetime.now(INDIA_TZ)
            if now_ist.time() >= NOTIFY_UNTIL:
                log('[main] notify-until reached — stopping notifications')
                break

            # prune cache using UTC cutoff as before
            cutoff = datetime.now(pytz.utc) - timedelta(minutes=20)
            notified = {k: v for k, v in notified.items() if v >= cutoff}

            # --- fetch & process signals (keeps your original logic) ---
            buy_signals = fetch_chartink_signals('BUY', buy_payload)
            sell_signals = fetch_chartink_signals('SELL', sell_payload)
            signals = buy_signals + sell_signals
            # signals =  sell_signals
            log(f"[main] {len(signals)} total signals this poll")

            buy_msgs, sell_msgs = [], []
            new_keys = []

            for s in signals:
                key = f"{s['symbol']}|{s['side']}"
                last = notified.get(key)
                if last and last >= cutoff:
                    log(f"[main] skip recent {key}")
                    continue

                close = s['close']
                qty = int((SIGNAL_AMOUNT * 5) // close) if close and close > 0 else 0
                msg = f"<b>{s['symbol']}</b> Qty={qty}"

                if s['side'] == "BUY":
                    buy_msgs.append(msg)
                else:
                    sell_msgs.append(msg)

                new_keys.append(key)

            if not new_keys:
                log("[main] no new signals to notify")
                save_notified_cache(notified)
            else:
                parts = []
                if buy_msgs:
                    parts.append("\n🟢 <u>Buy</u>\n" + "\n".join(buy_msgs))
                if sell_msgs:
                    parts.append("\n🔴 <u>Sell</u>\n" + "\n".join(sell_msgs))
                full_msg = "\n".join(parts).strip()

                # NEW: prevent empty message from being sent
                if not full_msg:
                    log("[main] full_msg empty — skipping telegram send")
                    save_notified_cache(notified)

                else:
                    log(f"[main] attempting to send message for keys: {new_keys}")
                    log(full_msg)

                    send_ok = False
                    for attempt in range(1, 4):
                        if send_telegram(full_msg):
                            send_ok = True
                            log(f"[main] telegram send succeeded on attempt {attempt}")
                            break
                        else:
                            wait = 2 ** attempt
                            log(f"[main] telegram send failed on attempt {attempt}; retrying in {wait}s")
                            time.sleep(wait)

                    if send_ok:
                        now_sent = datetime.now(pytz.utc)
                        for k in new_keys:
                            notified[k] = now_sent
                        log(f"[main] marked {len(new_keys)} keys as notified")
                    else:
                        log("[main] final: telegram send failed after retries; NOT marking keys as notified")

                    save_notified_cache(notified)

            # --- randomized sleep 9..14s but never sleep past the notify-until ---
            end_today = now_ist.replace(hour=NOTIFY_UNTIL.hour, minute=NOTIFY_UNTIL.minute,
                                       second=0, microsecond=0)
            remaining = (end_today - now_ist).total_seconds()

            if remaining <= 0:
                log('[main] notify-until reached while sleeping — exiting loop')
                break

            random_sleep = random.uniform(8, 12)        # pick a random sleep interval
            sleep_seconds = min(random_sleep, remaining)  # don't sleep past the window end

            time.sleep(sleep_seconds)

    except Exception as e:
        log(f"[main] unexpected error: {e}")

    finally:
        save_notified_cache(notified)
        log('[main] done')


if __name__ == '__main__':
    log("[boot] GitHub Actions run started")

    now_ist = datetime.now(INDIA_TZ)

    # Safety: don't start if already past notify-until
    if now_ist.time() >= NOTIFY_UNTIL:
        log("[boot] notify-until already passed, exiting")
        sys.exit(0)

    # Run ONCE for the entire market session
    main_loop()

    log("[boot] GitHub Actions run finished cleanly")
    sys.exit(0)
