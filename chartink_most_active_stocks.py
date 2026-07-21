"""
Chartink BUY/SELL signal monitor.

Assumes these already exist elsewhere in your project and are importable
here (they're referenced but not redefined): `Config`, `logger`, `telegram`,
`with_retry_call`, `buy_payload`, `sell_payload`. Swap the import block
below for wherever those actually live.
"""
import json
import logging
import os
import random
import time
import urllib.parse
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from zoneinfo import ZoneInfo

# from your_project import Config, telegram, with_retry_call
# from your_project import buy_payload, sell_payload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("Chartink_Monitor")


class Config:
    CHARTINK_URL = "https://chartink.com/screener/process"
    CHARTINK_COOKIE_RAW = os.environ.get("CHARTINK_COOKIE_RAW", "")
    CHARTINK_CSRF_TOKEN = os.environ.get("CHARTINK_CSRF_TOKEN", "")
    CHARTINK_REFRESH_AFTER = int(os.environ.get("CHARTINK_REFRESH_AFTER", "10"))
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


class _Telegram:
    def notify(self, text: str) -> bool:
        if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
            logger.error("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; skipping send.")
            return False

        url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": Config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        }
        try:
            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
            return True
        except requests.RequestException as e:
            logger.error("Telegram send failed: %s", e)
            return False


telegram = _Telegram()


def with_retry_call(fn, label: str = "", fallback: Any = None,
                     exceptions: tuple = (Exception,),
                     attempts: int = 3, base_delay: float = 1.5):
    """Call fn() with retries. Returns fallback if every attempt fails."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except exceptions as e:
            logger.warning("[%s] attempt %s/%s failed: %s", label, attempt, attempts, e)
            if attempt < attempts:
                time.sleep(base_delay * attempt)
    logger.error("[%s] all %s attempts failed, using fallback", label, attempts)
    return fallback


IST_ZONE = ZoneInfo("Asia/Kolkata")
MARKET_CLOSE_IST = dt_time(15, 30)

POLL_MIN_SECONDS = 15
POLL_MAX_SECONDS = 30

BLOCK_HOURS = 2.0          # a symbol's cache entry lives this long
COOLDOWN_MINUTES = 30      # hard block applied right after a side-flip re-alert

# Persisted so the 2h/30min windows survive across polls within a run. If
# this runs on GitHub Actions, point this at a path covered by actions/cache
# (e.g. .cache/chartink_state.json) and restore/save that cache key between
# workflow runs, or the windows reset every run.
STATE_FILE = Path(os.environ.get("CHARTINK_STATE_FILE", ".cache/chartink_state.json"))

chartink_empty_streak = 0


# --------------------------------------------------------------------------
# Chartink fetch (unchanged from what you gave me)
# --------------------------------------------------------------------------
def parse_cookie(raw: str) -> dict:
    cookies = {}
    for part in raw.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


def fetch_chartink_signals(scan_type: str, payload: dict) -> list:
    logger.info(f"[chartink:{scan_type}] fetch start")

    cookies = parse_cookie(Config.CHARTINK_COOKIE_RAW)
    token = Config.CHARTINK_CSRF_TOKEN or cookies.get("XSRF-TOKEN", "")
    if token:
        token = urllib.parse.unquote(token)

    headers = {
        "Referer": "https://chartink.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["X-XSRF-TOKEN"] = token

    def _call():
        r = requests.post(
            Config.CHARTINK_URL,
            headers=headers,
            data=payload,
            cookies=cookies,
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("scan_error"):
            logger.error(f"[chartink:{scan_type}] scan_error: {data['scan_error']}")
            telegram.notify("Upstox Chartink scan error")
            return []

        rows = [
            d for d in data.get("data", [])
            if isinstance(d, dict) and d.get("nsecode")
        ]
        if rows:
            logger.info(f"[chartink:{scan_type}] symbols: {[r['nsecode'].upper() for r in rows]}")
        return [
            {
                "symbol": d["nsecode"].upper(),
                "side": scan_type.upper(),
                "close": d.get("close"),
                "per_chg": d.get("per_chg"),
            }
            for d in rows
        ]

    return with_retry_call(
        _call, label=f"chartink:{scan_type}", fallback=[],
        exceptions=(requests.RequestException, ValueError, KeyError),
    )


def gather_signals() -> list:
    global chartink_empty_streak
    out = fetch_chartink_signals("BUY", buy_payload)
    out += fetch_chartink_signals("SELL", sell_payload)
    seen, uniq = set(), []
    for s in out:
        key = (s["symbol"], s["side"])
        if key not in seen:
            seen.add(key)
            uniq.append(s)
    if not uniq:
        chartink_empty_streak += 1
        if chartink_empty_streak >= Config.CHARTINK_REFRESH_AFTER:
            logger.warning(
                f"signals: {chartink_empty_streak} consecutive empty scans - "
                f"the Chartink cookie/token may have expired and need manual refresh."
            )
            chartink_empty_streak = 0
    else:
        chartink_empty_streak = 0
    return uniq


# --------------------------------------------------------------------------
# Alert state (persisted so cache/cooldown windows survive across polls)
# --------------------------------------------------------------------------
class AlertState:
    """
    Per-symbol record:
        {
            "side": "BUY" | "SELL",         # side at last notification
            "notified_at": iso str,
            "cooldown_until": iso str|None  # 30-min hard block after a flip-alert
        }
    """

    def __init__(self, path: Path = STATE_FILE):
        self.path = path
        self.data: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if self.path.exists():
            try:
                with open(self.path, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Could not read state file (%s), starting fresh: %s", self.path, e)
        return {}

    def save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "w") as f:
                json.dump(self.data, f, indent=2)
        except OSError as e:
            logger.error("Failed to save state file: %s", e)

    def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.data.get(symbol)

    def set(self, symbol: str, side: str, notified_at: datetime,
            cooldown_until: Optional[datetime]) -> None:
        self.data[symbol] = {
            "side": side,
            "notified_at": notified_at.isoformat(),
            "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
        }


# --------------------------------------------------------------------------
# Alert decision engine
#
# Chartink already hands us a discrete BUY/SELL, so there's no "dead zone"
# or "% move" like the NSE version. The equivalent of a re-alert-worthy
# move here is the symbol flipping sides (BUY scan today, SELL scan next
# poll) — that's the meaningful event. Same side, still fresh -> duplicate,
# skip. Cache entry aged out (>= BLOCK_HOURS) -> treat as a fresh signal.
# --------------------------------------------------------------------------
class AlertEngine:
    def __init__(self, state: AlertState):
        self.state = state

    @staticmethod
    def _now() -> datetime:
        return datetime.now(tz=IST_ZONE)

    @staticmethod
    def _parse_iso(value: Optional[str]) -> Optional[datetime]:
        return datetime.fromisoformat(value) if value else None

    def evaluate(self, sig: Dict[str, str]) -> Optional[Dict[str, Any]]:
        symbol = sig["symbol"]
        side = sig["side"]
        now = self._now()
        entry = self.state.get(symbol)

        # 1. 30-minute cooldown after a flip-alert -> ignore
        if entry:
            cooldown_until = self._parse_iso(entry.get("cooldown_until"))
            if cooldown_until and now < cooldown_until:
                logger.info("%s in 30-min cooldown until %s, skipping", symbol, cooldown_until)
                return None

        # 2. Expire the 2-hour cache entry if it's aged out
        if entry:
            notified_at = self._parse_iso(entry["notified_at"])
            if now - notified_at >= timedelta(hours=BLOCK_HOURS):
                entry = None  # treated as "not in cache"

        # 3. Not in cache -> first notification
        if entry is None:
            self.state.set(symbol, side, now, cooldown_until=None)
            return {"symbol": symbol, "side": side, "first": True, "row": sig}

        # 4. In cache, same side -> duplicate, skip
        if entry["side"] == side:
            logger.info("%s cached as %s, skipping duplicate", symbol, side)
            return None

        # 5. In cache, side flipped -> re-alert, then hard-block for 30 min
        cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
        self.state.set(symbol, side, now, cooldown_until=cooldown_until)
        return {"symbol": symbol, "side": side, "first": False, "row": sig}

    @staticmethod
    def format_batch(alerts: List[Dict[str, Any]]) -> str:
        """Combine every signal from one poll into a single message."""
        lines = [f"*Signals* ({len(alerts)}) — {datetime.now(tz=IST_ZONE).strftime('%H:%M:%S')} IST"]
        for a in alerts:
            row = a["row"]
            tag = "🟢 BUY" if a["side"] == "BUY" else "🔴 SELL"
            kind = "new" if a["first"] else "flip re-alert"
            close = row.get("close")
            per_chg = row.get("per_chg")
            volume = row.get("volume")
            vol_str = f"{volume:,}" if isinstance(volume, (int, float)) else str(volume)
            lines.append(
                f"{tag} `{a['symbol']}` ({kind}) — LTP {close}, Chg {per_chg}%, Vol {vol_str}"
            )
        return "\n".join(lines)

buy_payload = {"scan_clause": '''( {1339018} (  daily volume *  daily "high+low/2" >  1500000000 and  daily close >  1 day ago close *  1.04 ) )'''}
sell_payload = {"scan_clause": '''( {1339018} (  daily volume *  daily "high+low/2" >  1500000000 and  daily close <  1 day ago close *  0.96 ) )'''}

# --------------------------------------------------------------------------
# Entry point - loops every 12-22s until market close (15:30 IST),
# batching every signal from a single poll into one Telegram message.
# --------------------------------------------------------------------------
def run_until_close() -> None:
    state = AlertState()
    engine = AlertEngine(state)

    poll_count = 0
    while datetime.now(tz=IST_ZONE).time() < MARKET_CLOSE_IST:
        poll_count += 1
        try:
            signals = gather_signals()

            alerts = []
            for sig in signals:
                result = engine.evaluate(sig)
                if result:
                    alerts.append(result)

            if alerts:
                message = engine.format_batch(alerts)
                telegram.notify(message)
                logger.info("Sent batched alert for %s symbol(s): %s",
                            len(alerts), [a["symbol"] for a in alerts])

            state.save()

        except Exception as e:
            # Keep the loop alive across transient Chartink/network hiccups.
            logger.error("Poll #%s failed: %s", poll_count, e)

        sleep_for = random.uniform(POLL_MIN_SECONDS, POLL_MAX_SECONDS)
        time.sleep(sleep_for)

    logger.info("Market closed (15:30 IST). Loop ending after %s poll(s).", poll_count)


if __name__ == "__main__":
    run_until_close()
