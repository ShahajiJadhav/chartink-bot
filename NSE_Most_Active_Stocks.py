import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from zoneinfo import ZoneInfo


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("NSE_Monitor")


# --------------------------------------------------------------------------
# Alert configuration
# --------------------------------------------------------------------------
BUY_THRESHOLD = 2.0        # pChange > this  -> BUY signal
SELL_THRESHOLD = -2.0      # pChange < this  -> SELL signal
BLOCK_HOURS = 2.0          # a symbol's cache entry lives this long
RENOTIFY_DIFF_PCT = 3.0    # min move needed to re-alert while still cached
COOLDOWN_MINUTES = 30      # hard block applied after a re-alert

POLL_MIN_SECONDS = 12
POLL_MAX_SECONDS = 22

IST_ZONE = ZoneInfo("Asia/Kolkata")
MARKET_CLOSE_IST = dt_time(23, 30)

# Where alert state is persisted. On GitHub Actions this file needs to be
# committed back to the repo (or restored from cache) between runs, or the
# 2h/30min windows will never survive across separate workflow runs.
STATE_FILE = Path(os.environ.get("NSE_STATE_FILE", "state.json"))

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


# --------------------------------------------------------------------------
# NSE fetch/rank/filter (unchanged from your original)
# --------------------------------------------------------------------------
class NSEMarketMonitor:
    BASE_URL = "https://www.nseindia.com"
    REFERER_URL = "https://www.nseindia.com/market-data/most-active-equities"
    API_URL = "https://www.nseindia.com/api/live-analysis-most-active-securities"

    RANK_BY = "value"  # "value" or "volume"

    MARKET_CLOSE_IST = MARKET_CLOSE_IST
    IST_ZONE = IST_ZONE

    EXCLUDE_SYMBOLS = {
        "HDFCBANK",
        "BSE",
        "SBIN",
        "RELIANCE",
        "LIQUIDBEES",
        "ICICIBANK", "BHARTIARTL", "TCS", "INFY", "LT", "WIPRO", "ETERNAL"
    }

    def __init__(self):
        if self.RANK_BY not in {"value", "volume"}:
            raise ValueError("RANK_BY must be 'value' or 'volume'.")

        self.exclude_symbols = {x.upper() for x in self.EXCLUDE_SYMBOLS}

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": self.REFERER_URL,
            "Connection": "keep-alive",
        })

        logger.info(
            "Initialized rank_by=%s excluded=%s",
            self.RANK_BY,
            sorted(self.exclude_symbols)
        )

    def _now_ist(self) -> datetime:
        return datetime.now(tz=self.IST_ZONE)

    def _warmup(self) -> bool:
        for url in (self.REFERER_URL, self.BASE_URL):
            try:
                resp = self.session.get(url, timeout=15)
                if resp.ok:
                    logger.info("Warmup successful via %s", url)
                    return True
                logger.warning("Warmup got HTTP %s for %s", resp.status_code, url)
            except requests.RequestException as e:
                logger.warning("Warmup failed for %s: %s", url, e)
        return False

    def _fetch_rows(self) -> List[Dict[str, Any]]:
        params = {"index": self.RANK_BY}

        try:
            resp = self.session.get(self.API_URL, params=params, timeout=20)

            if resp.status_code in (401, 403):
                logger.warning("API returned %s, retrying after warmup", resp.status_code)
                self._warmup()
                time.sleep(1.5)
                resp = self.session.get(self.API_URL, params=params, timeout=20)

            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", [])

            if not isinstance(rows, list):
                raise ValueError("Unexpected API response: data is not a list")

            logger.info("Fetched %s rows from NSE API", len(rows))
            return rows

        except requests.exceptions.JSONDecodeError as e:
            raise RuntimeError(f"JSON decode failed: {e}") from e
        except requests.RequestException as e:
            raise RuntimeError(f"HTTP request failed: {e}") from e

    @staticmethod
    def _safe_float(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).replace(",", "").strip())
        except Exception:
            return 0.0

    def _sort_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        primary = "pChange" if self.RANK_BY == "value" else "pChange"
        secondary = "totalTradedValue" if self.RANK_BY == "value" else "totalTradedVolume"

        return sorted(
            rows,
            key=lambda r: (
                self._safe_float(r.get(primary)),
                self._safe_float(r.get(secondary)),
                self._safe_float(r.get("lastPrice"))
            ),
            reverse=True
        )

    def _filter_excluded(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered = [
            row for row in rows
            if str(row.get("symbol", "")).upper() not in self.exclude_symbols
        ]
        logger.info(
            "Rows after exclude filter: total=%s excluded=%s remaining=%s",
            len(rows),
            len(rows) - len(filtered),
            len(filtered)
        )
        return filtered

    def get_all_stocks(self) -> List[Dict[str, Any]]:
        """Fetch, rank, and return every stock except the excluded symbols."""
        rows = self._fetch_rows()
        if not rows:
            return []

        ranked = self._sort_rows(rows)
        filtered = self._filter_excluded(ranked)

        logger.info("Stocks after exclusion filter: %s", len(filtered))
        return filtered

    def build_table_rows(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten raw API rows into plain values ready for tabular display."""
        table_rows = []
        for stock in stocks:
            table_rows.append({
                "Symbol": str(stock.get("symbol", "N/A")).upper(),
                "LTP": round(self._safe_float(stock.get("lastPrice")), 2),
                "Change %": round(self._safe_float(stock.get("pChange")), 2),
                "Value (Cr)": round(self._safe_float(stock.get("totalTradedValue")) / 10000000, 2)
            })
        return table_rows

    def build_dataframe(self, stocks: List[Dict[str, Any]]):
        """Return a pandas DataFrame - renders as a clean table in Colab."""
        import pandas as pd
        return pd.DataFrame(self.build_table_rows(stocks))


# --------------------------------------------------------------------------
# Telegram
# --------------------------------------------------------------------------
def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; skipping send.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
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


# --------------------------------------------------------------------------
# Alert state (persisted to disk so it survives separate script runs)
# --------------------------------------------------------------------------
class AlertState:
    """
    Per-symbol record:
        {
            "change": float,              # pChange at last notification
            "notified_at": iso str,       # when last notified
            "cooldown_until": iso str|None  # 30-min hard block, if any
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
            with open(self.path, "w") as f:
                json.dump(self.data, f, indent=2)
        except OSError as e:
            logger.error("Failed to save state file: %s", e)

    def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.data.get(symbol)

    def set(self, symbol: str, change: float, notified_at: datetime,
            cooldown_until: Optional[datetime]) -> None:
        self.data[symbol] = {
            "change": change,
            "notified_at": notified_at.isoformat(),
            "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
        }


# --------------------------------------------------------------------------
# Alert decision engine (implements your flowchart)
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

    def evaluate(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Returns a small dict describing the signal if `row` should trigger
        an alert right now, else None. Mutates self.state as a side effect
        on any send. The caller batches these into a single message.
        """
        symbol = row["Symbol"]
        change = row["Change %"]

        # 1. Dead zone -> ignore
        if SELL_THRESHOLD <= change <= BUY_THRESHOLD:
            return None

        signal = "BUY" if change > BUY_THRESHOLD else "SELL"
        now = self._now()
        entry = self.state.get(symbol)

        # 2. 30-minute cooldown -> ignore
        if entry:
            cooldown_until = self._parse_iso(entry.get("cooldown_until"))
            if cooldown_until and now < cooldown_until:
                logger.info("%s in 30-min cooldown until %s, skipping", symbol, cooldown_until)
                return None

        # 3. Expire the 2-hour cache entry if it's aged out
        if entry:
            notified_at = self._parse_iso(entry["notified_at"])
            if now - notified_at >= timedelta(hours=BLOCK_HOURS):
                entry = None  # treated as "not in cache"

        # 4. Not in cache -> first notification
        if entry is None:
            self.state.set(symbol, change, now, cooldown_until=None)
            return {"signal": signal, "row": row, "first": True}

        # 5. In cache -> only re-alert on a >=3% move from what we last sent
        diff = abs(change - entry["change"])
        if diff < RENOTIFY_DIFF_PCT:
            logger.info("%s cached, diff %.2f%% < %.1f%%, skipping", symbol, diff, RENOTIFY_DIFF_PCT)
            return None

        cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
        self.state.set(symbol, change, now, cooldown_until=cooldown_until)
        return {"signal": signal, "row": row, "first": False}

    @staticmethod
    def format_batch(alerts: List[Dict[str, Any]]) -> str:
        """Combine any number of alerts from one poll into a single message."""
        lines = [f"*Signals* ({len(alerts)}) — {datetime.now(tz=IST_ZONE).strftime('%H:%M:%S')} IST"]
        for a in alerts:
            row = a["row"]
            tag = "🟢 BUY" if a["signal"] == "BUY" else "🔴 SELL"
            kind = "new" if a["first"] else "re-alert ≥3%"
            lines.append(
                f"{tag} `{row['Symbol']}` ({kind}) — "
                f"LTP {row['LTP']}, Chg {row['Change %']}%, Val ₹{row['Value (Cr)']}Cr"
            )
        return "\n".join(lines)


# --------------------------------------------------------------------------
# Entry point - loops every 12-22s until market close (15:30 IST),
# batching every signal from a single poll into one Telegram message.
# --------------------------------------------------------------------------
def run_until_close() -> None:
    monitor = NSEMarketMonitor()
    monitor._warmup()

    state = AlertState()
    engine = AlertEngine(state)

    poll_count = 0
    while datetime.now(tz=IST_ZONE).time() < MARKET_CLOSE_IST:
        poll_count += 1
        try:
            stocks = monitor.get_all_stocks()
            if stocks:
                rows = monitor.build_table_rows(stocks)

                alerts = []
                for row in rows:
                    result = engine.evaluate(row)
                    if result:
                        alerts.append(result)

                if alerts:
                    message = engine.format_batch(alerts)
                    if send_telegram_message(message):
                        logger.info("Sent batched alert for %s symbol(s): %s",
                                    len(alerts), [a["row"]["Symbol"] for a in alerts])

                state.save()
            else:
                logger.info("No stocks returned this poll; nothing to check.")

        except Exception as e:
            # Keep the loop alive across transient NSE/network hiccups.
            logger.error("Poll #%s failed: %s", poll_count, e)

        sleep_for = random.uniform(POLL_MIN_SECONDS, POLL_MAX_SECONDS)
        time.sleep(sleep_for)

    logger.info("Market closed (15:30 IST). Loop ending after %s poll(s).", poll_count)


if __name__ == "__main__":
    run_until_close()
