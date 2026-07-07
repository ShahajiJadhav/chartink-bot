import logging
import os
import time
from datetime import datetime, time as dt_time
from typing import Any, Dict, List, Set

import requests
from zoneinfo import ZoneInfo


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("NSE_Monitor")


class NSEMarketMonitor:
    BASE_URL = "https://www.nseindia.com"
    REFERER_URL = "https://www.nseindia.com/market-data/most-active-equities"
    API_URL = "https://www.nseindia.com/api/live-analysis-most-active-securities"

    POLL_MINUTES = 10
    RANK_BY = "value"  # "value" or "volume"
    TOP_N = 20

    MARKET_OPEN_IST = dt_time(9, 15)
    MARKET_CLOSE_IST = dt_time(15, 30)
    IST_ZONE = ZoneInfo("Asia/Kolkata")

    EXCLUDE_SYMBOLS = {
        "HDFCBANK",
        "BSE",
        "SBIN",
        "RELIANCE",
        "LIQUIDBEES",
        "ICICIBANK"
    }

    def __init__(self):
        if self.RANK_BY not in {"value", "volume"}:
            raise ValueError("RANK_BY must be 'value' or 'volume'.")

        self.telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        self.telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

        if not self.telegram_bot_token or not self.telegram_chat_id:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID environment variables.")

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

        # In-memory cache replacing JSON state file
        self._cache: Dict[str, Any] = {}
        self._normalize_state()

        logger.info(
            "Initialized rank_by=%s poll_minutes=%s top_n=%s excluded=%s",
            self.RANK_BY,
            self.POLL_MINUTES,
            self.TOP_N,
            sorted(self.exclude_symbols)
        )

    def _now_ist(self) -> datetime:
        return datetime.now(tz=self.IST_ZONE)

    def _today_str(self) -> str:
        return self._now_ist().date().isoformat()

    def _normalize_state(self) -> None:
        """Ensures the cache is strictly bound to the current trading date."""
        today = self._today_str()
        if self._cache.get("date") != today:
            self._cache = {
                "date": today,
                "sent_symbols": set()
            }

    def _get_sent_symbols_today(self) -> Set[str]:
        self._normalize_state()
        return self._cache.get("sent_symbols", set())

    def _mark_symbols_sent(self, symbols: List[str]) -> None:
        self._normalize_state()
        for symbol in symbols:
            if symbol:
                self._cache["sent_symbols"].add(symbol.upper())

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
        primary = "totalTradedValue" if self.RANK_BY == "value" else "totalTradedVolume"
        secondary = "totalTradedVolume" if self.RANK_BY == "value" else "totalTradedValue"

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

    def get_eligible_stocks(self) -> List[Dict[str, Any]]:
        rows = self._fetch_rows()
        if not rows:
            return []

        ranked = self._sort_rows(rows)
        filtered = self._filter_excluded(ranked)

        sent_today = self._get_sent_symbols_today()
        logger.info("Already sent today: %s", sorted(list(sent_today)))

        eligible = []
        for row in filtered:
            symbol = str(row.get("symbol", "")).upper()
            if not symbol or symbol in sent_today:
                continue
            eligible.append(row)

        logger.info("Eligible unsent symbols count: %s", len(eligible))
        return eligible[:self.TOP_N]

    def _format_value_in_crore(self, total_traded_value: Any) -> str:
        value = self._safe_float(total_traded_value)
        return f"{value / 10000000:,.2f} Cr"

    def build_message(self, stocks: List[Dict[str, Any]]) -> str:
        now_ist = self._now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
        lines = [f"Most Active NSE Stocks"]

        for idx, stock in enumerate(stocks, start=1):
            symbol = str(stock.get("symbol", "N/A")).upper()
            last_price = self._safe_float(stock.get("lastPrice"))
            pchange = self._safe_float(stock.get("pChange"))
            traded_value = self._format_value_in_crore(stock.get("totalTradedValue"))
            traded_volume = int(self._safe_float(stock.get("totalTradedVolume")))

            lines.append(
                f"{idx}. {symbol} | Change: {pchange:.2f}%\n"
                f"LTP: {last_price:,.2f} Value: {traded_value}"
            )

        return "\n\n".join(lines)

    def send_telegram(self, message: str) -> bool:
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": message
        }

        try:
            resp = self.session.post(url, json=payload, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                logger.error("Telegram API returned failure: %s", data)
                return False
            logger.info("Telegram message sent successfully")
            return True
        except requests.RequestException as e:
            logger.error("Telegram send failed: %s", e)
            return False

    def _is_weekday_ist(self) -> bool:
        return self._now_ist().weekday() < 5

    def _is_market_open_ist(self) -> bool:
        now_ist = self._now_ist()
        current_time = now_ist.time()
        return (
            self._is_weekday_ist() and
            self.MARKET_OPEN_IST <= current_time < self.MARKET_CLOSE_IST
        )

    def _is_market_closed_ist(self) -> bool:
        current_time = self._now_ist().time()
        return current_time >= self.MARKET_CLOSE_IST

    def run_once(self) -> None:
        logger.info("Starting one-shot NSE monitor job")
        self._warmup()

        stocks_to_send = self.get_eligible_stocks()
        if not stocks_to_send:
            logger.info("No eligible stocks found to send")
            return

        message = self.build_message(stocks_to_send)
        symbols_sent = [
            str(stock.get("symbol", "")).upper()
            for stock in stocks_to_send
            if str(stock.get("symbol", "")).strip()
        ]

        if self.send_telegram(message):
            self._mark_symbols_sent(symbols_sent)
            logger.info("Marked %s symbols as sent for today", len(symbols_sent))

    def run_forever(self) -> None:
        interval_seconds = self.POLL_MINUTES * 60
        logger.info("Starting continuous NSE monitor loop (timezone-safe IST)")

        while True:
            now_ist = self._now_ist()

            if self._is_market_closed_ist():
                logger.info("Current IST time is %s, market closed. Stopping monitor.", now_ist.strftime("%H:%M:%S"))
                break

            if not self._is_weekday_ist():
                logger.info("Today is weekend in IST. Stopping monitor.")
                break

            if now_ist.time() < self.MARKET_OPEN_IST:
                logger.info(
                    "Before market open in IST (%s). Current time: %s. Stopping monitor.",
                    self.MARKET_OPEN_IST.strftime("%H:%M:%S"),
                    now_ist.strftime("%H:%M:%S")
                )
                break

            try:
                self.run_once()
            except KeyboardInterrupt:
                logger.info("Received shutdown signal, exiting monitor loop")
                break
            except Exception as e:
                logger.exception("Unhandled error in monitor loop: %s", e)

            now_ist = self._now_ist()
            if self._is_market_closed_ist():
                logger.info("Market close reached after cycle at %s IST. Stopping monitor.", now_ist.strftime("%H:%M:%S"))
                break

            market_close_dt = datetime.combine(
                now_ist.date(),
                self.MARKET_CLOSE_IST,
                tzinfo=self.IST_ZONE
            )
            seconds_until_close = int((market_close_dt - now_ist).total_seconds())

            if seconds_until_close <= 0:
                logger.info("No time remaining before market close. Exiting.")
                break

            sleep_seconds = min(interval_seconds, seconds_until_close)
            logger.info("Sleeping for %s seconds", sleep_seconds)

            try:
                time.sleep(sleep_seconds)
            except KeyboardInterrupt:
                logger.info("Received shutdown signal during sleep, exiting")
                break


if __name__ == "__main__":
    try:
        monitor = NSEMarketMonitor()
        monitor.run_forever()
    except Exception as e:
        logger.exception("Execution failed: %s", e)
        raise
