import time, os
import json
import logging
from pathlib import Path
from datetime import date
from typing import Any, Dict, List, Set

import requests
import schedule


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("NSE_Monitor")


class NSEMarketMonitor:
    BASE_URL = "https://www.nseindia.com"
    REFERER_URL = "https://www.nseindia.com/market-data/most-active-equities"
    API_URL = "https://www.nseindia.com/api/live-analysis-most-active-securities"
    STATE_FILE = Path("nse_monitor_state.json")

    TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

    POLL_MINUTES = 5
    RANK_BY = "value"  # "value" or "volume"

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
        if not self.TELEGRAM_BOT_TOKEN or not self.TELEGRAM_CHAT_ID:
            raise ValueError("Telegram credentials missing in code.")

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

        self.state = self._load_state()
        self._normalize_state()

        logger.info(
            "Initialized. rank_by=%s poll_minutes=%s exclude_symbols=%s",
            self.RANK_BY,
            self.POLL_MINUTES,
            sorted(self.exclude_symbols)
        )

    def _load_state(self) -> Dict[str, Any]:
        if not self.STATE_FILE.exists():
            return {}
        try:
            with self.STATE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load state file: {e}")
            return {}

    def _save_state(self) -> None:
        try:
            with self.STATE_FILE.open("w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")

    def _today_str(self) -> str:
        return date.today().isoformat()

    def _normalize_state(self) -> None:
        today = self._today_str()
        if self.state.get("date") != today:
            self.state = {"date": today, "sent_symbols": []}
            self._save_state()
            return
        if not isinstance(self.state.get("sent_symbols", []), list):
            self.state["sent_symbols"] = []
            self._save_state()

    def _get_sent_symbols_today(self) -> Set[str]:
        self._normalize_state()
        return {str(x).upper() for x in self.state.get("sent_symbols", [])}

    def _mark_symbols_sent(self, symbols: List[str]) -> None:
        sent = self._get_sent_symbols_today()
        for symbol in symbols:
            sent.add(symbol.upper())
        self.state["sent_symbols"] = sorted(sent)
        self._save_state()

    def _warmup(self) -> bool:
        for url in (self.REFERER_URL, self.BASE_URL):
            try:
                resp = self.session.get(url, timeout=15)
                if resp.ok:
                    logger.info(f"Warmup successful via {url}")
                    return True
                logger.warning(f"Warmup got HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Warmup failed for {url}: {e}")
        return False

    def _fetch_rows(self) -> List[Dict[str, Any]]:
        params = {"index": self.RANK_BY}

        try:
            resp = self.session.get(self.API_URL, params=params, timeout=15)

            if resp.status_code in (401, 403):
                logger.warning(f"API returned {resp.status_code}. Retrying after warmup.")
                self._warmup()
                time.sleep(1.5)
                resp = self.session.get(self.API_URL, params=params, timeout=15)

            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", [])

            if not isinstance(rows, list):
                raise ValueError("Unexpected API response: 'data' is not a list.")

            logger.info("Fetched %s rows from NSE API.", len(rows))
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
        logger.info("Already sent today: %s", sorted(sent_today))

        eligible = []
        for row in filtered:
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            if symbol in sent_today:
                logger.info("Skipping %s because it was already sent today.", symbol)
                continue
            eligible.append(row)

        logger.info("Eligible unsent symbols count: %s", len(eligible))
        return eligible

    def _format_value_in_crore(self, total_traded_value: Any) -> str:
        value = self._safe_float(total_traded_value)
        return f"{value / 10000000:,.2f} Cr"

    def build_message(self, stocks: List[Dict[str, Any]]) -> str:
        lines = [f"Most Active NSE Stocks ({self.RANK_BY.upper()})"]

        for stock in stocks:
            symbol = str(stock.get("symbol", "N/A")).upper()
            last_price = self._safe_float(stock.get("lastPrice"))
            pchange = self._safe_float(stock.get("pChange"))
            traded_value = self._format_value_in_crore(stock.get("totalTradedValue"))

            lines.append(
                f"Symbol: {symbol}  lastPrice: {last_price:,.2f}\n"
                f"Change: {pchange:.2f}%, totalTradedValue: {traded_value}"
            )

        return "\n\n".join(lines)

    def send_telegram(self, message: str) -> bool:
        url = f"https://api.telegram.org/bot{self.TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": self.TELEGRAM_CHAT_ID,
            "text": message
        }

        try:
            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
            logger.info("Telegram message sent successfully.")
            return True
        except requests.RequestException as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    def execute_job(self) -> None:
        logger.info("Starting job.")

        try:
            stocks_to_send = self.get_eligible_stocks()
            if not stocks_to_send:
                logger.info("No eligible stocks found to send.")
                return

            message = self.build_message(stocks_to_send)
            symbols_sent = [
                str(stock.get("symbol", "")).upper()
                for stock in stocks_to_send
                if str(stock.get("symbol", "")).strip()
            ]

            if self.send_telegram(message):
                self._mark_symbols_sent(symbols_sent)
                logger.info("Marked %s symbols as sent for today.", len(symbols_sent))

        except Exception as e:
            logger.exception(f"Job failed: {e}")

    def run(self) -> None:
        logger.info("Starting NSE monitor service.")
        self._warmup()
        self.execute_job()
        schedule.every(self.POLL_MINUTES).minutes.do(self.execute_job)

        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    try:
        monitor = NSEMarketMonitor()
        monitor.run()
    except KeyboardInterrupt:
        logger.info("Service terminated by user.")
    except Exception as e:
        logger.critical(f"Service crashed: {e}")
        raise
