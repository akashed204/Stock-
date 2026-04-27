"""
Initial Balance (IB) Scanner using Alice Blue API (pya3)

What this script does:
1. Scans NSE symbols (default: NIFTY 50 list below).
2. Fetches today's 1-minute intraday candles.
3. Fetches daily candles for ATR (last 14 True Ranges).
4. Calculates:
   - IB High: highest high of first 60 minutes of current session.
   - IB Low: lowest low of first 60 minutes of current session.
   - IB Range: IB High - IB Low.
   - ATR(14): average of last 14 daily True Range values.
5. Classifies IB:
   - Small IB: range < 0.5 * ATR
   - Normal IB: 0.5 * ATR <= range <= 1.5 * ATR
   - Wide IB: range > 1.5 * ATR
6. Prints table: Symbol | IB High | IB Low | IB Range | ATR | Range/ATR | IB Type.
7. Optional CSV export and highlighting of Small IB rows in console output.

Install:
    pip install pya3 tabulate pytz

Usage example:
    python ib_scanner.py --username YOUR_ALICE_USERNAME --api-key YOUR_API_KEY --export-csv ib_results.csv

Notes:
- Ensure your API credentials/session are valid for pya3.
- pya3 method signatures can vary across versions; adjust `create_client` and
  historical fetch calls if your installed version differs.
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from pya3 import Aliceblue
from tabulate import tabulate


# Default NIFTY 50 symbols (NSE cash symbols commonly used with brokers)
DEFAULT_SYMBOLS: List[str] = [
    "ADANIENT",
    "ADANIPORTS",
    "APOLLOHOSP",
    "ASIANPAINT",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJAJFINSV",
    "BAJFINANCE",
    "BEL",
    "BHARTIARTL",
    "BPCL",
    "BRITANNIA",
    "CIPLA",
    "COALINDIA",
    "DRREDDY",
    "EICHERMOT",
    "ETERNAL",
    "GRASIM",
    "HCLTECH",
    "HDFCBANK",
    "HDFCLIFE",
    "HEROMOTOCO",
    "HINDALCO",
    "HINDUNILVR",
    "ICICIBANK",
    "INDUSINDBK",
    "INFY",
    "ITC",
    "JIOFIN",
    "JSWSTEEL",
    "KOTAKBANK",
    "LT",
    "M&M",
    "MARUTI",
    "NESTLEIND",
    "NTPC",
    "ONGC",
    "POWERGRID",
    "RELIANCE",
    "SBILIFE",
    "SBIN",
    "SHRIRAMFIN",
    "SUNPHARMA",
    "TATACONSUM",
    "TATAMOTORS",
    "TATASTEEL",
    "TCS",
    "TECHM",
    "TITAN",
    "TRENT",
    "ULTRACEMCO",
    "WIPRO",
]


# ANSI colors for highlighting Small IB rows in terminal
ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_YELLOW = "\033[93m"

IST = pytz.timezone("Asia/Kolkata")


@dataclass
class IBResult:
    symbol: str
    ib_high: float
    ib_low: float
    ib_range: float
    atr: float
    ib_type: str


def ist_now() -> datetime:
    return datetime.now(IST)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan NSE stocks for Initial Balance classification")
    parser.add_argument("--username", required=True, help="Alice Blue username")
    parser.add_argument("--api-key", required=True, help="Alice Blue API key")
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=DEFAULT_SYMBOLS,
        help="Space-separated NSE symbols. Default: NIFTY 50 list",
    )
    parser.add_argument(
        "--export-csv",
        default=None,
        help="Optional CSV file path to export results",
    )
    return parser.parse_args()


def create_client(username: str, api_key: str) -> Aliceblue:
    # Common pya3 login signature.
    # If your version differs, adapt this method accordingly.
    return Aliceblue(user_id=username, api_key=api_key)


def get_session_times() -> Tuple[datetime, datetime, datetime, datetime]:
    now = ist_now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    ib_end = market_open + timedelta(minutes=60)

    # Daily data window: enough days to safely derive 14 valid TRs
    daily_start = (now - timedelta(days=60)).replace(hour=0, minute=0, second=0, microsecond=0)
    daily_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return market_open, ib_end, daily_start, daily_end


def parse_candle(candle: Any) -> Optional[Dict[str, Any]]:
    """
    Normalize pya3 candle formats into a dict with keys:
    timestamp, open, high, low, close
    """
    if isinstance(candle, dict):
        keys = {k.lower(): k for k in candle.keys()}

        def g(*candidates: str) -> Optional[Any]:
            for c in candidates:
                if c in keys:
                    return candle[keys[c]]
            return None

        ts = g("time", "timestamp", "datetime")
        o = g("open", "o")
        h = g("high", "h")
        l = g("low", "l")
        c = g("close", "c")
    elif isinstance(candle, (list, tuple)) and len(candle) >= 5:
        # Typical format: [time, open, high, low, close, volume]
        ts, o, h, l, c = candle[0], candle[1], candle[2], candle[3], candle[4]
    else:
        return None

    try:
        return {
            "timestamp": ts,
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
        }
    except (TypeError, ValueError):
        return None


def parse_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return IST.localize(value)
        return value.astimezone(IST)
    if value is None:
        return None

    s = str(value).strip()
    # Try common broker/API time formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return IST.localize(datetime.strptime(s, fmt))
        except ValueError:
            continue

    # Fallback for epoch seconds
    try:
        return datetime.fromtimestamp(float(s), tz=IST)
    except (ValueError, OSError):
        return None


def safe_get_historical(
    client: Aliceblue,
    instrument: Any,
    start: datetime,
    end: datetime,
    interval: str,
) -> List[Dict[str, Any]]:
    """
    Fetch candles and normalize response across possible pya3 variants.
    """
    # Most common pya3 method signature
    # Many clients expect naive datetime values for market local wall-clock.
    api_start = start.replace(tzinfo=None) if isinstance(start, datetime) and start.tzinfo else start
    api_end = end.replace(tzinfo=None) if isinstance(end, datetime) and end.tzinfo else end

    response = client.get_historical(
        instrument,
        api_start,
        api_end,
        interval,
    )

    if isinstance(response, dict):
        # Some versions return {'status': 'success', 'data': [...]} etc.
        data = response.get("data", [])
    elif isinstance(response, list):
        data = response
    else:
        data = []

    parsed: List[Dict[str, Any]] = []
    for row in data:
        c = parse_candle(row)
        if c is not None:
            parsed.append(c)
    return parsed


def compute_ib_range(
    intraday_candles: Sequence[Dict[str, Any]],
    market_open: datetime,
    ib_end: datetime,
) -> Optional[Tuple[float, float, float]]:
    ib_highs: List[float] = []
    ib_lows: List[float] = []

    for c in intraday_candles:
        ts = parse_timestamp(c.get("timestamp"))
        if ts is None:
            continue
        if market_open <= ts < ib_end:
            ib_highs.append(c["high"])
            ib_lows.append(c["low"])

    if not ib_highs or not ib_lows:
        return None

    ib_high = max(ib_highs)
    ib_low = min(ib_lows)
    ib_range = ib_high - ib_low
    return ib_range, ib_high, ib_low


def compute_atr_14(daily_candles: Sequence[Dict[str, Any]], symbol: str = "UNKNOWN") -> Optional[float]:
    """
    TradingView-style ATR(14) using Wilder's RMA method.

    Rules:
    - Keep valid daily candles only.
    - Deduplicate by date (last seen candle wins).
    - Sort candles ascending by timestamp.
    - Use a larger recent window (last 50 candles) for smoothing continuity.
    - Return None if required data is missing or TR count is insufficient.
    """
    if not daily_candles:
        return None

    # Keep only valid candles, deduplicate by trading date (last seen wins).
    by_date: Dict[Any, Dict[str, Any]] = {}

    for candle in daily_candles:
        if not isinstance(candle, dict):
            continue

        ts = parse_timestamp(candle.get("timestamp"))
        if ts is None:
            continue

        try:
            o = float(candle.get("open"))
            h = float(candle.get("high"))
            l = float(candle.get("low"))
            c = float(candle.get("close"))
        except (TypeError, ValueError):
            continue

        if h < l:
            continue

        by_date[ts.date()] = {
            "timestamp": ts,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
        }

    candles = sorted(by_date.values(), key=lambda x: x["timestamp"])

    if len(candles) < 15:
        return None

    # Use a larger lookback window for Wilder smoothing continuity.
    recent_candles = candles[-50:]

    tr_values: List[float] = []
    for i in range(1, len(recent_candles)):
        today = recent_candles[i]
        prev = recent_candles[i - 1]

        high = today.get("high")
        low = today.get("low")
        prev_close = prev.get("close")

        if high is None or low is None or prev_close is None:
            return None

        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_values.append(tr)

    if len(tr_values) < 14:
        return None

    # Step 1: initial ATR from first 14 TR values.
    atr = sum(tr_values[:14]) / 14.0

    # Step 2: Wilder RMA smoothing for remaining TR values.
    for tr in tr_values[14:]:
        atr = ((atr * 13.0) + tr) / 14.0

    return atr


def classify_ib(ib_range: Optional[float], atr: Optional[float]) -> Optional[str]:
    if ib_range is None:
        return None
    if atr is None or atr <= 0:
        return None

    if ib_range < 0.5 * atr:
        return "Small IB"
    if ib_range <= 1.5 * atr:
        return "Normal IB"
    return "Wide IB"


def scan_symbol(
    client: Aliceblue,
    symbol: str,
    market_open: datetime,
    ib_end: datetime,
    daily_start: datetime,
    daily_end: datetime,
) -> Optional[IBResult]:
    try:
        current_time = ist_now()
        if current_time < ib_end:
            return None

        instrument = client.get_instrument_by_symbol("NSE", symbol)
        intraday = safe_get_historical(client, instrument, market_open, current_time, "1")
        daily = safe_get_historical(client, instrument, daily_start, daily_end, "D")

        if not intraday or not daily:
            return None

        ib_data = compute_ib_range(intraday, market_open, ib_end)
        atr = compute_atr_14(daily)

        if ib_data is None:
            return None

        ib_range, ib_high, ib_low = ib_data
        ib_type = classify_ib(ib_range, atr)
        if ib_type is None:
            return None

        return IBResult(
            symbol=symbol,
            ib_high=ib_high,
            ib_low=ib_low,
            ib_range=ib_range,
            atr=atr,
            ib_type=ib_type,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] {symbol}: {exc}")
        return None


def print_results(results: Sequence[IBResult], highlight_small: bool = True) -> None:
    if not results:
        print("No valid results generated.")
        return

    table_rows: List[List[str]] = []
    for r in results:
        ratio = r.ib_range / r.atr if r.atr else 0.0
        row = [
            r.symbol,
            f"{r.ib_high:.2f}",
            f"{r.ib_low:.2f}",
            f"{r.ib_range:.2f}",
            f"{r.atr:.2f}",
            f"{ratio:.3f}",
            r.ib_type,
        ]
        if highlight_small and r.ib_type == "Small IB":
            row = [f"{ANSI_BOLD}{ANSI_YELLOW}{cell}{ANSI_RESET}" for cell in row]
        table_rows.append(row)

    print(
        tabulate(
            table_rows,
            headers=["Symbol", "IB High", "IB Low", "IB Range", "ATR", "Range/ATR", "IB Type"],
            tablefmt="github",
        )
    )


def export_to_csv(path: str, results: Sequence[IBResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Symbol", "IB High", "IB Low", "IB Range", "ATR", "Range/ATR", "IB Type"])
        for r in results:
            ratio = r.ib_range / r.atr if r.atr else 0.0
            writer.writerow(
                [
                    r.symbol,
                    f"{r.ib_high:.6f}",
                    f"{r.ib_low:.6f}",
                    f"{r.ib_range:.6f}",
                    f"{r.atr:.6f}",
                    f"{ratio:.6f}",
                    r.ib_type,
                ]
            )


def main() -> None:
    args = parse_args()
    client = create_client(args.username, args.api_key)

    market_open, ib_end, daily_start, daily_end = get_session_times()
    current_time = ist_now()

    if current_time < ib_end:
        print(
            "IB window not complete yet. "
            f"Current IST: {current_time.strftime('%H:%M:%S')} | "
            f"IB end IST: {ib_end.strftime('%H:%M:%S')}"
        )
        return

    print("Scanning symbols...")
    results: List[IBResult] = []

    with ThreadPoolExecutor(max_workers=min(16, max(4, len(args.symbols)))) as executor:
        futures = {
            executor.submit(
                scan_symbol,
                client,
                symbol,
                market_open,
                ib_end,
                daily_start,
                daily_end,
            ): symbol
            for symbol in args.symbols
        }

        for future in as_completed(futures):
            out = future.result()
            if out is not None:
                results.append(out)

    # Sort by IB range relative to ATR (smallest first)
    results.sort(key=lambda x: x.ib_range / x.atr)
    print_results(results, highlight_small=True)

    if args.export_csv:
        export_to_csv(args.export_csv, results)
        print(f"CSV exported to: {args.export_csv}")


if __name__ == "__main__":
    main()
