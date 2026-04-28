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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from json import JSONDecodeError
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
    atr: float = 0.0
    ib_type: str = ""


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
    parser.add_argument(
        "--daily-csv",
        default=None,
        help="Optional path to a CSV file containing historical daily candles."
    )
    parser.add_argument(
        "--intraday-csv",
        default=None,
        help="Optional path to a CSV file containing intraday 1-minute candles."
    )
    return parser.parse_args()


def create_client(username: str, api_key: str) -> Aliceblue:
    # Common pya3 login signature.
    # If your version differs, adapt this method accordingly.
    return Aliceblue(user_id=username, api_key=api_key)


def is_session_valid(client: Aliceblue) -> bool:
    """Lightweight API sanity check before starting a scan."""
    try:
        get_session_id = getattr(client, "get_session_id", None)
        if callable(get_session_id):
            session_id = get_session_id()
            if not session_id:
                print("[ERROR] Alice Blue session is invalid or expired")
                return False

        time.sleep(0.3)
        instrument = client.get_instrument_by_symbol("NSE", "SBIN")
        if instrument is None:
            print("[ERROR] Alice Blue API session check failed")
            return False
        return True
    except JSONDecodeError:
        print("API returned empty response")
        return False
    except Exception as exc:  # noqa: BLE001
        if "JSONDecodeError" in type(exc).__name__:
            print("API returned empty response")
        else:
            print(f"[ERROR] Alice Blue session check failed: {type(exc).__name__}: {exc}")
        return False


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
    api_start = start
    api_end = end

    response: Any = None
    for attempt in range(1, 4):
        try:
            time.sleep(0.3)
            response = client.get_historical(
                instrument,
                api_start,
                api_end,
                interval,
            )
            break
        except JSONDecodeError:
            print("API returned empty response")
        except Exception as e:
            if "JSONDecodeError" in type(e).__name__:
                print("API returned empty response")
            else:
                print(
                    f"[API ERROR] get_historical failed "
                    f"(attempt {attempt}/3): {type(e).__name__}: {str(e)[:100]}"
                )

        if attempt < 3:
            time.sleep(1)
    else:
        return []

    if isinstance(response, dict):
        # pya3 can return {'stat': 'Not_Ok', ...} on API-side errors.
        status = str(response.get("status") or response.get("stat") or "").lower()
        if status and status not in {"success", "ok", "true"}:
            print(f"[API ERROR] Invalid historical response status: {response.get('status') or response.get('stat')}")
            return []
        data = response.get("data") or response.get("candles") or response.get("result") or []
    elif isinstance(response, list):
        data = response
    elif hasattr(response, "to_dict"):
        try:
            data = response.to_dict("records")
        except Exception as exc:  # noqa: BLE001
            print(f"[API ERROR] Could not parse historical dataframe: {type(exc).__name__}: {exc}")
            return []
    else:
        return []

    if not isinstance(data, list) or not data:
        return []

    parsed: List[Dict[str, Any]] = []
    for row in data:
        c = parse_candle(row)
        if c is not None:
            parsed.append(c)

    if not parsed:
        return []
    return parsed


def load_csv_candles(path: str, key_field: str = "symbol") -> Dict[str, List[Dict[str, Any]]]:
    """
    Load a CSV of candles into a mapping: symbol -> list of candle dicts.

    Expected CSV columns: symbol, timestamp, open, high, low, close, [volume]
    Timestamp parsing uses parse_timestamp.
    """
    mapping: Dict[str, List[Dict[str, Any]]] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = row.get(key_field) or row.get("symbol")
                if not sym:
                    continue
                ts_raw = row.get("timestamp") or row.get("time")
                ts = parse_timestamp(ts_raw)
                if ts is None:
                    continue
                try:
                    o = float(row.get("open"))
                    h = float(row.get("high"))
                    l = float(row.get("low"))
                    c = float(row.get("close"))
                except (TypeError, ValueError):
                    continue

                candle = {"timestamp": ts, "open": o, "high": h, "low": l, "close": c}
                mapping.setdefault(sym.strip().upper(), []).append(candle)
    except FileNotFoundError:
        print(f"[WARN] CSV not found: {path}")
    return mapping


def fetch_yfinance_intraday(symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """
    Fallback 1-minute NSE intraday candles using Yahoo Finance.

    This is only used when Alice Blue historical data returns no usable candles.
    """
    try:
        import yfinance as yf
    except ImportError:
        print("[WARN] yfinance is not installed; fallback intraday source skipped")
        return []

    ticker = f"{symbol.strip().upper()}.NS"
    try:
        time.sleep(0.3)
        df = yf.download(
            ticker,
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] {symbol}: yfinance fallback failed: {type(exc).__name__}: {exc}")
        return []

    if df is None or df.empty:
        return []

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [col[0] for col in df.columns]

    candles: List[Dict[str, Any]] = []
    for ts, row in df.iterrows():
        parsed_ts = parse_timestamp(ts)
        if parsed_ts is None or not (start <= parsed_ts < end):
            continue

        try:
            candles.append(
                {
                    "timestamp": parsed_ts,
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue

    return candles


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


def classify_ib(ib_range: Optional[float]) -> Optional[str]:
    """Classify IB based on range alone (no ATR)."""
    if ib_range is None:
        return None

    # Simple classification: < 50 = Small, 50-200 = Normal, > 200 = Wide
    if ib_range < 50:
        return "Small IB"
    if ib_range <= 200:
        return "Normal IB"
    return "Wide IB"


def scan_symbol(
    client: Aliceblue,
    symbol: str,
    market_open: datetime,
    ib_end: datetime,
    daily_start: datetime,
    daily_end: datetime,
    intraday_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    daily_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Optional[IBResult]:
    try:
        current_time = ist_now()
        if current_time < ib_end:
            return None

        symbol_key = symbol.strip().upper()

        # Step 1: Get instrument
        time.sleep(0.3)
        instrument = client.get_instrument_by_symbol("NSE", symbol)
        if instrument is None:
            print(f"[ERROR] {symbol}: Instrument not found in NSE")
            return None

        # Step 2: Fetch only 1-minute intraday candles from IB window
        if intraday_map is not None:
            intraday = intraday_map.get(symbol_key, [])
        else:
            intraday = safe_get_historical(client, instrument, market_open, ib_end, "1")
            if not intraday:
                print(f"[WARN] {symbol}: Alice Blue returned no intraday candles; trying yfinance fallback")
                intraday = fetch_yfinance_intraday(symbol, market_open, ib_end)

        if not isinstance(intraday, list) or not intraday:
            print(f"[ERROR] {symbol}: No intraday candles returned (time range: {market_open} to {ib_end})")
            return None

        # Step 3: Compute IB range only (no ATR needed)
        ib_data = compute_ib_range(intraday, market_open, ib_end)

        if ib_data is None:
            print(f"[ERROR] {symbol}: Could not compute IB range from {len(intraday)} intraday candles")
            return None

        ib_range, ib_high, ib_low = ib_data

        # Step 4: Classify IB based on range alone
        ib_type = classify_ib(ib_range)
        if ib_type is None:
            print(f"[ERROR] {symbol}: Classification failed for range={ib_range:.2f}")
            return None

        atr = 0.0
        if daily_map is not None:
            daily = daily_map.get(symbol_key, [])
            atr = compute_atr_14(daily, symbol) or 0.0

        print(f"[OK] {symbol}: {ib_type} (range={ib_range:.2f})")
        return IBResult(
            symbol=symbol,
            ib_high=ib_high,
            ib_low=ib_low,
            ib_range=ib_range,
            atr=atr,
            ib_type=ib_type,
        )
    except JSONDecodeError:
        print("API returned empty response")
        return None
    except Exception as exc:  # noqa: BLE001
        if "JSONDecodeError" in type(exc).__name__:
            print("API returned empty response")
            return None
        print(f"[EXCEPTION] {symbol}: {type(exc).__name__}: {exc}")
        import traceback
        traceback.print_exc()
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

    if not is_session_valid(client):
        print("Scan stopped: Alice Blue API session is not valid.")
        return

    intraday_map: Optional[Dict[str, List[Dict[str, Any]]]] = None
    daily_map: Optional[Dict[str, List[Dict[str, Any]]]] = None
    if args.intraday_csv:
        intraday_map = load_csv_candles(args.intraday_csv)
        print(f"Loaded intraday CSV for {len(intraday_map)} symbols")
    if args.daily_csv:
        daily_map = load_csv_candles(args.daily_csv)
        print(f"Loaded daily CSV for {len(daily_map)} symbols")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                scan_symbol,
                client,
                symbol,
                market_open,
                ib_end,
                daily_start,
                daily_end,
                intraday_map,
                daily_map,
            ): symbol
            for symbol in args.symbols
        }

        for future in as_completed(futures):
            out = future.result()
            if out is not None:
                results.append(out)

    results.sort(key=lambda x: x.ib_range)
    print_results(results, highlight_small=True)

    if args.export_csv:
        export_to_csv(args.export_csv, results)
        print(f"CSV exported to: {args.export_csv}")


if __name__ == "__main__":
    main()
