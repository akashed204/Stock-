from __future__ import annotations

import argparse
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pytz
from pya3 import Aliceblue
from tabulate import tabulate


IST = pytz.timezone("Asia/Kolkata")
DATA_DIR = Path(__file__).resolve().parent / "data"
MAX_WORKERS = 5
API_RETRIES = 3
API_DELAY_SECONDS = 0.1
RETRY_DELAY_SECONDS = 1.0


@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class ScanResult:
    symbol: str
    ib_high: float
    ib_low: float
    ib_range: float
    atr: float
    ib_type: str


@dataclass
class ScanStatus:
    symbol: str
    status: str
    detail: str
    source: str = "Alice Blue"


@dataclass
class DailySymbolData:
    candles: List[Candle]
    atr: Optional[float]


def parse_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return IST.localize(value)
        return value.astimezone(IST)

    if value is None:
        return None

    text = str(value).strip()
    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d",
    )

    for fmt in formats:
        try:
            return IST.localize(datetime.strptime(text[:19], fmt))
        except ValueError:
            continue

    try:
        epoch_value = float(text)
        if epoch_value > 10_000_000_000:
            epoch_value = epoch_value / 1000
        return datetime.fromtimestamp(epoch_value, tz=IST)
    except (ValueError, OSError):
        return None


def parse_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_candle(row: Any) -> Optional[Candle]:
    if isinstance(row, dict):
        keys = {str(key).lower(): key for key in row.keys()}

        def get(*names: str) -> Any:
            for name in names:
                key = keys.get(name.lower())
                if key is not None:
                    return row[key]
            return None

        ts = parse_timestamp(get("date", "datetime", "timestamp", "time"))
        open_price = parse_float(get("open", "o"))
        high = parse_float(get("high", "h"))
        low = parse_float(get("low", "l"))
        close = parse_float(get("close", "c"))
    elif isinstance(row, (list, tuple)) and len(row) >= 5:
        ts = parse_timestamp(row[0])
        open_price = parse_float(row[1])
        high = parse_float(row[2])
        low = parse_float(row[3])
        close = parse_float(row[4])
    else:
        return None

    if ts is None or open_price is None or high is None or low is None or close is None:
        return None
    if high < low:
        return None

    return Candle(timestamp=ts, open=open_price, high=high, low=low, close=close)


def symbol_from_csv_path(path: Path) -> str:
    name = path.stem.upper()
    if name.endswith(".NS"):
        name = name[:-3]
    return name


def load_csv_data(data_dir: Path = DATA_DIR) -> Dict[str, List[Candle]]:
    """Load daily OHLC CSV files from data_dir as SYMBOL -> candles."""
    candles_by_symbol: Dict[str, List[Candle]] = {}

    if not data_dir.exists():
        print(f"[ERROR] CSV folder missing: {data_dir}")
        return candles_by_symbol

    for path in sorted(data_dir.glob("*.csv")):
        symbol = symbol_from_csv_path(path)
        rows: List[Candle] = []

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    candle = normalize_candle(row)
                    if candle is not None:
                        rows.append(candle)
        except FileNotFoundError:
            print(f"[WARN] {symbol}: CSV missing, skipped")
            continue
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] {symbol}: CSV read failed: {exc}")
            continue

        rows = sorted(rows, key=lambda candle: candle.timestamp)
        if len(rows) < 15:
            print(f"[WARN] {symbol}: not enough daily rows for ATR, skipped")
            continue

        candles_by_symbol[symbol] = rows

    return candles_by_symbol


def build_daily_symbol_data(daily_data: Dict[str, List[Candle]]) -> Dict[str, DailySymbolData]:
    """Attach precomputed ATR to each symbol so dashboard scans avoid recalculating it."""
    return {
        symbol: DailySymbolData(candles=candles, atr=compute_atr(candles))
        for symbol, candles in daily_data.items()
    }


def compute_atr(daily_candles: Sequence[Candle], period: int = 14) -> Optional[float]:
    """Calculate ATR(14) using Wilder's RMA method from daily CSV candles."""
    if len(daily_candles) < period + 1:
        return None

    candles = sorted(daily_candles, key=lambda candle: candle.timestamp)
    true_ranges: List[float] = []

    for index in range(1, len(candles)):
        current = candles[index]
        previous = candles[index - 1]
        tr = max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        )
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    atr = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr = ((atr * (period - 1)) + tr) / period

    return atr


def create_client(username: str, api_key: str) -> Aliceblue:
    return Aliceblue(user_id=username, api_key=api_key)


def session_is_valid(client: Aliceblue) -> bool:
    try:
        time.sleep(API_DELAY_SECONDS)
        instrument = client.get_instrument_by_symbol("NSE", "SBIN")
        return instrument is not None
    except JSONDecodeError:
        print("API returned empty response")
        return False
    except Exception as exc:  # noqa: BLE001
        if "JSONDecodeError" in type(exc).__name__:
            print("API returned empty response")
        else:
            print(f"[ERROR] Alice Blue session check failed: {exc}")
        return False


def extract_historical_rows(response: Any) -> List[Any]:
    if isinstance(response, dict):
        status = str(response.get("status") or response.get("stat") or "").lower()
        if status and status not in {"success", "ok", "true"}:
            return []
        data = response.get("data") or response.get("candles") or response.get("result") or []
        return data if isinstance(data, list) else []

    if isinstance(response, list):
        return response

    if hasattr(response, "to_dict"):
        try:
            return response.to_dict("records")
        except Exception:
            return []

    return []


def fetch_intraday_data(
    client: Aliceblue,
    symbol: str,
    start: datetime,
    end: datetime,
    instrument_cache: Optional[Dict[str, Any]] = None,
) -> List[Candle]:
    """Fetch today's 1-minute candles from Alice Blue only."""
    try:
        instrument = instrument_cache.get(symbol) if instrument_cache is not None else None
        if instrument is None:
            time.sleep(API_DELAY_SECONDS)
            instrument = client.get_instrument_by_symbol("NSE", symbol)
            if instrument_cache is not None and instrument is not None:
                instrument_cache[symbol] = instrument
    except JSONDecodeError:
        print("API returned empty response")
        return []
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] {symbol}: instrument lookup failed: {exc}")
        return []

    if instrument is None:
        print(f"[WARN] {symbol}: instrument not found")
        return []

    response: Any = None
    for attempt in range(1, API_RETRIES + 1):
        try:
            time.sleep(API_DELAY_SECONDS)
            response = client.get_historical(instrument, start, end, "1")
            break
        except JSONDecodeError:
            print("API returned empty response")
        except Exception as exc:  # noqa: BLE001
            if "JSONDecodeError" in type(exc).__name__:
                print("API returned empty response")
            else:
                print(f"[WARN] {symbol}: intraday fetch failed attempt {attempt}/{API_RETRIES}: {exc}")

        if attempt < API_RETRIES:
            time.sleep(RETRY_DELAY_SECONDS)
    else:
        return []

    candles = []
    for row in extract_historical_rows(response):
        candle = normalize_candle(row)
        if candle is not None:
            candles.append(candle)

    return sorted(candles, key=lambda candle: candle.timestamp)


def fetch_yahoo_intraday_data(symbol: str, start: datetime, end: datetime) -> List[Candle]:
    """Optional fallback source for today's 1-minute NSE candles."""
    try:
        import yfinance as yf
    except ImportError:
        return []

    ticker = f"{symbol.upper()}.NS"
    try:
        time.sleep(API_DELAY_SECONDS)
        data = yf.download(
            ticker,
            period="1d",
            interval="1m",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception:
        return []

    if data is None or data.empty:
        return []

    if hasattr(data.columns, "nlevels") and data.columns.nlevels > 1:
        data.columns = [column[0] for column in data.columns]

    candles: List[Candle] = []
    for timestamp, row in data.iterrows():
        candle = normalize_candle(
            {
                "datetime": timestamp,
                "open": row.get("Open"),
                "high": row.get("High"),
                "low": row.get("Low"),
                "close": row.get("Close"),
            }
        )
        if candle is not None and start <= candle.timestamp < end:
            candles.append(candle)

    return sorted(candles, key=lambda candle: candle.timestamp)


def compute_ib(
    intraday_candles: Sequence[Candle],
    market_open: datetime,
    ib_end: datetime,
) -> Optional[Tuple[float, float, float]]:
    ib_candles = [
        candle
        for candle in intraday_candles
        if market_open <= candle.timestamp < ib_end
    ]

    if not ib_candles:
        return None

    ib_high = max(candle.high for candle in ib_candles)
    ib_low = min(candle.low for candle in ib_candles)
    ib_range = ib_high - ib_low
    return ib_high, ib_low, ib_range


def classify_ib(ib_range: float, atr: float) -> str:
    if ib_range < 0.5 * atr:
        return "Small IB"
    if ib_range <= 1.5 * atr:
        return "Normal IB"
    return "Wide IB"


def today_session_times() -> Tuple[datetime, datetime, datetime]:
    now = datetime.now(IST)
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    ib_end = market_open + timedelta(hours=1)
    return now, market_open, ib_end


def scan_symbol(
    client: Aliceblue,
    symbol: str,
    daily_candles: Sequence[Candle],
    market_open: datetime,
    ib_end: datetime,
    now: datetime,
) -> Optional[ScanResult]:
    atr = compute_atr(daily_candles)
    if atr is None or atr <= 0:
        print(f"[WARN] {symbol}: ATR unavailable")
        return None

    intraday = fetch_intraday_data(client, symbol, market_open, now)
    if not intraday:
        print(f"[WARN] {symbol}: no intraday data, skipped")
        return None

    ib = compute_ib(intraday, market_open, ib_end)
    if ib is None:
        print(f"[WARN] {symbol}: IB candles missing, skipped")
        return None

    ib_high, ib_low, ib_range = ib
    return ScanResult(
        symbol=symbol,
        ib_high=ib_high,
        ib_low=ib_low,
        ib_range=ib_range,
        atr=atr,
        ib_type=classify_ib(ib_range, atr),
    )


def scan_symbol_detailed(
    client: Aliceblue,
    symbol: str,
    daily_candles: Sequence[Candle],
    market_open: datetime,
    ib_end: datetime,
    now: datetime,
    use_yahoo_fallback: bool = False,
    use_alice: bool = True,
    atr: Optional[float] = None,
    instrument_cache: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[ScanResult], ScanStatus]:
    atr = atr if atr is not None else compute_atr(daily_candles)
    if atr is None or atr <= 0:
        return None, ScanStatus(symbol, "Skipped", "ATR unavailable")

    intraday: List[Candle] = []
    source = "Alice Blue" if use_alice else "Yahoo fallback"

    if use_alice:
        intraday = fetch_intraday_data(client, symbol, market_open, now, instrument_cache)

    if not intraday and use_yahoo_fallback:
        intraday = fetch_yahoo_intraday_data(symbol, market_open, now)
        source = "Yahoo fallback" if (intraday or not use_alice) else "Alice Blue"

    if not intraday:
        return None, ScanStatus(symbol, "Skipped", "No intraday candles returned", source)

    ib = compute_ib(intraday, market_open, ib_end)
    if ib is None:
        return None, ScanStatus(
            symbol,
            "Skipped",
            f"Intraday candles found ({len(intraday)}), but no candles inside 09:15-10:15",
            source,
        )

    ib_high, ib_low, ib_range = ib
    result = ScanResult(
        symbol=symbol,
        ib_high=ib_high,
        ib_low=ib_low,
        ib_range=ib_range,
        atr=atr,
        ib_type=classify_ib(ib_range, atr),
    )
    return result, ScanStatus(symbol, "OK", f"{len(intraday)} intraday candles parsed", source)


def run_scan(client: Aliceblue, daily_data: Dict[str, List[Candle]]) -> List[ScanResult]:
    now, market_open, ib_end = today_session_times()

    if now < ib_end:
        print(
            "Run scanner after 10:15 AM IST. "
            f"Current IST: {now.strftime('%H:%M:%S')}"
        )
        return []

    results: List[ScanResult] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(client_scan, client, symbol, candles, market_open, ib_end, now): symbol
            for symbol, candles in daily_data.items()
        }

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)

    return sorted(results, key=lambda item: item.ib_range / item.atr)


def client_scan(
    client: Aliceblue,
    symbol: str,
    candles: Sequence[Candle],
    market_open: datetime,
    ib_end: datetime,
    now: datetime,
) -> Optional[ScanResult]:
    try:
        return scan_symbol(client, symbol, candles, market_open, ib_end, now)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] {symbol}: scan failed: {exc}")
        return None


def print_results(results: Sequence[ScanResult]) -> None:
    if not results:
        print("No valid scan results.")
        return

    rows = [
        [
            result.symbol,
            f"{result.ib_high:.2f}",
            f"{result.ib_low:.2f}",
            f"{result.ib_range:.2f}",
            f"{result.atr:.2f}",
            result.ib_type,
        ]
        for result in results
    ]

    print(
        tabulate(
            rows,
            headers=["Symbol", "IB High", "IB Low", "IB Range", "ATR", "IB Type"],
            tablefmt="github",
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NSE IB scanner using CSV ATR and Alice Blue intraday data")
    parser.add_argument("--username", required=True, help="Alice Blue username")
    parser.add_argument("--api-key", required=True, help="Alice Blue API key")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Folder containing daily CSV files")
    parser.add_argument("--symbols", nargs="*", default=None, help="Optional NSE symbols to scan")
    parser.add_argument("--refresh", action="store_true", help="Refresh every 5 minutes")
    return parser.parse_args()


def filter_symbols(
    daily_data: Dict[str, List[Candle]],
    symbols: Optional[Iterable[str]],
) -> Dict[str, List[Candle]]:
    if not symbols:
        return daily_data

    wanted = {symbol.upper().replace(".NS", "") for symbol in symbols}
    return {symbol: candles for symbol, candles in daily_data.items() if symbol in wanted}


def main() -> None:
    args = parse_args()
    client = create_client(args.username, args.api_key)

    if not session_is_valid(client):
        print("Scan stopped: Alice Blue API session is invalid.")
        return

    while True:
        daily_data = load_csv_data(Path(args.data_dir))
        daily_data = filter_symbols(daily_data, args.symbols)

        if not daily_data:
            print("No daily CSV data found.")
            return

        results = run_scan(client, daily_data)
        print_results(results)

        if not args.refresh:
            break

        print("Refreshing again in 5 minutes...")
        time.sleep(300)


if __name__ == "__main__":
    main()
