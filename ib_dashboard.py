from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from trading_scanner import (
    DATA_DIR,
    MAX_WORKERS,
    Candle,
    DailySymbolData,
    ScanResult,
    ScanStatus,
    build_daily_symbol_data,
    create_client,
    load_csv_data,
    scan_symbol_detailed,
    session_is_valid,
    today_session_times,
)


st.set_page_config(page_title="IB Scanner", layout="wide")

st.markdown(
    """
    <style>
    .stApp {
        background: var(--background-color);
        color: var(--text-color);
    }
    [data-testid="stSidebar"] {
        background: var(--secondary-background-color);
        border-right: 1px solid rgba(128, 128, 128, 0.22);
    }
    [data-testid="stSidebar"] * {
        color: var(--text-color);
    }
    .main .block-container {
        padding-top: 1.4rem;
        max-width: 1380px;
    }
    .page-title {
        font-size: 30px;
        font-weight: 760;
        letter-spacing: 0;
        margin: 0 0 0.25rem 0;
        color: var(--text-color);
    }
    .page-subtitle {
        font-size: 14px;
        color: rgba(128, 128, 128, 0.95);
        margin: 0 0 1rem 0;
    }
    .metric-panel {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.24);
        border-radius: 8px;
        padding: 16px 18px;
        min-height: 94px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
    }
    .metric-label {
        color: rgba(128, 128, 128, 0.95);
        font-size: 12px;
        font-weight: 650;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 8px;
    }
    .metric-value {
        font-size: 28px;
        font-weight: 780;
        color: var(--text-color);
        line-height: 1;
    }
    .metric-note {
        color: rgba(128, 128, 128, 0.95);
        font-size: 12px;
        margin-top: 8px;
    }
    .status-strip {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.24);
        border-radius: 8px;
        padding: 12px 14px;
        color: var(--text-color);
        font-size: 13px;
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(128, 128, 128, 0.24);
        border-radius: 8px;
        overflow: hidden;
        background: var(--secondary-background-color);
    }
    h1, h2, h3, h4, h5, h6,
    label, p, span, div[data-testid="stMarkdownContainer"] {
        color: var(--text-color);
    }
    div[data-baseweb="input"],
    div[data-baseweb="select"] > div,
    div[data-baseweb="base-input"] {
        background-color: var(--background-color);
        color: var(--text-color);
        border-color: rgba(128, 128, 128, 0.32);
    }
    input {
        color: var(--text-color) !important;
    }
    .stButton > button {
        border-radius: 7px;
        border: 1px solid rgba(128, 128, 128, 0.32);
        font-weight: 650;
        min-height: 38px;
        background: var(--background-color);
        color: var(--text-color);
    }
    div[data-testid="stAlert"] {
        color: var(--text-color);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_optional_secret(name: str, default: str = "") -> str:
    try:
        value = st.secrets[name]
        return str(value) if value is not None else default
    except Exception:
        return default


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".NS", "")


@st.cache_resource(show_spinner=False)
def get_client(username: str, api_key: str):
    return create_client(username, api_key)


@st.cache_data(ttl=60, show_spinner=False)
def cached_daily_data(data_dir: str) -> Dict[str, DailySymbolData]:
    return build_daily_symbol_data(load_csv_data(Path(data_dir)))


@st.cache_resource(show_spinner=False)
def get_instrument_cache() -> Dict[str, object]:
    return {}


def run_dashboard_scan(
    username: str,
    api_key: str,
    daily_data: Dict[str, DailySymbolData],
    selected_symbols: Tuple[str, ...],
    data_source: str,
    scan_limit: int,
) -> Tuple[List[ScanResult], List[ScanStatus], str]:
    now, market_open, ib_end = today_session_times()

    if now < ib_end:
        return [], [], (
            "IB window is not complete yet. "
            f"Current IST {now.strftime('%H:%M:%S')} | IB end {ib_end.strftime('%H:%M:%S')}"
        )

    use_alice = data_source != "Yahoo only"
    use_yahoo_fallback = data_source != "Alice Blue only"
    client = get_client(username, api_key) if use_alice and username and api_key else None
    alice_ok = session_is_valid(client) if client is not None else False

    if use_alice and not alice_ok and not use_yahoo_fallback:
        return [], [], (
            "Alice Blue API returned empty response for today's intraday candles. "
            "Historical ATR data is already loaded from local CSV files."
        )

    scan_note = ""
    if not use_alice:
        scan_note = "Using Yahoo intraday data. Alice Blue is not used for this scan."
    elif not alice_ok and use_yahoo_fallback:
        scan_note = "Alice Blue API returned empty response. Showing Yahoo fallback intraday results."

    wanted = {normalize_symbol(symbol) for symbol in selected_symbols}
    scan_data = {symbol: data for symbol, data in daily_data.items() if symbol in wanted}
    if scan_limit > 0:
        scan_data = dict(list(scan_data.items())[:scan_limit])

    instrument_cache = get_instrument_cache()

    results: List[ScanResult] = []
    statuses: List[ScanStatus] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                scan_symbol_detailed,
                client,
                symbol,
                data.candles,
                market_open,
                ib_end,
                now,
                use_yahoo_fallback,
                alice_ok,
                data.atr,
                instrument_cache,
            ): symbol
            for symbol, data in scan_data.items()
        }

        for future in as_completed(futures):
            result, status = future.result()
            statuses.append(status)
            if result is not None:
                results.append(result)

    results.sort(key=lambda item: item.ib_range / item.atr if item.atr else 0)
    statuses.sort(key=lambda item: (item.status != "OK", item.symbol))
    return results, statuses, scan_note


def build_dataframe(results: Sequence[ScanResult]) -> pd.DataFrame:
    rows = []
    for result in results:
        ratio = result.ib_range / result.atr if result.atr else 0.0
        rows.append(
            {
                "Symbol": result.symbol,
                "IB High": round(result.ib_high, 2),
                "IB Low": round(result.ib_low, 2),
                "IB Range": round(result.ib_range, 2),
                "ATR": round(result.atr, 2),
                "Range / ATR": round(ratio, 3),
                "IB Type": result.ib_type,
            }
        )

    df = pd.DataFrame(
        rows,
        columns=["Symbol", "IB High", "IB Low", "IB Range", "ATR", "Range / ATR", "IB Type"],
    )
    if not df.empty:
        df = df.sort_values(["IB Type", "Range / ATR", "Symbol"]).reset_index(drop=True)
    return df


def build_status_dataframe(statuses: Sequence[ScanStatus]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Symbol": status.symbol,
                "Status": status.status,
                "Source": status.source,
                "Detail": status.detail,
            }
            for status in statuses
        ],
        columns=["Symbol", "Status", "Source", "Detail"],
    )


def style_results(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    colors = {
        "Small IB": "background-color: #dcfce7; color: #14532d;",
        "Normal IB": "background-color: #fef9c3; color: #713f12;",
        "Wide IB": "background-color: #fee2e2; color: #7f1d1d;",
    }

    def row_style(row: pd.Series) -> List[str]:
        style = colors.get(row["IB Type"], "")
        return [style if column == "IB Type" else "" for column in row.index]

    return df.style.apply(row_style, axis=1).format(
        {
            "IB High": "{:.2f}",
            "IB Low": "{:.2f}",
            "IB Range": "{:.2f}",
            "ATR": "{:.2f}",
            "Range / ATR": "{:.3f}",
        }
    )


def metric_panel(label: str, value: str, note: str = "") -> None:
    st.markdown(
        f"""
        <div class="metric-panel">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.markdown('<h1 class="page-title">Initial Balance Scanner</h1>', unsafe_allow_html=True)
st.markdown(
    '<p class="page-subtitle">Historical ATR from local CSV data, today intraday candles from Alice Blue API.</p>',
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Scanner")
    username_default = get_optional_secret("ALICE_USERNAME", "")
    api_key_default = get_optional_secret("ALICE_API_KEY", "")

    username = st.text_input("Alice Blue Username", value=username_default)
    api_key = st.text_input("Alice Blue API Key", type="password", value=api_key_default)
    data_dir = st.text_input("Daily CSV Folder", value=str(DATA_DIR))
    auto_refresh = st.toggle("Refresh every 5 minutes", value=False)
    data_source = st.selectbox(
        "Intraday Source",
        ["Alice Blue only", "Alice Blue first, Yahoo fallback", "Yahoo only"],
        index=0,
    )

    st.divider()

    daily_data = cached_daily_data(data_dir)
    available_symbols = sorted(daily_data.keys())
    default_symbols = available_symbols[: min(20, len(available_symbols))]

    if "selected_symbols" not in st.session_state:
        st.session_state.selected_symbols = default_symbols

    st.caption("Quick select")
    quick_cols = st.columns(2)
    if quick_cols[0].button("First 20", use_container_width=True):
        st.session_state.selected_symbols = available_symbols[: min(20, len(available_symbols))]
    if quick_cols[1].button("First 50", use_container_width=True):
        st.session_state.selected_symbols = available_symbols[: min(50, len(available_symbols))]

    quick_cols = st.columns(2)
    if quick_cols[0].button("All", use_container_width=True):
        st.session_state.selected_symbols = available_symbols
    if quick_cols[1].button("Clear", use_container_width=True):
        st.session_state.selected_symbols = []

    selected_symbols = st.multiselect(
        "Symbols",
        options=available_symbols,
        key="selected_symbols",
    )
    scan_limit = st.number_input(
        "Scan limit (0 = all selected)",
        min_value=0,
        max_value=max(0, len(available_symbols)),
        value=0,
        step=5,
        help="Set to 0 to scan all selected symbols. Use a positive number only if you want to cap scanning.",
    )
    ib_filter = st.selectbox("IB Type", ["All", "Small IB", "Normal IB", "Wide IB"])

    test_connection = st.button("Test Alice Session", use_container_width=True)

if auto_refresh:
    st_autorefresh(interval=5 * 60 * 1000, key="ib_dashboard_refresh")

if test_connection:
    if not username or not api_key:
        st.warning("Enter Alice Blue credentials first.")
    else:
        with st.spinner("Checking Alice Blue session..."):
            ok = session_is_valid(get_client(username, api_key))
        st.success("Alice Blue session is valid.") if ok else st.error("Alice Blue session failed.")

now, market_open, ib_end = today_session_times()
status_cols = st.columns([1.2, 1.2, 1.2, 2.4])
with status_cols[0]:
    metric_panel("CSV Symbols", str(len(available_symbols)), "Loaded from local data folder")
with status_cols[1]:
    scan_note = "all selected" if int(scan_limit) == 0 else f"up to {int(scan_limit)}"
    metric_panel("Selected", str(len(selected_symbols)), f"Scanning {scan_note} at {MAX_WORKERS} workers")
with status_cols[2]:
    metric_panel("IB Window", "09:15-10:15", "NSE cash session")
with status_cols[3]:
    st.markdown(
        f"""
        <div class="status-strip">
            <b>Current IST:</b> {now.strftime('%Y-%m-%d %H:%M:%S')}<br>
            <b>Market open:</b> {market_open.strftime('%H:%M')} &nbsp; 
            <b>IB complete:</b> {ib_end.strftime('%H:%M')} &nbsp;
            <b>Intraday:</b> {data_source}<br>
            <b>Historical CSV folder:</b> {Path(data_dir)}
        </div>
        """,
        unsafe_allow_html=True,
    )

if (not username or not api_key) and data_source != "Yahoo only":
    st.info("Enter Alice Blue username and API key in the sidebar to fetch today's intraday candles.")
    st.stop()

if not available_symbols:
    st.error("No CSV data found. Run download_all.py first.")
    st.stop()

if not selected_symbols:
    st.warning("Select at least one symbol.")
    st.stop()

with st.spinner(f"Scanning IB using {data_source}..."):
    results, scan_statuses, status_message = run_dashboard_scan(
        username,
        api_key,
        daily_data,
        tuple(selected_symbols),
        data_source,
        int(scan_limit),
    )

if status_message:
    st.warning(status_message)

df = build_dataframe(results)
status_df = build_status_dataframe(scan_statuses)
if ib_filter != "All" and not df.empty:
    df = df[df["IB Type"] == ib_filter].reset_index(drop=True)

summary = df["IB Type"].value_counts().to_dict() if not df.empty else {}
metric_cols = st.columns(5)
with metric_cols[0]:
    metric_panel("Results", str(len(df)), "Valid symbols")
with metric_cols[1]:
    metric_panel("Small IB", str(summary.get("Small IB", 0)), "Range < 0.5 x ATR")
with metric_cols[2]:
    metric_panel("Normal IB", str(summary.get("Normal IB", 0)), "0.5 x ATR to 1.5 x ATR")
with metric_cols[3]:
    metric_panel("Wide IB", str(summary.get("Wide IB", 0)), "Range > 1.5 x ATR")
with metric_cols[4]:
    avg_ratio = df["Range / ATR"].mean() if not df.empty else 0
    metric_panel("Avg Range / ATR", f"{avg_ratio:.3f}", "Across visible results")

left, right = st.columns([1, 2])
with left:
    st.subheader("IB Type Mix")
    if df.empty:
        st.info("No results to chart.")
    else:
        chart_df = (
            df["IB Type"]
            .value_counts()
            .rename_axis("IB Type")
            .reset_index(name="Count")
            .set_index("IB Type")
        )
        st.bar_chart(chart_df, use_container_width=True)

with right:
    st.subheader("Scanner Results")
    if df.empty:
        st.info("No valid results generated. Empty API responses and missing CSVs are skipped.")
    else:
        st.dataframe(
            style_results(df),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Range / ATR": st.column_config.NumberColumn(format="%.3f"),
                "IB High": st.column_config.NumberColumn(format="%.2f"),
                "IB Low": st.column_config.NumberColumn(format="%.2f"),
                "IB Range": st.column_config.NumberColumn(format="%.2f"),
                "ATR": st.column_config.NumberColumn(format="%.2f"),
            },
        )

st.subheader("Scan Diagnostics")
if status_df.empty:
    st.info("No diagnostics available yet.")
else:
    st.dataframe(status_df, use_container_width=True, hide_index=True)

st.caption("Tip: Run after 10:15 AM IST. Use the daily download task after market close to prepare CSV data for the next day.")
