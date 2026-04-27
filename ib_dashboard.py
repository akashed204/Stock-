from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Sequence, Tuple

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from ib_scanner import DEFAULT_SYMBOLS, IBResult, create_client, get_session_times, ist_now, scan_symbol


st.set_page_config(page_title="IB Scanner Dashboard", layout="wide")

# Auto-refresh every 60 seconds.
st_autorefresh(interval=60 * 1000, key="ib_scanner_refresh")

st.title("IB Scanner Dashboard")


@st.cache_resource
def get_client(username: str, api_key: str):
    return create_client(username, api_key)


@st.cache_data(ttl=55, show_spinner=False)
def run_scan(username: str, api_key: str, symbols: Tuple[str, ...]) -> Tuple[List[IBResult], str]:
    client = get_client(username, api_key)
    market_open, ib_end, daily_start, daily_end = get_session_times()
    current_time = ist_now()

    if current_time < ib_end:
        return [], (
            "IB window not complete yet. "
            f"Current IST: {current_time.strftime('%H:%M:%S')} | "
            f"IB end IST: {ib_end.strftime('%H:%M:%S')}"
        )

    results: List[IBResult] = []

    max_workers = min(16, max(4, len(symbols))) if symbols else 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
            for symbol in symbols
        }

        for future in as_completed(futures):
            item = future.result()
            if item is not None:
                results.append(item)

    results.sort(key=lambda x: x.ib_range / x.atr)
    return results, ""


def build_dataframe(results: Sequence[IBResult]) -> pd.DataFrame:
    rows = []
    for r in results:
        ratio = r.ib_range / r.atr if r.atr else 0.0
        rows.append(
            {
                "Symbol": r.symbol,
                "IB High": round(r.ib_high, 2),
                "IB Low": round(r.ib_low, 2),
                "IB Range": round(r.ib_range, 2),
                "ATR": round(r.atr, 2),
                "Range/ATR": round(ratio, 3),
                "IB Type": r.ib_type,
            }
        )

    df = pd.DataFrame(rows, columns=["Symbol", "IB High", "IB Low", "IB Range", "ATR", "Range/ATR", "IB Type"])
    if not df.empty:
        df = df.sort_values(by="Range/ATR", ascending=True).reset_index(drop=True)
    return df


def style_by_ib_type(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def row_style(row: pd.Series) -> List[str]:
        if row["IB Type"] == "Small IB":
            color = "background-color: #c8f7c5"
        elif row["IB Type"] == "Normal IB":
            color = "background-color: #fff7ae"
        elif row["IB Type"] == "Wide IB":
            color = "background-color: #f8c6c6"
        else:
            color = ""
        return [color] * len(row)

    return df.style.apply(row_style, axis=1)


with st.sidebar:
    st.header("Settings")
    username_default = st.secrets.get("ALICE_USERNAME", "")
    api_key_default = st.secrets.get("ALICE_API_KEY", "")

    username = st.text_input("Alice Blue Username", value=username_default)
    api_key = st.text_input("Alice Blue API Key", type="password", value=api_key_default)
    selected_symbols = st.multiselect("Symbols", options=DEFAULT_SYMBOLS, default=DEFAULT_SYMBOLS)
    ib_filter = st.selectbox("IB Type Filter", ["All", "Small IB", "Normal IB", "Wide IB"])

if not username or not api_key:
    st.info("Enter Alice Blue Username and API Key in the sidebar to start scanning.")
    st.stop()

if not selected_symbols:
    st.warning("Select at least one symbol.")
    st.stop()

with st.spinner("Fetching live data..."):
    try:
        results, status_message = run_scan(username, api_key, tuple(selected_symbols))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Scanner failed: {exc}")
        st.stop()

if status_message:
    st.warning(status_message)

st.caption(f"Last updated (IST): {ist_now().strftime('%Y-%m-%d %H:%M:%S')}")

if not results:
    st.info("No valid results generated.")
    st.stop()

df = build_dataframe(results)

if ib_filter != "All":
    df = df[df["IB Type"] == ib_filter].reset_index(drop=True)

if df.empty:
    st.info("No rows match the selected filter.")
    st.stop()

styled_df = style_by_ib_type(df)
st.dataframe(styled_df, use_container_width=True, hide_index=True)
