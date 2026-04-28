import yfinance as yf
import argparse
import os
import time
from pathlib import Path

# Create folder if not exists
if not os.path.exists("data"):
    os.makedirs("data")

# NIFTY 200 Yahoo Finance symbols
stocks = [
    "360ONE.NS",
    "ABB.NS",
    "APLAPOLLO.NS",
    "AUBANK.NS",
    "ADANIENSOL.NS",
    "ADANIENT.NS",
    "ADANIGREEN.NS",
    "ADANIPORTS.NS",
    "ADANIPOWER.NS",
    "ATGL.NS",
    "ABCAPITAL.NS",
    "ALKEM.NS",
    "AMBUJACEM.NS",
    "APOLLOHOSP.NS",
    "ASHOKLEY.NS",
    "ASIANPAINT.NS",
    "ASTRAL.NS",
    "AUROPHARMA.NS",
    "DMART.NS",
    "AXISBANK.NS",
    "BSE.NS",
    "BAJAJ-AUTO.NS",
    "BAJFINANCE.NS",
    "BAJAJFINSV.NS",
    "BAJAJHLDNG.NS",
    "BANKBARODA.NS",
    "BANKINDIA.NS",
    "BDL.NS",
    "BEL.NS",
    "BHARATFORG.NS",
    "BHEL.NS",
    "BPCL.NS",
    "BHARTIARTL.NS",
    "GROWW.NS",
    "BIOCON.NS",
    "BLUESTARCO.NS",
    "BOSCHLTD.NS",
    "BRITANNIA.NS",
    "CGPOWER.NS",
    "CANBK.NS",
    "CHOLAFIN.NS",
    "CIPLA.NS",
    "COALINDIA.NS",
    "COCHINSHIP.NS",
    "COFORGE.NS",
    "COLPAL.NS",
    "CONCOR.NS",
    "COROMANDEL.NS",
    "CUMMINSIND.NS",
    "DLF.NS",
    "DABUR.NS",
    "DIVISLAB.NS",
    "DIXON.NS",
    "DRREDDY.NS",
    "EICHERMOT.NS",
    "ETERNAL.NS",
    "EXIDEIND.NS",
    "NYKAA.NS",
    "FEDERALBNK.NS",
    "FORTIS.NS",
    "GAIL.NS",
    "GVT&D.NS",
    "GMRAIRPORT.NS",
    "GLENMARK.NS",
    "GODFRYPHLP.NS",
    "GODREJCP.NS",
    "GODREJPROP.NS",
    "GRASIM.NS",
    "HCLTECH.NS",
    "HDFCAMC.NS",
    "HDFCBANK.NS",
    "HDFCLIFE.NS",
    "HAVELLS.NS",
    "HEROMOTOCO.NS",
    "HINDALCO.NS",
    "HAL.NS",
    "HINDPETRO.NS",
    "HINDUNILVR.NS",
    "HINDZINC.NS",
    "POWERINDIA.NS",
    "HUDCO.NS",
    "HYUNDAI.NS",
    "ICICIBANK.NS",
    "ICICIGI.NS",
    "ICICIAMC.NS",
    "IDFCFIRSTB.NS",
    "ITC.NS",
    "INDIANB.NS",
    "INDHOTEL.NS",
    "IOC.NS",
    "IRCTC.NS",
    "IRFC.NS",
    "IREDA.NS",
    "INDUSTOWER.NS",
    "INDUSINDBK.NS",
    "NAUKRI.NS",
    "INFY.NS",
    "INDIGO.NS",
    "JSWENERGY.NS",
    "JSWSTEEL.NS",
    "JINDALSTEL.NS",
    "JIOFIN.NS",
    "JUBLFOOD.NS",
    "KEI.NS",
    "KPITTECH.NS",
    "KALYANKJIL.NS",
    "KOTAKBANK.NS",
    "LTF.NS",
    "LGEINDIA.NS",
    "LICHSGFIN.NS",
    "LTM.NS",
    "LT.NS",
    "LAURUSLABS.NS",
    "LENSKART.NS",
    "LODHA.NS",
    "LUPIN.NS",
    "MRF.NS",
    "M&MFIN.NS",
    "M&M.NS",
    "MANKIND.NS",
    "MARICO.NS",
    "MARUTI.NS",
    "MFSL.NS",
    "MAXHEALTH.NS",
    "MAZDOCK.NS",
    "MOTILALOFS.NS",
    "MPHASIS.NS",
    "MCX.NS",
    "MUTHOOTFIN.NS",
    "NHPC.NS",
    "NMDC.NS",
    "NTPC.NS",
    "NATIONALUM.NS",
    "NESTLEIND.NS",
    "OBEROIRLTY.NS",
    "ONGC.NS",
    "OIL.NS",
    "PAYTM.NS",
    "OFSS.NS",
    "POLICYBZR.NS",
    "PIIND.NS",
    "PAGEIND.NS",
    "PATANJALI.NS",
    "PERSISTENT.NS",
    "PHOENIXLTD.NS",
    "PIDILITIND.NS",
    "POLYCAB.NS",
    "PFC.NS",
    "POWERGRID.NS",
    "PREMIERENE.NS",
    "PRESTIGE.NS",
    "PNB.NS",
    "RECLTD.NS",
    "RADICO.NS",
    "RVNL.NS",
    "RELIANCE.NS",
    "SBICARD.NS",
    "SBILIFE.NS",
    "SRF.NS",
    "MOTHERSON.NS",
    "SHREECEM.NS",
    "SHRIRAMFIN.NS",
    "ENRIN.NS",
    "SIEMENS.NS",
    "SOLARINDS.NS",
    "SBIN.NS",
    "SAIL.NS",
    "SUNPHARMA.NS",
    "SUPREMEIND.NS",
    "SUZLON.NS",
    "SWIGGY.NS",
    "TVSMOTOR.NS",
    "TATACAP.NS",
    "TATACOMM.NS",
    "TCS.NS",
    "TATACONSUM.NS",
    "TATAELXSI.NS",
    "TATAINVEST.NS",
    "TMCV.NS",
    "TMPV.NS",
    "TATAPOWER.NS",
    "TATASTEEL.NS",
    "TECHM.NS",
    "TITAN.NS",
    "TORNTPHARM.NS",
    "TRENT.NS",
    "TIINDIA.NS",
    "UPL.NS",
    "ULTRACEMCO.NS",
    "UNIONBANK.NS",
    "UNITDSPR.NS",
    "VBL.NS",
    "VEDL.NS",
    "VMM.NS",
    "IDEA.NS",
    "VOLTAS.NS",
    "WAAREEENER.NS",
    "WIPRO.NS",
    "YESBANK.NS",
    "ZYDUSLIFE.NS",
]

def flatten_yfinance_columns(data):
    if hasattr(data.columns, "nlevels") and data.columns.nlevels > 1:
        data.columns = [col[0] for col in data.columns]
    return data


def save_stock_csv(stock, data_dir, period, interval):
    print(f"Downloading {stock}...")

    data = yf.download(
        stock,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if data.empty:
        print(f"No data for {stock}")
        return False

    data = flatten_yfinance_columns(data)
    data.reset_index(inplace=True)

    date_column = "Date" if "Date" in data.columns else "Datetime"
    keep_columns = [date_column, "Close", "High", "Low", "Open", "Volume"]
    data = data[[column for column in keep_columns if column in data.columns]]
    data.rename(columns={date_column: "Date"}, inplace=True)
    data["Date"] = data["Date"].astype(str).str.slice(0, 10)
    data.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    data.sort_values("Date", inplace=True)

    file_path = data_dir / f"{stock}.csv"
    temp_path = data_dir / f"{stock}.csv.tmp"
    data.to_csv(temp_path, index=False)
    os.replace(temp_path, file_path)

    latest_date = data["Date"].iloc[-1]
    print(f"Saved {stock} latest date {latest_date}")
    return True


def download_all(period="3mo", interval="1d", delay=0.3):
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    print(f"Download started for {len(stocks)} stocks...")
    success_count = 0
    failed = []

    for stock in stocks:
        try:
            if save_stock_csv(stock, data_dir, period, interval):
                success_count += 1
        except Exception as exc:
            failed.append(stock)
            print(f"Error with {stock}: {exc}")
        time.sleep(delay)

    print(f"Download completed. Saved {success_count}/{len(stocks)} stocks.")
    if failed:
        print("Failed symbols: " + ", ".join(failed))


def parse_args():
    parser = argparse.ArgumentParser(description="Download NIFTY 200 daily data to data/*.csv")
    parser.add_argument("--period", default="3mo", help="Yahoo period, example: 3mo, 6mo, 1y")
    parser.add_argument("--interval", default="1d", help="Yahoo interval, example: 1d")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between stocks in seconds")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    download_all(period=args.period, interval=args.interval, delay=args.delay)
