import requests
import pandas as pd
import yfinance as yf
import time
import os

url = "https://api.hyperliquid.xyz/info"
start_ms = int((time.time() - 90 * 24 * 3600) * 1000)

HOLDING_DAYS = 30  # assumed holding period for basis annualization

# ── Asset Mapping: Hyperliquid perp coin → yfinance ticker ────
# Stocks use ticker directly, precious metals use futures
ASSETS = {
    "xyz:GOLD":   "GC=F",   # Gold futures
    "xyz:SILVER": "SI=F",   # Silver futures
    "xyz:NVDA":   "NVDA",
    "xyz:AAPL":   "AAPL",
    "xyz:AMZN":   "AMZN",
    "xyz:COIN":   "COIN",
    "xyz:GOOGL":  "GOOGL",
    "xyz:META":   "META",
    "xyz:MSFT":   "MSFT",
    "xyz:PLTR":   "PLTR",
    "xyz:TSLA":   "TSLA",
}

# ── Funding History ────────────────────────────────────────
def get_funding_history(coin):
    payload = {
        "type": "fundingHistory",
        "coin": coin,
        "dex": "xyz",
        "startTime": start_ms
    }
    resp = requests.post(url, json=payload)
    data = resp.json()
    if not data:
        print(f"  WARNING: No funding data for {coin}")
        return pd.DataFrame(columns=["date", "daily_funding_bps", "ann_funding_bps"])

    df = pd.DataFrame(data)
    df["time"]        = pd.to_datetime(df["time"], unit="ms", utc=True)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["date"]        = df["time"].dt.date

    # hourly → daily sum (positive: short receives / negative: short pays)
    daily = df.groupby("date").agg(
        daily_funding = ("fundingRate", "sum"),
    ).reset_index()

    daily["daily_funding_bps"] = daily["daily_funding"] * 10000
    daily["ann_funding_bps"]   = daily["daily_funding_bps"] * 365
    return daily

# ── Perp Price ────────────────────────────────────────────
def get_perp_price(coin):
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": "1d",
            "startTime": start_ms,
            "dex": "xyz"
        }
    }
    resp = requests.post(url, json=payload)
    data = resp.json()
    if not data:
        print(f"  WARNING: No perp price data for {coin}")
        return pd.DataFrame(columns=["date", "perp_close"])

    df = pd.DataFrame(data)
    df["date"]       = pd.to_datetime(df["t"], unit="ms", utc=True).dt.date
    df["perp_close"] = df["c"].astype(float)
    return df[["date", "perp_close"]]

# ── Spot Price (yfinance) ─────────────────────────────────
def get_spot_price(ticker):
    raw = yf.download(ticker, period="3mo", progress=False)
    if raw.empty:
        print(f"  WARNING: No spot data for {ticker}")
        return pd.DataFrame(columns=["date", "spot_close"])

    spot = raw["Close"].reset_index()
    spot.columns = ["date", "spot_close"]
    spot["date"] = spot["date"].dt.date
    return spot

# ── Build DataFrame ───────────────────────────────────────
def build_df(funding, perp, spot_df):
    if funding.empty or perp.empty or spot_df.empty:
        return pd.DataFrame()

    df = funding.merge(perp, on="date").merge(spot_df, on="date")
    if df.empty:
        return df

    # Basis (locked in at entry, realized as it converges at exit)
    df["basis_bps"]     = (df["perp_close"] - df["spot_close"]) / df["spot_close"] * 10000
    df["ann_basis_bps"] = df["basis_bps"] / HOLDING_DAYS * 365

    # total annualized alpha
    df["ann_total_bps"] = df["ann_funding_bps"] + df["ann_basis_bps"]
    return df

# ── Resample Summary ──────────────────────────────────────
def resample_summary(df, freq):
    return df.resample(freq, on="date").agg(
        spot_close      = ("spot_close",       "last"),
        perp_close      = ("perp_close",        "last"),
        basis_bps       = ("basis_bps",          "last"),
        funding_bps_sum = ("daily_funding_bps",  "sum"),
        ann_funding_bps = ("ann_funding_bps",     "mean"),
        ann_basis_bps   = ("ann_basis_bps",       "mean"),
        ann_total_bps   = ("ann_total_bps",        "mean"),
    ).dropna()

# ── Output Columns ────────────────────────────────────────
cols_daily = [
    "date",
    "spot_close", "perp_close",
    "basis_bps",
    "daily_funding_bps",
    "ann_funding_bps",
    "ann_basis_bps",
    "ann_total_bps",
]

pd.set_option("display.float_format", "{:.2f}".format)
os.makedirs("output", exist_ok=True)

# ── Main Loop ─────────────────────────────────────────────
all_monthly = {}  # collect monthly summaries per asset for comparison

for coin, ticker in ASSETS.items():
    asset_name = coin.replace("xyz:", "")  # e.g. "GOLD", "NVDA"
    print(f"\n{'='*50}")
    print(f"Processing {asset_name} (perp: {coin}, spot: {ticker})")
    print('='*50)

    funding = get_funding_history(coin)
    perp    = get_perp_price(coin)
    spot    = get_spot_price(ticker)
    df      = build_df(funding, perp, spot)

    if df.empty:
        print(f"  SKIP: insufficient data for {asset_name}")
        continue

    # Daily output
    print(f"\n--- {asset_name} Daily (last 5) ---")
    print(df[cols_daily].tail(5).to_string(index=False))

    # Weekly / Monthly output
    df["date"] = pd.to_datetime(df["date"])
    weekly  = resample_summary(df, "W")
    monthly = resample_summary(df, "ME")

    print(f"\n--- {asset_name} Monthly ---")
    print(monthly.tail(3).to_string())

    # Save CSVs
    df.to_csv(f"output/{asset_name.lower()}_daily.csv",   index=False)
    weekly.to_csv(f"output/{asset_name.lower()}_weekly.csv")
    monthly.to_csv(f"output/{asset_name.lower()}_monthly.csv")

    # collect monthly data for comparison
    monthly["asset"] = asset_name
    all_monthly[asset_name] = monthly

# ── All Assets Comparison Summary ────────────────────────
print(f"\n{'='*50}")
print("=== ALL ASSETS — Latest Monthly Snapshot ===")
print('='*50)

summary_rows = []
for asset_name, monthly in all_monthly.items():
    if monthly.empty:
        continue
    last = monthly.iloc[-1]
    summary_rows.append({
        "asset":            asset_name,
        "basis_bps":        round(last["basis_bps"], 2),
        "ann_funding_bps":  round(last["ann_funding_bps"], 2),
        "ann_basis_bps":    round(last["ann_basis_bps"], 2),
        "ann_total_bps":    round(last["ann_total_bps"], 2),
        "funding_bps_sum":  round(last["funding_bps_sum"], 2),
    })

summary_df = pd.DataFrame(summary_rows).sort_values("ann_total_bps", ascending=False)
print(summary_df.to_string(index=False))
summary_df.to_csv("output/all_assets_summary.csv", index=False)

print(f"\nAll CSVs saved to ./output/")
print(f"Note: ann_basis_bps assumes {HOLDING_DAYS}-day holding period.")