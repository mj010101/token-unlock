import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import os

# ============================================================
# STEP 1: UNLOCK EVENT DATA
# Add OTC deal terms from Jeff's network here
# ============================================================

UNLOCK_EVENTS = [
    # (symbol, unlock_date, unlock_amount_tokens, total_supply, category)

    # --- Original 18 events ---
    ("ARB",   "2024-03-16", 1_110_000_000, 10_000_000_000, "VC"),
    ("ARB",   "2024-04-16",    92_500_000, 10_000_000_000, "VC"),
    ("ARB",   "2024-05-16",    92_500_000, 10_000_000_000, "VC"),
    ("ARB",   "2024-06-16",    92_500_000, 10_000_000_000, "VC"),
    ("OP",    "2024-01-31",    24_160_000,  4_294_967_296, "VC"),
    ("OP",    "2024-02-29",    24_160_000,  4_294_967_296, "VC"),
    ("OP",    "2024-05-31",    24_160_000,  4_294_967_296, "VC"),
    ("APT",   "2024-01-12",     2_830_000,  1_000_000_000, "VC"),
    ("APT",   "2024-04-12",     2_830_000,  1_000_000_000, "VC"),
    ("SUI",   "2024-05-03",    64_363_000, 10_000_000_000, "VC"),
    ("SUI",   "2024-06-03",    64_363_000, 10_000_000_000, "VC"),
    ("SUI",   "2024-07-03",    64_363_000, 10_000_000_000, "VC"),
    ("PYTH",  "2024-05-20", 2_500_000_000, 10_000_000_000, "OTC"),
    ("STRK",  "2024-06-15",    64_000_000, 10_000_000_000, "VC"),
    ("JUP",   "2025-01-31", 1_000_000_000, 10_000_000_000, "VC"),
    ("ZK",    "2025-06-17", 3_675_000_000, 21_000_000_000, "VC"),
    ("ENA",   "2025-04-05",   180_000_000, 15_000_000_000, "OTC"),
    ("EIGEN", "2025-09-30",    86_000_000,  1_674_203_671, "VC"),

    # --- Expanded dataset ---

    # Layer 1 / Layer 2
    ("IMX",   "2024-01-24",    36_332_000,  2_000_000_000, "VC"),
    ("IMX",   "2024-07-24",    36_332_000,  2_000_000_000, "VC"),
    ("AVAX",  "2024-01-01",     9_500_000,    720_000_000, "VC"),
    ("SEI",   "2024-08-15",   900_000_000, 10_000_000_000, "VC"),
    ("TIA",   "2024-10-30",   175_586_000,  1_000_000_000, "VC"),
    ("ALT",   "2024-03-25",   518_400_000, 10_000_000_000, "VC"),

    # DeFi
    ("WLD",   "2024-03-24",   101_695_000, 10_000_000_000, "OTC"),
    ("WLD",   "2024-07-24",   101_695_000, 10_000_000_000, "OTC"),
    ("DYDX",  "2024-02-03",   150_000_000,  1_000_000_000, "VC"),
    ("DYDX",  "2024-06-01",   150_000_000,  1_000_000_000, "VC"),
    ("BLUR",  "2024-02-14",   300_000_000,  3_000_000_000, "VC"),
    ("UNI",   "2024-04-17",    43_000_000,  1_000_000_000, "VC"),

    # AI / Gaming
    ("RNDR",  "2024-04-01",     5_000_000,    536_870_912, "VC"),
    ("AXS",   "2024-01-22",     6_000_000,    270_000_000, "VC"),
    ("RON",   "2024-01-27",    58_000_000,  1_000_000_000, "VC"),

    # 2023 data (3-year lookback)
    ("ARB",   "2023-09-16", 1_110_000_000, 10_000_000_000, "VC"),
    ("OP",    "2023-05-31",    24_160_000,  4_294_967_296, "VC"),
    ("OP",    "2023-06-30",    24_160_000,  4_294_967_296, "VC"),
    ("OP",    "2023-07-31",    24_160_000,  4_294_967_296, "VC"),
    ("APE",   "2023-03-17",    15_607_000,  1_000_000_000, "VC"),
    ("APE",   "2023-06-17",    15_607_000,  1_000_000_000, "VC"),
    ("LDO",   "2023-05-25",    22_965_000,  1_000_000_000, "VC"),
    ("MAGIC", "2023-02-25",    30_000_000,    350_000_000, "VC"),
    ("GMT",   "2023-03-09",   600_000_000,  6_000_000_000, "OTC"),
]


# ============================================================
# STEP 2: PRICE DATA FROM BINANCE
# ============================================================

def get_binance_ohlcv(symbol: str, date_str: str, days_before: int = 1, days_after: int = 15):
    """Fetch OHLCV data around unlock date from Binance."""
    binance_symbol = f"{symbol}USDT"

    unlock_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_date  = unlock_date - timedelta(days=days_before)
    end_date    = unlock_date + timedelta(days=days_after + 1)

    start_ms = int(start_date.timestamp() * 1000)
    end_ms   = int(end_date.timestamp()   * 1000)

    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol":    binance_symbol,
        "interval":  "1d",
        "startTime": start_ms,
        "endTime":   end_ms,
        "limit":     20,
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"  [SKIP] {binance_symbol} not found on Binance")
            return None

        data = resp.json()
        if not data:
            return None

        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close",
            "volume", "close_time", "quote_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ])
        df["date"]  = pd.to_datetime(df["open_time"], unit="ms").dt.date
        df["open"]  = df["open"].astype(float)
        df["close"] = df["close"].astype(float)
        return df[["date", "open", "close"]]

    except Exception as e:
        print(f"  [ERROR] {symbol}: {e}")
        return None


def get_btc_return(date_str: str, day_offset: int) -> float:
    """Fetch BTC return from unlock day open to day_offset close."""
    unlock_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_ms = int((unlock_date - timedelta(days=1)).timestamp() * 1000)
    end_ms   = int((unlock_date + timedelta(days=day_offset + 1)).timestamp() * 1000)

    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol":    "BTCUSDT",
        "interval":  "1d",
        "startTime": start_ms,
        "endTime":   end_ms,
        "limit":     20,
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if len(data) < 2:
            return None

        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close",
            "volume", "close_time", "quote_vol", "trades",
            "tbbase", "tbquote", "ignore"
        ])
        df["open"]  = df["open"].astype(float)
        df["close"] = df["close"].astype(float)

        open0  = df.iloc[0]["open"]
        target = df.iloc[min(day_offset, len(df) - 1)]["close"]
        return (target / open0) - 1

    except Exception:
        return None


# ============================================================
# STEP 3: BUILD PIVOT TABLE
# ============================================================

def build_pivot_table(unlock_events):
    results = []

    for symbol, date_str, unlock_amount, total_supply, category in unlock_events:
        print(f"Processing {symbol} {date_str}...")

        price_df = get_binance_ohlcv(symbol, date_str)
        if price_df is None or len(price_df) < 2:
            print(f"  [SKIP] {symbol} {date_str} - insufficient price data")
            continue

        unlock_dt  = datetime.strptime(date_str, "%Y-%m-%d").date()
        unlock_day = price_df[price_df["date"] == unlock_dt]

        if unlock_day.empty:
            price_df["date_dt"] = pd.to_datetime(price_df["date"])
            unlock_day = price_df.iloc[[0]]

        open0 = float(unlock_day.iloc[0]["open"])

        row = {
            "symbol":            symbol,
            "unlock_date":       date_str,
            "category":          category,
            "unlock_amount":     unlock_amount,
            "total_supply":      total_supply,
            "unlock_pct_supply": round(unlock_amount / total_supply * 100, 4),
            "open_price":        open0,
        }

        for day in range(15):
            target_dt = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=day)).date()
            day_data  = price_df[price_df["date"] == target_dt]

            if not day_data.empty:
                close_n      = float(day_data.iloc[0]["close"])
                token_return = (close_n / open0) - 1
            else:
                token_return = None

            btc_return = get_btc_return(date_str, day)

            excess = (token_return - btc_return) if (token_return is not None and btc_return is not None) else None

            row[f"return_{day}"]        = round(token_return, 6) if token_return is not None else None
            row[f"btc_return_{day}"]    = round(btc_return,   6) if btc_return   is not None else None
            row[f"excess_return_{day}"] = round(excess,        6) if excess       is not None else None

        results.append(row)
        time.sleep(0.3)

    return pd.DataFrame(results)


# ============================================================
# STEP 4: ANALYSIS & OUTPUT
# ============================================================

def print_analysis(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("TOKEN UNLOCK ALPHA ANALYSIS")
    print("=" * 60)

    # 1. Average excess return by unlock size bucket
    print("\n[1] Average Excess Return by Unlock Size (% of supply)")
    print("-" * 55)
    bins   = [0, 1, 5, 10, 100]
    labels = ["Small  <1%", "Mid  1-5%", "Large 5-10%", "XL   >10%"]
    df["size_bucket"] = pd.cut(df["unlock_pct_supply"], bins=bins, labels=labels)

    for day in [0, 1, 3, 7, 14]:
        col = f"excess_return_{day}"
        print(f"\n  Day {day:>2}:")
        result = df.groupby("size_bucket", observed=True)[col].agg(["mean", "count"])
        for bucket, r in result.iterrows():
            direction = "DOWN" if r["mean"] < 0 else "UP  "
            print(f"    [{direction}] {bucket}: {r['mean']*100:+.1f}%  (n={int(r['count'])})")

    # 2. VC vs OTC comparison
    print("\n\n[2] VC vs OTC Average Excess Return")
    print("-" * 40)
    print(f"  {'Day':<6} {'VC':>10} {'OTC':>10}")
    for day in [0, 1, 3, 7, 14]:
        col     = f"excess_return_{day}"
        vc_avg  = df[df["category"] == "VC"][col].mean()
        otc_avg = df[df["category"] == "OTC"][col].mean()
        print(f"  Day {day:<2}   {vc_avg*100:>+9.1f}%  {otc_avg*100:>+9.1f}%")

    # 3. Individual event summary sorted by Day1 excess return
    print("\n\n[3] Individual Events (sorted by Day1 Excess Return)")
    print("-" * 75)
    cols    = ["symbol", "unlock_date", "category", "unlock_pct_supply",
               "excess_return_0", "excess_return_1", "excess_return_7", "excess_return_14"]
    summary = df[cols].copy()
    summary.columns = ["Symbol", "Date", "Type", "Unlock%", "Day0", "Day1", "Day7", "Day14"]
    for col in ["Day0", "Day1", "Day7", "Day14"]:
        summary[col] = summary[col].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "N/A")
    summary = summary.sort_values("Day1")
    print(summary.to_string(index=False))

    # 4. Notable outliers
    print("\n\n[4] Notable Outliers (|Day1 Excess| > 10%)")
    print("-" * 55)
    col      = "excess_return_1"
    outliers = df[df[col].abs() > 0.10][
        ["symbol", "unlock_date", "category", "unlock_pct_supply", col]
    ].sort_values(col)
    for _, r in outliers.iterrows():
        direction = "SURGE" if r[col] > 0 else "DUMP "
        print(f"  [{direction}] {r['symbol']} {r['unlock_date']}: "
              f"Day1 Excess={r[col]*100:+.1f}%  "
              f"(unlock={r['unlock_pct_supply']}%, {r['category']})")

    # 5. Overall summary
    print("\n\n[5] Overall Average Excess Return (all events)")
    print("-" * 40)
    for day in [0, 1, 3, 7, 14]:
        col = f"excess_return_{day}"
        avg = df[col].mean()
        print(f"  Day {day:>2}: {avg*100:+.2f}%")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Token Unlock Alpha Framework starting...\n")

    df = build_pivot_table(UNLOCK_EVENTS)

    os.makedirs("Jeff", exist_ok=True)
    df.to_csv("Jeff/unlock_alpha.csv", index=False)
    print("\nSaved: Jeff/unlock_alpha.csv")

    print_analysis(df)

    print("\nDone.")