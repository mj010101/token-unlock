import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import os

# ============================================================
# COINGECKO MAPPINGS
# Free API for historical circulating supply data
# ============================================================

# Hardcoded circulating supply snapshots (as of unlock date)
# Source: CoinGecko/CMC historical data
# ZK, ENA, EIGEN are estimates pending actual unlock dates

CIRCULATING_SUPPLY_SNAPSHOT = {
    ("ARB",   "2024-03-16"): 3_275_000_000,
    ("OP",    "2024-01-31"):   893_000_000,
    ("APT",   "2024-01-12"):   374_000_000,
    ("SUI",   "2024-05-03"):   928_000_000,
    ("PYTH",  "2024-05-20"): 3_550_000_000,
    ("STRK",  "2024-06-15"):   728_000_000,
    ("JUP",   "2025-01-31"): 1_350_000_000,
    ("ZK",    "2025-06-17"): 5_600_000_000,
    ("ENA",   "2025-04-05"): 3_500_000_000,
    ("EIGEN", "2025-09-30"):   400_000_000,
    ("IMX",   "2024-01-24"):   975_000_000,
    ("AVAX",  "2024-01-01"):   391_000_000,
    ("SEI",   "2024-08-15"): 2_590_000_000,
    ("TIA",   "2024-10-30"):   192_000_000,
    ("ALT",   "2024-03-25"):   851_000_000,
    ("WLD",   "2024-03-24"):   154_000_000,
    ("DYDX",  "2024-02-03"):   170_000_000,
    ("BLUR",  "2024-02-14"):   560_000_000,
    ("UNI",   "2024-04-17"):   601_000_000,
    ("RNDR",  "2024-04-01"):   379_000_000,
    ("AXS",   "2024-01-22"):    60_000_000,
    ("APE",   "2023-03-17"):   289_000_000,
    ("LDO",   "2023-05-25"):   878_000_000,
    ("MAGIC", "2023-02-25"):   213_000_000,
    ("GMT",   "2023-03-09"):   600_000_000,
}

# ============================================================
# STEP 1: UNLOCK EVENT DATA
# Add OTC deal terms from Jeff's network here
# ============================================================

UNLOCK_EVENTS = [
    # (symbol, unlock_date, unlock_amount_tokens, total_supply, category)
    # Only the first unlock per token is included
    
    ("ARB",   "2024-03-16", 1_110_000_000, 10_000_000_000, "VC"),
    ("OP",    "2024-01-31",    24_160_000,  4_294_967_296, "VC"),
    ("APT",   "2024-01-12",     2_830_000,  1_000_000_000, "VC"),
    ("SUI",   "2024-05-03",    64_363_000, 10_000_000_000, "VC"),
    ("PYTH",  "2024-05-20", 2_500_000_000, 10_000_000_000, "OTC"),
    ("STRK",  "2024-06-15",    64_000_000, 10_000_000_000, "VC"),
    ("JUP",   "2025-01-31", 1_000_000_000, 10_000_000_000, "VC"),
    ("ZK",    "2025-06-17", 3_675_000_000, 21_000_000_000, "VC"),
    ("ENA",   "2025-04-05",   180_000_000, 15_000_000_000, "OTC"),
    ("EIGEN", "2025-09-30",    86_000_000,  1_674_203_671, "VC"),
    ("IMX",   "2024-01-24",    36_332_000,  2_000_000_000, "VC"),
    ("AVAX",  "2024-01-01",     9_500_000,    720_000_000, "VC"),
    ("SEI",   "2024-08-15",   900_000_000, 10_000_000_000, "VC"),
    ("TIA",   "2024-10-30",   175_586_000,  1_000_000_000, "VC"),
    ("ALT",   "2024-03-25",   518_400_000, 10_000_000_000, "VC"),
    ("WLD",   "2024-03-24",   101_695_000, 10_000_000_000, "OTC"),
    ("DYDX",  "2024-02-03",   150_000_000,  1_000_000_000, "VC"),
    ("BLUR",  "2024-02-14",   300_000_000,  3_000_000_000, "VC"),
    ("UNI",   "2024-04-17",    43_000_000,  1_000_000_000, "VC"),
    ("RNDR",  "2024-04-01",     5_000_000,    536_870_912, "VC"),
    ("AXS",   "2024-01-22",     6_000_000,    270_000_000, "VC"),
    ("APE",   "2023-03-17",    15_607_000,  1_000_000_000, "VC"),
    ("LDO",   "2023-05-25",    22_965_000,  1_000_000_000, "VC"),
    ("MAGIC", "2023-02-25",    30_000_000,    350_000_000, "VC"),
    ("GMT",   "2023-03-09",   600_000_000,  6_000_000_000, "OTC"),
]


# ============================================================
# STEP 2: PRICE DATA FROM BINANCE
# ============================================================

def get_binance_ohlcv(symbol: str, date_str: str, days_before: int = 8, days_after: int = 8):

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
        "limit":     30,
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
        df["volume"] = df["volume"].astype(float)
        df["quote_volume"] = df["quote_volume"].astype(float)
        return df[["date", "open", "close", "volume", "quote_volume"]]

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


def get_circulating_supply(symbol: str, date_str: str) -> float | None:
    """
    Fetch circulating supply from hardcoded snapshot.
    Returns circulating supply as float, or None if unavailable.
    """
    supply = CIRCULATING_SUPPLY_SNAPSHOT.get((symbol, date_str))
    if supply is None:
        print(f"  [WARN] No snapshot entry for {symbol} {date_str}")
        return None
    return float(supply)


# ============================================================
# STEP 3: BUILD PIVOT TABLE
# ============================================================

BIG_UNLOCK_THRESHOLD = 1.0  # % of circulating supply

def build_pivot_table(unlock_events):
    results = []
    price_cache = {}  # Cache to store price_df for each (symbol, date_str)

    for symbol, date_str, unlock_amount, total_supply, category in unlock_events:
        print(f"Processing {symbol} {date_str}...")

        # Fetch historical circulating supply from snapshot
        circulating_supply = get_circulating_supply(symbol, date_str)

        if circulating_supply is None or circulating_supply == 0:
            print(f"  [SKIP] {symbol} {date_str} - could not fetch circulating supply")
            continue

        unlock_pct_circulating = round(unlock_amount / circulating_supply * 100, 4)

        # Filter by big unlock threshold (>= 5% of circulating supply)
        if unlock_pct_circulating < BIG_UNLOCK_THRESHOLD:
            print(f"  [SKIP] {symbol} - small unlock ({unlock_pct_circulating:.2f}% of circulating)")
            continue

        price_df = get_binance_ohlcv(symbol, date_str, days_before=8, days_after=8)
        if price_df is None or len(price_df) < 2:
            print(f"  [SKIP] {symbol} {date_str} - insufficient price data")
            continue
        
        # Cache the price data for later raw export
        price_cache[(symbol, date_str)] = price_df

        # Calculate average daily USD volume for liquidity filter
        avg_daily_volume_usd = price_df["quote_volume"].mean()
        if avg_daily_volume_usd < 1_000_000:
            print(f"  [SKIP] {symbol} - low liquidity (avg ${avg_daily_volume_usd:,.0f}/day)")
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
            "circulating_supply": circulating_supply,
            "unlock_pct_supply": round(unlock_amount / total_supply * 100, 4),
            "unlock_pct_circulating": unlock_pct_circulating,
            "open_price":        open0,
            "avg_daily_volume_usd": round(avg_daily_volume_usd, 0),
        }

        # Calculate pre-unlock returns (Day -7 to Day -1)
        unlock_day_idx = None
        for idx, row_date in enumerate(price_df["date"]):
            if row_date == unlock_dt:
                unlock_day_idx = idx
                break
        
        if unlock_day_idx is None:
            unlock_day_idx = 0
        
        pre_unlock_rows = price_df.iloc[max(0, unlock_day_idx - 7):unlock_day_idx]
        
        if len(pre_unlock_rows) >= 2:
            pre_open = float(pre_unlock_rows.iloc[0]["open"])
            pre_close = float(pre_unlock_rows.iloc[-1]["close"])
            row["pre7_return"] = round((pre_close / pre_open) - 1, 6)
            
            # Calculate individual day returns for Day -7 to Day -1
            for i, (pre_idx, pre_row) in enumerate(pre_unlock_rows.iterrows()):
                pre_day_return = (float(pre_row["close"]) / pre_open) - 1
                days_from_end = i - len(pre_unlock_rows)
                row[f"pre_return_{days_from_end}"] = round(pre_day_return, 6)
            
            # Calculate pre7 excess return (token return vs BTC return)
            pre_start_date_str = str(pre_unlock_rows.iloc[0]["date"])
            pre_end_date_str = str(pre_unlock_rows.iloc[-1]["date"])
            btc_pre7_return = get_btc_range_return(pre_start_date_str, pre_end_date_str)
            
            if btc_pre7_return is not None:
                row["pre7_excess_return"] = round(row["pre7_return"] - btc_pre7_return, 6)
            else:
                row["pre7_excess_return"] = None
        else:
            row["pre7_return"] = None
            row["pre7_excess_return"] = None
            for d in range(-7, 0):
                row[f"pre_return_{d}"] = None

        for day in range(8):
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

    return pd.DataFrame(results), price_cache


# ============================================================
# STEP 4: ANALYSIS & OUTPUT
# ============================================================

def get_btc_range_return(start_date_str: str, end_date_str: str) -> float:
    """Fetch BTC return for a date range (start_date open to end_date close)."""
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    start_ms = int((start_date - timedelta(days=1)).timestamp() * 1000)
    end_ms = int((end_date + timedelta(days=1)).timestamp() * 1000)
    
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 30,
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
        df["open"] = df["open"].astype(float)
        df["close"] = df["close"].astype(float)
        
        open0 = df.iloc[0]["open"]
        close_end = df.iloc[-1]["close"]
        return (close_end / open0) - 1
    except Exception:
        return None


def print_analysis(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("TOKEN UNLOCK ALPHA ANALYSIS")
    print("=" * 60)
    print("\nNote: All post-unlock returns are cumulative from Day 0 open. Day N = close_N / open_0 - 1.")

    # 1. Day -7 vs Day +7 (PRIMARY ANALYSIS)
    print("\n[1] Day -7 to -1 (Pre-Unlock) vs Day 0 to +7 (Post-Unlock)")
    print("-" * 55)
    
    pre7_avg = df["pre7_excess_return"].mean()
    post7_avg = df["excess_return_7"].mean()
    
    print(f"  Day -7 to -1 (pre-unlock):   {pre7_avg*100:+.2f}%  (excess vs BTC)")
    print(f"  Day  0 to +7 (post-unlock):  {post7_avg*100:+.2f}%  (excess vs BTC)")
    
    print(f"\n  By category:")
    print(f"    {'':20} {'Pre-7 (vs BTC)':>15} {'Post-7 (vs BTC)':>15}")
    for cat in ["VC", "OTC"]:
        cat_df = df[df["category"] == cat]
        if len(cat_df) > 0:
            pre7_cat = cat_df["pre7_excess_return"].mean()
            post7_cat = cat_df["excess_return_7"].mean()
            print(f"    {cat:20} {pre7_cat*100:>+14.2f}%  {post7_cat*100:>+14.2f}%")

    # 2. Average excess return by unlock size bucket
    print("\n\n[2] Average Excess Return by Unlock Size (% of circulating supply)")
    print("-" * 55)
    bins   = [0, 5, 10, 20, 100]
    labels = ["Small <5%", "Mid 5-10%", "Large 10-20%", "XL >20%"]
    df["size_bucket"] = pd.cut(df["unlock_pct_circulating"], bins=bins, labels=labels)

    for day in [0, 1, 3, 7]:
        col = f"excess_return_{day}"
        print(f"\n  Day {day:>2}:")
        result = df.groupby("size_bucket", observed=True)[col].agg(["mean", "count"])
        for bucket, r in result.iterrows():
            direction = "DOWN" if r["mean"] < 0 else "UP  "
            print(f"    [{direction}] {bucket}: {r['mean']*100:+.1f}%  (n={int(r['count'])})")

    # 3. VC vs OTC comparison
    print("\n\n[3] VC vs OTC Average Excess Return")
    print("-" * 40)
    print(f"  {'Day':<6} {'VC':>10} {'OTC':>10}")
    for day in [0, 1, 3, 7]:
        col     = f"excess_return_{day}"
        vc_avg  = df[df["category"] == "VC"][col].mean()
        otc_avg = df[df["category"] == "OTC"][col].mean()
        print(f"  Day {day:<2}   {vc_avg*100:>+9.1f}%  {otc_avg*100:>+9.1f}%")

    # 4. Individual event summary sorted by Day1 excess return
    print("\n\n[4] Individual Events (sorted by Day1 Excess Return)")
    print("-" * 75)
    cols    = ["symbol", "unlock_date", "category", "unlock_pct_supply",
               "excess_return_0", "excess_return_1", "excess_return_7"]
    summary = df[cols].copy()
    summary.columns = ["Symbol", "Date", "Type", "Unlock%", "Day0", "Day1", "Day7"]
    # Sort by Day1 BEFORE converting to strings
    summary = summary.sort_values("Day1")
    for col in ["Day0", "Day1", "Day7"]:
        summary[col] = summary[col].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "N/A")
    print(summary.to_string(index=False))

    # 5. Notable outliers
    print("\n\n[5] Notable Outliers (|Day1 Excess| > 10%)")
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

    # 6. Overall summary
    print("\n\n[6] Overall Average Excess Return (all events)")
    print("-" * 40)
    for day in [0, 1, 3, 7]:
        col = f"excess_return_{day}"
        avg = df[col].mean()
        print(f"  Day {day:>2}: {avg*100:+.2f}%")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Token Unlock Alpha Framework starting...\n")

    df, price_cache = build_pivot_table(UNLOCK_EVENTS)

    os.makedirs("Jeff", exist_ok=True)
    df.to_csv("Jeff/unlock_alpha.csv", index=False)
    print("\nSaved: Jeff/unlock_alpha.csv")

    print_analysis(df)

    # Export raw price data for all events (using cached data)
    print("\n\nExporting raw price data...")
    raw_prices = []
    
    for symbol, date_str, unlock_amount, total_supply, category in UNLOCK_EVENTS:
        if (symbol, date_str) not in price_cache:
            continue
        
        price_df = price_cache[(symbol, date_str)]
        unlock_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        # Find the unlock day index
        unlock_day_idx = None
        for idx, row_date in enumerate(price_df["date"]):
            if row_date == unlock_dt:
                unlock_day_idx = idx
                break
        
        if unlock_day_idx is None:
            unlock_day_idx = 0
        
        # Get unlock_pct_supply from the dataframe
        unlock_pct = round(unlock_amount / total_supply * 100, 4)
        
        # Tag each row with unlock info and day offset, filter to -7 to +7
        for idx, (_, row) in enumerate(price_df.iterrows()):
            day_offset = idx - unlock_day_idx
            if -7 <= day_offset <= 7:  # Only keep data in range
                raw_prices.append({
                    "symbol": symbol,
                    "unlock_date": date_str,
                    "category": category,
                    "unlock_pct_supply": unlock_pct,
                    "day_offset": day_offset,
                    "date": row["date"],
                    "open": row["open"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "quote_volume": row["quote_volume"],
                })
    
    if raw_prices:
        raw_df = pd.DataFrame(raw_prices)
        raw_df.to_csv("Jeff/unlock_raw_prices.csv", index=False)
        print("Saved: Jeff/unlock_raw_prices.csv")

    print("\nDone.")