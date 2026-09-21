"""
Build 4 shuffled portfolio sets of 50 stocks each from the mid + high-mid cap,
20+ year history universe ($2B-$50B current market cap, >=20yr trading history).

Steps:
  1. Select eligible tickers: >=20yr history (validation_summary.csv) AND
     current market cap in [2B, 50B) (tickers/sp500_companies.csv).
  2. Shuffle the ticker list (fixed seed for reproducibility), keep first 200
     (203 eligible - drop 3 after shuffling), split into 4 sets of 50.
     Only the *order of stocks* is shuffled - each stock's own time series
     (row order) is left untouched.
  3. For each ticker, reprocess output/sp500_individual/{TICKER}.csv:
     drop bad RET codes ('C'/'B'), compute the 6 predictors, keep
     [date, RET, VOL_CHANGE, BA_SPREAD, ILLIQUIDITY, sprtrn, TURNOVER].
  4. Save to output/mid_highmid_20yr_portfolios/set{1..4}/{TICKER}.csv
"""

import os
import random
import pandas as pd
import numpy as np

SEED = 42

VAL_SUMMARY = "output/sp500_individual/validation_summary.csv"
CAP_FILE = "tickers/sp500_companies.csv"
IN_DIR = "output/sp500_individual"
OUT_DIR = "output/mid_highmid_20yr_portfolios"

CAP_MIN_B = 2
CAP_MAX_B = 50
MIN_YEARS = 20
N_SETS = 4
SET_SIZE = 50

KEEP_COLS = ["date", "RET", "VOL_CHANGE", "BA_SPREAD", "ILLIQUIDITY", "sprtrn", "TURNOVER", "PRC", "TRAN_COST"]


def parse_cap(s):
    s = str(s).strip()
    if s.endswith("T"):
        return float(s[:-1]) * 1000
    if s.endswith("B"):
        return float(s[:-1])
    if s.endswith("M"):
        return float(s[:-1]) / 1000
    try:
        return float(s)
    except ValueError:
        return None


def select_eligible_tickers():
    val = pd.read_csv(VAL_SUMMARY)
    val["date_start"] = pd.to_datetime(val["date_start"])
    val["date_end"] = pd.to_datetime(val["date_end"])
    val["years"] = (val["date_end"] - val["date_start"]).dt.days / 365.25

    cap = pd.read_csv(CAP_FILE)
    cap["mcap_B"] = cap["Market Cap"].apply(parse_cap)

    merged = val.merge(cap[["Symbol", "mcap_B"]], left_on="ticker", right_on="Symbol", how="left")
    eligible = merged[
        (merged["years"] >= MIN_YEARS)
        & (merged["mcap_B"] >= CAP_MIN_B)
        & (merged["mcap_B"] < CAP_MAX_B)
    ]
    return sorted(eligible["ticker"].tolist())


def process_ticker(ticker):
    df = pd.read_csv(os.path.join(IN_DIR, f"{ticker}.csv"), low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    df = df[~df["RET"].isin(["C", "B"])]
    df["RET"] = pd.to_numeric(df["RET"], errors="coerce")

    for col in ["PRC", "VOL", "SHROUT", "ASK", "BID"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["PRC"] = df["PRC"].abs()

    df["VOL_CHANGE"] = df["VOL"].pct_change(fill_method=None)
    df["BA_SPREAD"] = (df["ASK"] - df["BID"]) / df["PRC"]
    df["ILLIQUIDITY"] = df["RET"] / (df["VOL"] * df["PRC"])
    df["TURNOVER"] = df["VOL"] / df["SHROUT"]
    df["TRAN_COST"] = (df["ASK"] - df["BID"]) / 2

    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df[KEEP_COLS]


def main():
    eligible = select_eligible_tickers()
    print(f"Eligible tickers ($2B-$50B, >=20yr history): {len(eligible)}")

    rng = random.Random(SEED)
    shuffled = eligible.copy()
    rng.shuffle(shuffled)

    n_use = N_SETS * SET_SIZE
    kept = shuffled[:n_use]
    dropped = shuffled[n_use:]
    print(f"Using {len(kept)} tickers, dropped after shuffle: {dropped}")

    for i in range(N_SETS):
        set_tickers = kept[i * SET_SIZE : (i + 1) * SET_SIZE]
        set_dir = os.path.join(OUT_DIR, f"set{i+1}")
        os.makedirs(set_dir, exist_ok=True)
        for ticker in set_tickers:
            out_df = process_ticker(ticker)
            out_df.to_csv(os.path.join(set_dir, f"{ticker}.csv"), index=False)
        print(f"set{i+1}: {len(set_tickers)} tickers -> {set_dir}/")
        print(f"  {sorted(set_tickers)}")

    # sanity: no repeats across sets
    all_used = [t for i in range(N_SETS) for t in kept[i*SET_SIZE:(i+1)*SET_SIZE]]
    assert len(all_used) == len(set(all_used)), "duplicate ticker across sets!"
    print(f"\nDone. {N_SETS} sets x {SET_SIZE} tickers = {len(all_used)} files, no repeats confirmed.")


if __name__ == "__main__":
    main()
