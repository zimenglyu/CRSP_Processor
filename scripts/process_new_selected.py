"""
Process selected mid-cap stocks (not in existing selected_tickers_50.txt).
Steps per file:
  1. Filter to 2000-01-01 – 2023-12-31
  2. Drop rows where RET is 'C' or 'B'
  3. Add predictors following stock.py conventions
  4. Keep only the columns matching the reference output
  5. Save to output/new_selected_raw/

Final 18 tickers (all have full 2000-2023 history, none in selected_tickers_50.txt):
  Kept from original picks  : APA, DGX, IP, KEY, NI, NTAP, SNA, WSM
  Replacements (non-overlap): ZBRA, SJM, RF, RVTY, EME, MAA, WAB, BAX, CMS, FITB
"""

import os
import pandas as pd
import numpy as np

TICKERS = [
    # kept
    "APA", "DGX", "IP", "KEY", "NI", "NTAP", "SNA", "WSM",
    # replacements
    "ZBRA", "SJM", "RF", "RVTY", "EME", "MAA", "WAB", "BAX", "CMS", "FITB",
]

IN_DIR  = "output/sp500_individual"
OUT_DIR = "output/new_selected_raw"
os.makedirs(OUT_DIR, exist_ok=True)

KEEP_COLS = [
    "date", "TICKER", "PERMNO", "COMNAM", "SHRCLS", "NAMEENDT",
    "RET", "VOL_CHANGE", "BA_SPREAD", "ILLIQUIDITY", "sprtrn",
    "TURNOVER", "PRC", "SHROUT", "MARKET_CAP", "TRAN_COST", "ASK", "BID",
]

for ticker in TICKERS:
    df = pd.read_csv(os.path.join(IN_DIR, f"{ticker}.csv"), low_memory=False)

    # ── 1. date filter ────────────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= "2000-01-01") & (df["date"] <= "2023-12-31")].copy()
    df = df.sort_values("date").reset_index(drop=True)

    # ── 2. drop bad RET rows (CRSP codes 'C' = no valid price, 'B' = bad data)
    df = df[~df["RET"].isin(["C", "B"])]
    df["RET"] = pd.to_numeric(df["RET"], errors="coerce")

    # numeric casts for fields used in calculations
    for col in ["PRC", "VOL", "SHROUT", "ASK", "BID"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # PRC in CRSP can be negative (bid-ask midpoint when no trade); take abs
    df["PRC"] = df["PRC"].abs()

    # ── 3. add predictors (mirrors stock.py) ─────────────────────────────────
    df["VOL_CHANGE"]  = df["VOL"].pct_change(fill_method=None)
    df["BA_SPREAD"]   = (df["ASK"] - df["BID"]) / df["PRC"]
    df["ILLIQUIDITY"] = df["RET"] / (df["VOL"] * df["PRC"])
    df["TURNOVER"]    = df["VOL"] / df["SHROUT"]
    df["TRAN_COST"]   = (df["ASK"] - df["BID"]) / 2
    df["MARKET_CAP"]  = df["PRC"] * df["SHROUT"]

    # replace inf with nan
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # ── 4. keep only target columns ───────────────────────────────────────────
    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"  [WARN] {ticker}: missing columns {missing}, filling with NaN")
        for c in missing:
            df[c] = np.nan

    df = df[KEEP_COLS]

    # ── 5. save ───────────────────────────────────────────────────────────────
    out_path = os.path.join(OUT_DIR, f"{ticker}.csv")
    df.to_csv(out_path, index=False)
    print(f"  {ticker:<6} rows={len(df):>5}  "
          f"{df['date'].min().date()} → {df['date'].max().date()}  "
          f"cols={list(df.columns)[:4]}…")

print(f"\nSaved {len(TICKERS)} files to {OUT_DIR}/")
