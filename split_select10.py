"""
Select 10 lowest market-cap stocks from new_selected_raw,
split each chronologically 70/15/15, save to 2023_select_10_701515_raw/.
"""

import os, pandas as pd, numpy as np

IN_DIR  = "output/new_selected_raw"
OUT_DIR = "output/2023_select_10_701515_raw"
os.makedirs(OUT_DIR, exist_ok=True)

ALL_18 = ["APA","DGX","IP","KEY","NI","NTAP","SNA","WSM",
          "ZBRA","SJM","RF","RVTY","EME","MAA","WAB","BAX","CMS","FITB"]

# Compute 2023 median market cap for each
cap = {}
for t in ALL_18:
    df = pd.read_csv(f"{IN_DIR}/{t}.csv", usecols=["date","PRC","SHROUT"], low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    yr = df[df["date"].dt.year == 2023].copy()
    yr["mc"] = pd.to_numeric(yr["PRC"], errors="coerce") * pd.to_numeric(yr["SHROUT"], errors="coerce")
    cap[t] = yr["mc"].median() / 1e6

cap_series = pd.Series(cap).sort_values()
top10 = cap_series.head(10)
print("10 lowest market caps (2023 median, $B):")
for t, v in top10.items():
    print(f"  {t:<6} ${v:.1f}B")

# Split each
print()
for ticker in top10.index:
    df = pd.read_csv(f"{IN_DIR}/{ticker}.csv", low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    n = len(df)
    n_train = round(n * 0.70)
    n_val   = round(n * 0.15)
    n_test  = n - n_train - n_val

    train = df.iloc[:n_train]
    val   = df.iloc[n_train : n_train + n_val]
    test  = df.iloc[n_train + n_val :]

    train.to_csv(f"{OUT_DIR}/{ticker}_train.csv", index=False)
    val.to_csv(  f"{OUT_DIR}/{ticker}_val.csv",   index=False)
    test.to_csv( f"{OUT_DIR}/{ticker}_test.csv",  index=False)

    print(f"  {ticker:<6} total={n}  "
          f"train={len(train)} ({train['date'].min().date()}→{train['date'].max().date()})  "
          f"val={len(val)} ({val['date'].min().date()}→{val['date'].max().date()})  "
          f"test={len(test)} ({test['date'].min().date()}→{test['date'].max().date()})")

print(f"\nDone. {len(top10)*3} files saved to {OUT_DIR}/")
