"""
Extract one CSV per S&P 500 company from the CRSP filtered dataset.

Strategy:
  - For each ticker in sp500_tickers.txt, find the PERMNO whose last
    trading row carries that ticker.  That PERMNO is the current security.
  - Pull ALL rows for that PERMNO (covers the full history even if the
    company traded under a different ticker earlier, e.g. FB -> META).
  - Sort by date and run anomaly checks (backward date jumps, large gaps).
  - Save to output/sp500_individual/{TICKER}.csv

Anomaly report is written to output/sp500_individual/anomaly_report.txt
"""

import os
import pandas as pd

# ── config ────────────────────────────────────────────────────────────────────
CRSP_CSV    = "datasets/2025-CRSP-filtered.csv"
TICKERS_TXT = "tickers/sp500_tickers.txt"
OUT_DIR     = "output/sp500_individual"
GAP_WARN_DAYS = 30          # calendar days; gap larger than this gets flagged

os.makedirs(OUT_DIR, exist_ok=True)

# ── load data ─────────────────────────────────────────────────────────────────
print("Loading CRSP data …")
df = pd.read_csv(CRSP_CSV, low_memory=False)
df["date"] = pd.to_datetime(df["date"])
df["PERMNO"] = df["PERMNO"].astype(str)

with open(TICKERS_TXT) as f:
    tickers = [line.strip() for line in f if line.strip()]

print(f"Loaded {len(df):,} rows | {len(tickers)} tickers to process\n")

# ── build a lookup: for every (TICKER, PERMNO) pair, what is the last date? ──
# This lets us identify which PERMNO is the *current* company for a given ticker.
last_date_by_ticker_permno = (
    df.groupby(["TICKER", "PERMNO"])["date"].max().reset_index()
)

anomalies = []
not_found  = []
saved      = []

for ticker in tickers:
    candidates = last_date_by_ticker_permno[
        last_date_by_ticker_permno["TICKER"] == ticker
    ]

    if candidates.empty:
        not_found.append(ticker)
        print(f"  [NOT FOUND] {ticker}")
        continue

    # Pick the PERMNO whose last ticker==ticker row is the most recent.
    # That is the current (or most recently active) company with this ticker.
    best_row = candidates.loc[candidates["date"].idxmax()]
    permno   = best_row["PERMNO"]
    last_dt  = best_row["date"].date()

    # If multiple PERMNOs share the same max date, warn but still pick first.
    if (candidates["date"] == best_row["date"]).sum() > 1:
        tied = candidates[candidates["date"] == best_row["date"]]["PERMNO"].tolist()
        anomalies.append(
            f"[TIE] {ticker}: multiple PERMNOs share latest date {last_dt}: {tied}. "
            f"Picked {permno}."
        )

    # Pull full history for this PERMNO (all dates, all tickers it ever had)
    company_df = df[df["PERMNO"] == permno].sort_values("date").reset_index(drop=True)

    # ── anomaly checks ────────────────────────────────────────────────────────
    diffs = company_df["date"].diff().dt.days

    # 1. Backward date jumps (should never happen within one PERMNO)
    backward = diffs[diffs < 0]
    if not backward.empty:
        for idx in backward.index:
            anomalies.append(
                f"[BACKWARD] {ticker} PERMNO={permno}: date goes backward at "
                f"index {idx}, row date={company_df.at[idx,'date'].date()}, "
                f"prev date={company_df.at[idx-1,'date'].date()}"
            )

    # 2. Large forward gaps
    large_gaps = diffs[diffs > GAP_WARN_DAYS]
    if not large_gaps.empty:
        for idx in large_gaps.index:
            anomalies.append(
                f"[GAP] {ticker} PERMNO={permno}: {int(diffs[idx])}-day gap between "
                f"{company_df.at[idx-1,'date'].date()} and {company_df.at[idx,'date'].date()}"
            )

    # ── ticker history summary for logging ───────────────────────────────────
    ticker_history = (
        company_df.groupby("TICKER")["date"]
        .agg(["min", "max"])
        .sort_values("min")
    )
    history_str = ", ".join(
        f"{t}({r['min'].date()}→{r['max'].date()})"
        for t, r in ticker_history.iterrows()
    )

    # ── save ──────────────────────────────────────────────────────────────────
    out_path = os.path.join(OUT_DIR, f"{ticker}.csv")
    company_df.to_csv(out_path, index=False)
    saved.append(ticker)

    print(
        f"  [OK] {ticker:<8} PERMNO={permno}  "
        f"{company_df['date'].min().date()} → {company_df['date'].max().date()}  "
        f"rows={len(company_df):>5}  ticker history: {history_str}"
    )

# ── write anomaly report ──────────────────────────────────────────────────────
report_path = os.path.join(OUT_DIR, "anomaly_report.txt")
with open(report_path, "w") as f:
    f.write(f"Anomaly Report\n{'='*70}\n\n")
    f.write(f"Tickers requested : {len(tickers)}\n")
    f.write(f"Files saved       : {len(saved)}\n")
    f.write(f"Not found in CRSP : {len(not_found)}\n")
    if not_found:
        f.write("  " + ", ".join(not_found) + "\n")
    f.write(f"\nAnomalies ({len(anomalies)}):\n")
    for a in anomalies:
        f.write(f"  {a}\n")

print(f"\nDone. {len(saved)}/{len(tickers)} files saved to {OUT_DIR}/")
print(f"Not found: {len(not_found)}  Anomalies: {len(anomalies)}")
print(f"Anomaly report: {report_path}")
