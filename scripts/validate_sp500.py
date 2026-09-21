"""
Validate all per-company CSVs in output/sp500_individual/.
Checks per file:
  1. Duplicate dates
  2. Backward date jumps
  3. Gaps > GAP_WARN_DAYS calendar days (with context: what happened around the gap)
  4. Ticker/company-name transitions across the history
  5. Very short histories (< MIN_ROWS trading days)
  6. Rows where RET is null for extended runs (> NULL_RUN_WARN consecutive rows)

Writes:
  output/sp500_individual/validation_report.txt   — full detail
  output/sp500_individual/validation_summary.csv  — one row per file, machine-readable
"""

import os, glob
import pandas as pd
import numpy as np

IN_DIR          = "output/sp500_individual"
GAP_WARN_DAYS   = 30    # calendar days
MIN_ROWS        = 100   # flag stocks with very little data
NULL_RUN_WARN   = 20    # consecutive null RET rows

SKIP  = {"validation_summary.csv"}
files = sorted(
    f for f in glob.glob(os.path.join(IN_DIR, "*.csv"))
    if os.path.basename(f) not in SKIP
)
print(f"Validating {len(files)} files …\n")

summary_rows = []
report_lines = []

def section(title):
    report_lines.append(f"\n{'─'*72}")
    report_lines.append(f"  {title}")
    report_lines.append(f"{'─'*72}")

for fpath in files:
    ticker = os.path.splitext(os.path.basename(fpath))[0]
    df = pd.read_csv(fpath, low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    issues = []

    # ── basic stats ───────────────────────────────────────────────────────────
    n_rows   = len(df)
    date_min = df["date"].min().date()
    date_max = df["date"].max().date()
    permno   = df["PERMNO"].iloc[0] if "PERMNO" in df.columns else "?"

    # Ticker / company name transitions
    transitions = []
    prev_ticker = prev_name = None
    for _, row in df[["date","TICKER","COMNAM"]].drop_duplicates(
            subset=["TICKER","COMNAM"]).iterrows():
        t = str(row.get("TICKER","")).strip()
        n = str(row.get("COMNAM","")).strip()
        if t != prev_ticker or n != prev_name:
            transitions.append((row["date"].date(), t, n))
            prev_ticker, prev_name = t, n

    history_str = " → ".join(
        f"{t}[{d}]" if t == transitions[-1][1] else f"{t}[{d}]"
        for d, t, n in transitions
    )
    name_history = " → ".join(f"{n}[{d}]" for d, t, n in transitions)

    # ── 1. duplicate dates ────────────────────────────────────────────────────
    dupes = df[df.duplicated("date", keep=False)]
    if not dupes.empty:
        issues.append(f"DUPLICATE DATES ({len(dupes)} rows): "
                      + ", ".join(str(d.date()) for d in dupes["date"].unique()[:5]))

    # ── 2. backward jumps ─────────────────────────────────────────────────────
    diffs = df["date"].diff().dt.days
    bw = diffs[diffs < 0]
    if not bw.empty:
        for idx in bw.index:
            issues.append(
                f"BACKWARD JUMP at idx {idx}: "
                f"{df.at[idx-1,'date'].date()} → {df.at[idx,'date'].date()}"
            )

    # ── 3. large gaps ─────────────────────────────────────────────────────────
    gaps = diffs[diffs > GAP_WARN_DAYS]
    for idx in gaps.index:
        d_before = df.at[idx-1, "date"].date()
        d_after  = df.at[idx,   "date"].date()
        t_before = df.at[idx-1, "TICKER"] if "TICKER" in df.columns else "?"
        t_after  = df.at[idx,   "TICKER"] if "TICKER" in df.columns else "?"
        n_before = df.at[idx-1, "COMNAM"] if "COMNAM" in df.columns else "?"
        n_after  = df.at[idx,   "COMNAM"] if "COMNAM" in df.columns else "?"
        issues.append(
            f"GAP {int(diffs[idx])} days: {d_before} ({t_before} / {n_before}) "
            f"→ {d_after} ({t_after} / {n_after})"
        )

    # ── 4. short history ──────────────────────────────────────────────────────
    if n_rows < MIN_ROWS:
        issues.append(f"SHORT HISTORY: only {n_rows} rows")

    # ── 5. long null-RET runs ─────────────────────────────────────────────────
    if "RET" in df.columns:
        ret_null = df["RET"].isna()
        run = max_run = 0
        for v in ret_null:
            run = run + 1 if v else 0
            max_run = max(max_run, run)
        if max_run >= NULL_RUN_WARN:
            issues.append(f"LONG NULL-RET RUN: {max_run} consecutive missing returns")

    # ── record ────────────────────────────────────────────────────────────────
    status = "OK" if not issues else "WARN"
    summary_rows.append({
        "ticker":       ticker,
        "permno":       permno,
        "status":       status,
        "rows":         n_rows,
        "date_start":   str(date_min),
        "date_end":     str(date_max),
        "ticker_history": history_str,
        "name_history": name_history,
        "issue_count":  len(issues),
        "issues":       " | ".join(issues),
    })

    if issues:
        section(f"{ticker}  PERMNO={permno}  {date_min} → {date_max}  rows={n_rows}")
        report_lines.append(f"  Ticker history : {history_str}")
        report_lines.append(f"  Name history   : {name_history}")
        for iss in issues:
            report_lines.append(f"  !! {iss}")

# ── write report ──────────────────────────────────────────────────────────────
ok_count   = sum(1 for r in summary_rows if r["status"] == "OK")
warn_count = sum(1 for r in summary_rows if r["status"] == "WARN")

header = [
    "Validation Report",
    "="*72,
    f"Files validated : {len(files)}",
    f"  OK            : {ok_count}",
    f"  WARN          : {warn_count}",
]

report_path = os.path.join(IN_DIR, "validation_report.txt")
with open(report_path, "w") as f:
    f.write("\n".join(header) + "\n")
    f.write("\n".join(report_lines) + "\n")

summary_df = pd.DataFrame(summary_rows)
summary_path = os.path.join(IN_DIR, "validation_summary.csv")
summary_df.to_csv(summary_path, index=False)

print(f"Done.  OK={ok_count}  WARN={warn_count}")
print(f"Report : {report_path}")
print(f"Summary: {summary_path}")

# Print warn tickers to console
warn_tickers = [r["ticker"] for r in summary_rows if r["status"] == "WARN"]
if warn_tickers:
    print(f"\nWARN tickers ({len(warn_tickers)}): {', '.join(warn_tickers)}")
