"""
Fix duplicate-date rows in per-company CSVs.

All duplicates are caused by CRSP recording multiple dividend distributions
on the same trading day (e.g. DISTCD 1232 = regular div, 1272 = special div).
PRC and RET are identical across the duplicates; only DIVAMT/DISTCD differ.

Resolution: for each (PERMNO, date) group with >1 row, sum DIVAMT and
keep the first row for all other fields. DISTCD is set to 0 (combined)
when multiple distribution codes are present on the same date.

Rewrites only the files that have duplicates (in-place).
"""

import os, glob
import pandas as pd

IN_DIR = "output/sp500_individual"
SKIP = {"validation_summary.csv"}
files  = sorted(
    f for f in glob.glob(os.path.join(IN_DIR, "*.csv"))
    if os.path.basename(f) not in SKIP
)

fixed = 0
for fpath in files:
    ticker = os.path.splitext(os.path.basename(fpath))[0]
    df = pd.read_csv(fpath, low_memory=False)
    df["date"] = pd.to_datetime(df["date"])

    dupes = df.duplicated("date", keep=False)
    if not dupes.any():
        continue  # nothing to do

    n_before = len(df)

    # For duplicated dates: sum DIVAMT, mark DISTCD=0 (combined), keep first otherwise
    dup_dates  = df.loc[df.duplicated("date", keep=False), "date"].unique()
    clean_rows = df[~df["date"].isin(dup_dates)]

    merged_parts = []
    for dt in dup_dates:
        g = df[df["date"] == dt]
        out = g.iloc[[0]].copy()
        if "DIVAMT" in g.columns:
            out["DIVAMT"] = g["DIVAMT"].sum()
        if "DISTCD" in g.columns:
            codes = g["DISTCD"].dropna().unique()
            out["DISTCD"] = 0 if len(codes) > 1 else (codes[0] if len(codes) == 1 else float("nan"))
        merged_parts.append(out)

    df = (
        pd.concat([clean_rows] + merged_parts)
          .sort_values("date")
          .reset_index(drop=True)
    )

    n_after = len(df)
    df.to_csv(fpath, index=False)
    fixed += 1
    print(f"  {ticker:<8}: {n_before} → {n_after} rows  (removed {n_before - n_after} duplicate rows)")

print(f"\nFixed {fixed} files.")
