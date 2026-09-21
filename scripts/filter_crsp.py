"""
Filter 2025-CRSP.csv to retain only rows from 2000-01-01 onward.
Output: datasets/2025-CRSP-filtered.csv
"""

import pandas as pd

INPUT = "datasets/2025-CRSP.csv"
OUTPUT = "datasets/2025-CRSP-filtered.csv"
START_DATE = "2000-01-01"

print(f"Reading {INPUT} ...")
df = pd.read_csv(INPUT, dtype=str)
print(f"  Total rows: {len(df):,}")

df["date"] = pd.to_datetime(df["date"])
mask = df["date"] >= START_DATE
df_filtered = df[mask].copy()
print(f"  Rows from {START_DATE} onward: {len(df_filtered):,}")

df_filtered.to_csv(OUTPUT, index=False)
print(f"Saved to {OUTPUT}")
