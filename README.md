# CRSP_Processor

This repo processes daily stock data from the [CRSP](https://www.crsp.org/)
database (S&P 500 companies) into clean per-company files, portfolio sets, and
the pooled datasets used for training.

CRSP data is licensed, so no data is included. You need your own CRSP export.

## Setup

```
pip install pandas numpy
```

Put your raw CRSP daily export at `datasets/2025-CRSP.csv`. It is one big CSV
with every company stacked row by row. Run every command from the repo root.

## Layout

| Folder | What is in it |
|---|---|
| `scripts/` | The processing steps, run one at a time in the order below |
| `pooled/` | Builds the pooled datasets (details in `pooled/README.md`) |
| `dataloader/` | Classes used by the older pipeline in `main.py` |
| `tools/` | Small helpers (plots, ticker ranking) |
| `tickers/` | Ticker lists and S&P 500 company info |
| `datasets/`, `output/` | Your data and the results (not in git) |

## How to run

**1. Per-company files**

```
python scripts/filter_crsp.py        # keep 2000 onward
python scripts/extract_sp500.py      # one CSV per S&P 500 ticker
python scripts/dedup_sp500.py        # merge duplicate-date rows
python scripts/validate_sp500.py     # check for gaps and duplicates
```

Result: `output/sp500_individual/<TICKER>.csv`

**2. Portfolio sets**

```
python scripts/build_portfolio_sets.py
```

Picks mid and high-mid cap stocks with 20+ years of history, adds the
predictors, and writes 4 sets of 50 stocks to
`output/mid_highmid_20yr_portfolios/set1..set4/`.

**3. Pooled datasets**

```
python main.py pooled                              # both papers, all 4 sets
python main.py pooled --target onenas --sets set1  # just one
```

Cleans the portfolio sets and builds the pooled data in `output/pooled/`:
`examm/` (one wide 300-input panel per set) and `onenas/` (50 per-stock files
plus a shared calendar per set). Run `python main.py pooled --help` for the
options.

**Optional: the 10-stock dataset**

```
python scripts/process_new_selected.py
python scripts/split_select10.py
```

Writes a 70/15/15 train/val/test split to `output/2023_select_10_701515_raw/`.

## Predictors

| Column | Formula |
|---|---|
| `VOL_CHANGE` | daily % change of `VOL` |
| `BA_SPREAD` | `(ASK - BID) / PRC` |
| `ILLIQUIDITY` | `RET / (VOL * PRC)` |
| `TURNOVER` | `VOL / SHROUT` |
| `TRAN_COST` | `(ASK - BID) / 2` |

## Note on tickers

Companies are matched by `PERMNO`, not `TICKER`. Tickers change (FB became
META) and get reused by unrelated companies; `PERMNO` never changes.
