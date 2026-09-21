# CRSP_Processor

This repository processes daily stock data from the
[CRSP](https://wrds-www.wharton.upenn.edu/pages/about/data-vendors/center-for-research-in-security-prices-crsp/)
database (S&P 500 companies) into clean per-company files, portfolio sets, and
the pooled datasets used for training.

## Setup

```
pip install pandas numpy
```

Place the raw CRSP daily export at `datasets/2025-CRSP.csv`. The file is a
single CSV with all companies stacked row by row. All commands are run from
the repository root.

## Layout

| Folder | Contents |
|---|---|
| `scripts/` | Processing steps, run one at a time in the order below |
| `pooled/` | Pooled dataset construction (details in `pooled/README.md`) |
| `dataloader/` | Classes used by the older pipeline in `main.py` |
| `tools/` | Small helpers (plots, ticker ranking) |
| `tickers/` | Ticker lists and S&P 500 company information |
| `datasets/`, `output/` | Input data and results (not tracked in git) |

## Usage

**1. Per-company files**

```
python scripts/filter_crsp.py        # keep 2000 onward
python scripts/extract_sp500.py      # one CSV per S&P 500 ticker
python scripts/dedup_sp500.py        # merge duplicate-date rows
python scripts/validate_sp500.py     # check for gaps and duplicates
```

Output: `output/sp500_individual/<TICKER>.csv`

**2. Portfolio sets**

```
python scripts/build_portfolio_sets.py
```

Selects mid and high-mid cap stocks with at least 20 years of history, adds
the predictors, and writes 4 sets of 50 stocks to
`output/mid_highmid_20yr_portfolios/set1..set4/`.

**3. Pooled datasets**

```
python main.py pooled                              # both papers, all 4 sets
python main.py pooled --target onenas --sets set1  # a single target and set
```

Cleans the portfolio sets and builds the pooled data in `output/pooled/`:
`examm/` (one wide 300-input panel per set) and `onenas/` (50 per-stock files
plus a shared calendar per set). All options are listed by
`python main.py pooled --help`.

**Optional: 10-stock dataset**

```
python scripts/process_new_selected.py
python scripts/split_select10.py
```

Writes a 70/15/15 train/validation/test split to
`output/2023_select_10_701515_raw/`.

## Predictors

| Column | Formula |
|---|---|
| `VOL_CHANGE` | daily percentage change of `VOL` |
| `BA_SPREAD` | `(ASK - BID) / PRC` |
| `ILLIQUIDITY` | `RET / (VOL * PRC)` |
| `TURNOVER` | `VOL / SHROUT` |
| `TRAN_COST` | `(ASK - BID) / 2` |

## Note on tickers

Companies are matched by `PERMNO` rather than `TICKER`. Tickers change over
time (FB became META) and are reused by unrelated companies, whereas `PERMNO`
is permanent.
