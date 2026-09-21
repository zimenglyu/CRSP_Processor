# Pooled dataset construction, both submitted papers

Everything that turns CRSP output into what the two papers train on.

In this repo, `build_pooled_dataset.py` (or `python main.py pooled`) runs all of
the steps below in order on `output/mid_highmid_20yr_portfolios/` and writes to
`output/pooled/`. The scripts can still be run one at a time as described here.

The papers share one cleaning step and then diverge: they pool the same 50
names into two different shapes, so there is no single "pooled dataset
builder" -- there are two, with one cleaner ahead of both.

```
CRSP_Processor
   |
   +-- 7-column export   date,RET,VOL_CHANGE,BA_SPREAD,ILLIQUIDITY,sprtrn,TURNOVER
   +-- 9-column export   ...the same, plus PRC,TRAN_COST
              |
       clean_mid_highmid.py     one script, run once per export
              |
   +-- cleaned 7-column  --> examm/    build_walkforward_splits -> align_cohort
   |                                   -> build_combined_panel, build_cs_target
   |                                   ICAIF paper, one wide 300-input file
   |
   +-- cleaned 9-column  --> onenas/   prep_panel.py
                                       IAAI paper, 50 per-stock CSVs + calendar
```

**The two exports are the thing to get right.** `clean_mid_highmid.py`
preserves `PRC` and `TRAN_COST` only when the input already has them
(it detects this at line 202 and keeps them at line 304). EXAMM trains
without prices; ONE-NAS needs them, because the trading book prices
positions and charges the spread. Run the cleaner on the **price-bearing**
export for the ONE-NAS path, or `prep_panel.py` dies with
`KeyError: 'PRC'`.

Dependencies: `clean_mid_highmid.py` needs pandas and numpy,
`build_combined_panel.py` needs pandas, the other three and
`prep_panel.py` are pure standard library. None of the six contains an
absolute path, and all six run standalone from this folder.

## 1. clean_mid_highmid.py -- shared, run first, once per export

```
python3 clean_mid_highmid.py --in <raw_sets_dir> --out <clean_sets_dir> \
                             [--sets set1 set2 set3 set4]
```

Resolves blank fields per ticker. Output shape equals input shape: one
`<TICKER>.csv` per stock. It does **not** pool.

Not cosmetic. EXAMM's C++ CSV parser is positional with no date-awareness:
on a blank field it logs an error and skips appending that one value to
that one column, silently misaligning it against every other column from
that row to the end of the file. Not a crash -- wrong data. This resolves
every blank first, or refuses to write. Rule ordering matters and is
documented in the script header.

On the price-bearing export it also repairs `TRAN_COST`, which is
`BA_SPREAD * PRC / 2` exactly; where it is blank but recomputable it is
recomputed, and where it is not, the script fails rather than guessing.

## 2a. examm/ -- the ICAIF paper's pooled panel

```
python3 build_walkforward_splits.py --continuous \
        --source-dir <clean_7col>/set1 \
        --cutoff 2020-12-31 --val-year 2021 --test-year 2022 \
        --out <wf>/set1/cohort_2020

python3 align_cohort.py        --in <wf>/set1/cohort_2020 \
                               --out <wf>/set1/cohort_2020_aligned
python3 build_combined_panel.py --src <wf>/set1/cohort_2020_aligned \
                                --out <wf>/set1/cohort_2020_aligned_combined
python3 build_cs_target.py      --in  <wf>/set1/cohort_2020_aligned \
                                --out <wf>/set1/cohort_2020_aligned_cs
```

`--cutoff`, `--val-year`, `--test-year` and `--out` are all required on
the first; `--in` and `--out` are both required on the other three.

`align_cohort.py` is the load-bearing one. Stocks IPO on different dates,
so row index `j` is not the same date across files. It truncates to the
common start and then *verifies* the calendars are byte-identical across
every stock, failing loudly rather than proceeding misaligned. The
cross-sectional IC loss is meaningless without that.

`build_combined_panel.py` is the pooling step: 50 stocks x 6 predictors
become one 300-input, 50-output series. `build_cs_target.py` adds the
cross-sectional z-scored target (Qlib's CSZScoreNorm).

## 2b. onenas/ -- the IAAI paper's pooled panel

```
python3 prep_panel.py <clean_9col>/set1 <out_dir> \
        --feature-set core7 --stats-end 2019-12-31
python3 prep_panel.py --selftest
```

`set_dir` and `out_dir` are positional. Neither flag is a default and both
matter: `core7` selects the deployed feature set (`legacy` is the default
and reproduces the older eight-column layout byte-identically), and
`--stats-end` confines every normalisation statistic to the burn-in span,
which is the leakage guard.

Input must carry exactly:

```
date,RET,VOL_CHANGE,BA_SPREAD,ILLIQUIDITY,sprtrn,TURNOVER,PRC,TRAN_COST
```

All nine are read regardless of `--feature-set` -- under `core7` neither
`sprtrn` nor `VOL_CHANGE` reaches the output, but both must still be
present. Every ticker must share one trading calendar: after truncation
the script asserts equal row counts *and* identical date sequences, so a
single missing day stops the run rather than being filled. A missing
column surfaces as a bare `KeyError` from `csv.DictReader`.

`ILLIQUIDITY` must arrive as the **signed** Amihud ratio `RET/(VOL*PRC)`;
the script takes the absolute value itself. `PRC` is negative where CRSP
quotes a midpoint rather than a traded close.

Output: 50 per-stock numeric CSVs, `panel_dates.csv` carrying
`row, date, PRC_<T>, TC_<T>, RET_raw_<T>` so scoring can rebuild realised
returns without inverting any normalisation, and `panel_meta.json`
recording the settings each panel was built with.

`--selftest` runs with no data and checks the window arithmetic: that
`trailing_mean` is causal, that `REV21_1` excludes today, the `TURN_RATIO`
and `VOL21` windows, and that `RET_CS5` -- the one deliberately forward
column -- reads only rows `i+1..i+5` and is a target, never an input. The
burn-in prefix guard is not in the selftest; it is asserted on every real
run, since it depends on the data's dates.

## Provenance

The ONE-NAS settings are recovered from the panels actually used rather
than recalled: each panel's `panel_meta.json` records its feature set,
stats end, common start date and column list. The EXAMM invocations are
the production examples from each script's own header.
