"""
Build the pooled datasets from output/mid_highmid_20yr_portfolios (the 9-column
export written by build_portfolio_sets.py).

The other scripts in this folder do the actual work and are run unmodified, one
subprocess each; this file only chains them and fixes the paths. See
pooled/README.md for what each step does and why.

  examm  (ICAIF paper): 7-column copy -> clean -> walk-forward split -> align
                        -> combined 300-input panel + cross-sectional target
  onenas (IAAI paper):  9-column export -> clean -> prep_panel (needs PRC/TRAN_COST)

Steps:
  1. examm only: project the export down to the 7 training columns (text
     pass-through, values untouched) so the cohort files match the paper's.
  2. Clean each export with pooled/clean_mid_highmid.py.
  3. Run the per-paper builders for every requested set.

Usage:
    python pooled/build_pooled_dataset.py                       # both papers, all 4 sets
    python pooled/build_pooled_dataset.py --target onenas --sets set1
    python main.py pooled ...                                   # same thing
"""

import argparse
import csv
import subprocess
import sys
from pathlib import Path

POOLED = Path(__file__).resolve().parent
REPO = POOLED.parent

IN_DIR = "output/mid_highmid_20yr_portfolios"
OUT_DIR = "output/pooled"

ALL_SETS = ["set1", "set2", "set3", "set4"]
TRAIN_COLS = ["date", "RET", "VOL_CHANGE", "BA_SPREAD", "ILLIQUIDITY", "sprtrn", "TURNOVER"]
PRICE_COLS = ["PRC", "TRAN_COST"]


def run_script(script, *args):
    cmd = [sys.executable, str(POOLED / script)] + [str(a) for a in args]
    print(f"\n>>> {script} {' '.join(str(a) for a in args)}", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f"ERROR: {script} failed (exit {result.returncode}) -- stopping")


def check_export(in_dir, sets, need_price):
    need = TRAIN_COLS + (PRICE_COLS if need_price else [])
    for set_name in sets:
        files = sorted((in_dir / set_name).glob("*.csv"))
        if not files:
            sys.exit(f"ERROR: no CSVs in {in_dir / set_name} -- run build_portfolio_sets.py first")
        with open(files[0], newline="") as f:
            header = next(csv.reader(f))
        missing = [c for c in need if c not in header]
        if missing:
            sys.exit(f"ERROR: {files[0]} is missing columns {missing}")


def project_to_train_cols(in_dir, out_dir, sets):
    """Copy each ticker CSV keeping only TRAIN_COLS; fields are passed through as text."""
    for set_name in sets:
        dst = out_dir / set_name
        dst.mkdir(parents=True, exist_ok=True)
        for src in sorted((in_dir / set_name).glob("*.csv")):
            with open(src, newline="") as f:
                reader = csv.reader(f)
                header = next(reader)
                idx = [header.index(c) for c in TRAIN_COLS]
                with open(dst / src.name, "w", newline="") as g:
                    writer = csv.writer(g)
                    writer.writerow(TRAIN_COLS)
                    for row in reader:
                        writer.writerow([row[i] for i in idx])
    print(f"wrote 7-column export -> {out_dir}")


def build_examm(in_dir, out_dir, sets, cutoff, val_year, test_year):
    raw7 = out_dir / "raw_7col"
    clean7 = out_dir / "clean_7col"
    project_to_train_cols(in_dir, raw7, sets)
    run_script("clean_mid_highmid.py", "--in", raw7, "--out", clean7, "--sets", *sets)

    for set_name in sets:
        cohort = out_dir / "examm" / set_name / f"cohort_{cutoff[:4]}"
        aligned = Path(f"{cohort}_aligned")
        run_script("examm/build_walkforward_splits.py", "--continuous",
                   "--source-dir", clean7 / set_name,
                   "--cutoff", cutoff, "--val-year", val_year, "--test-year", test_year,
                   "--out", cohort)
        run_script("examm/align_cohort.py", "--in", cohort, "--out", aligned)
        run_script("examm/build_combined_panel.py",
                   "--src", aligned, "--out", f"{aligned}_combined")
        run_script("examm/build_cs_target.py", "--in", aligned, "--out", f"{aligned}_cs")


def build_onenas(in_dir, out_dir, sets, feature_set, stats_end):
    clean9 = out_dir / "clean_9col"
    run_script("clean_mid_highmid.py", "--in", in_dir, "--out", clean9, "--sets", *sets)

    for set_name in sets:
        run_script("onenas/prep_panel.py", clean9 / set_name, out_dir / "onenas" / set_name,
                   "--feature-set", feature_set, "--stats-end", stats_end)


def build_pooled_dataset(target="both", sets=None, in_dir=IN_DIR, out_dir=OUT_DIR,
                         cutoff="2020-12-31", val_year="2021", test_year="2022",
                         feature_set="core7", stats_end="2019-12-31"):
    sets = sets or ALL_SETS
    in_dir = (REPO / in_dir).resolve()
    out_dir = (REPO / out_dir).resolve()

    check_export(in_dir, sets, need_price=target in ("onenas", "both"))

    if target in ("examm", "both"):
        build_examm(in_dir, out_dir, sets, cutoff, str(val_year), str(test_year))
    if target in ("onenas", "both"):
        build_onenas(in_dir, out_dir, sets, feature_set, stats_end)

    print(f"\nDone. Pooled datasets -> {out_dir}")


def add_arguments(ap):
    ap.add_argument("--target", choices=["examm", "onenas", "both"], default="both")
    ap.add_argument("--sets", nargs="+", default=ALL_SETS)
    ap.add_argument("--in", dest="in_dir", default=IN_DIR)
    ap.add_argument("--out", dest="out_dir", default=OUT_DIR)
    ap.add_argument("--cutoff", default="2020-12-31", help="examm: last training date")
    ap.add_argument("--val-year", default="2021", help="examm")
    ap.add_argument("--test-year", default="2022", help="examm")
    ap.add_argument("--feature-set", choices=["legacy", "core7"], default="core7", help="onenas")
    ap.add_argument("--stats-end", default="2019-12-31", help="onenas: end of burn-in span")


def run_from_args(args):
    build_pooled_dataset(args.target, args.sets, args.in_dir, args.out_dir,
                         args.cutoff, args.val_year, args.test_year,
                         args.feature_set, args.stats_end)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_arguments(parser)
    run_from_args(parser.parse_args())
