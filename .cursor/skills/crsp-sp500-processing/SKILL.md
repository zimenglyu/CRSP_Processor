---
name: crsp-sp500-processing
description: How we process CRSP S&P 500 data (sp-500-24.csv) into one file per company (PERMCO), and post-process (drop pre-2000, select A over B for dual-class).
---

# CRSP S&P 500 Processing Pipeline

Use this skill when working with `datasets/sp-500-24.csv` or the split output in `datasets/sp500_by_permco/`. It documents the rules and steps we use to produce one file per company and clean dual-class cases.

## 1. Split by PERMCO (time-contiguous blocks)

**Entry point:** `main.py` → `split_sp500_by_permco(data_path, output_dir, chunksize=100000)`.

- **Input:** `datasets/sp-500-24.csv` (or similar CRSP-style CSV with `date`, `PERMCO`).
- **Output:** One CSV per company in `datasets/sp500_by_permco/`, file name = **PERMCO** (e.g. `21287.csv`).
- **Filter key:** **PERMCO** (not PERMNO). Rows with missing or invalid PERMCO are skipped.
- **Order:** We do **not** shuffle. Rows stay in original dataset order.
- **Slicing rule (only rule that starts a new file):**
  - When **date goes backwards** (`row_date < last_date`), we flush the current block to a file and start a new block.
- **When PERMCO changes but time does not go back:** We do **not** slice. We keep appending to the same block and set the output file name to the **latest PERMCO** in that block (so one file can contain multiple PERMCOs over time; we name it by the last one).
- **Chunking:** Data is read in chunks (default 100k rows); state is carried across chunks (current block, last_date, current_permco).

## 2. Drop files ending before 2000-01-01

- **Rule:** For each CSV in the PERMCO folder, read the **last row** and its `date`.
- If the last row’s date is **before 2000-01-01** (or missing), **delete that file**.
- This keeps only companies whose series extend at least into the year 2000.

## 3. Dual-class (A/B): keep A, delete B

- **When:** Same company, same **last row date**, but **different PERMNO/PERMCO** (e.g. Class A and Class B shares).
- **How to detect:** Group files by `(COMNAM, last_row_date)`. If a group has more than one file and the **SHRCLS** values in the last row include both `A` and `B`, it’s a dual-class pair.
- **Rule:** **Keep the file where the last row has SHRCLS=A; delete the file(s) where the last row has SHRCLS=B.**
- Examples we applied: NEWS CORP (13963 A / 13964 B), FOX CORP (18420 A / 18421 B), Lennar (52708 A / 89731 B), Molson Coors (90562 A / 59248 B), Travelers (89346 A / 89495 B), Crown Central (31042 A / 61532 B). The B files were deleted.

## Summary table

| Step | What | Key rule |
|------|------|----------|
| Split | PERMCO-based, time-contiguous blocks | Slice only when date goes backwards; file name = latest PERMCO |
| Drop old | Last row before 2000-01-01 | Delete file |
| Dual-class | Same company, same end date, A and B | Keep SHRCLS=A file, delete SHRCLS=B file(s) |

## Code references

- Split logic: `main.py` → `split_sp500_by_permco()`.
- Default run: `__main__` calls `split_sp500_by_permco` with `data_path="datasets/sp-500-24.csv"` and `output_dir="datasets/sp500_by_permco"`.
- Post-processing (steps 2–3) has been done with one-off Python scripts (pandas over the CSV folder); no separate module yet.
