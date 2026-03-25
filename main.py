import os

import pandas as pd
from dataloader.dataloader import DataLoader


def read_words_from_file(file_path):
    with open(file_path, "r") as file:
        words = [line.strip() for line in file]
    return words


def create_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def get_class_B_stocks(data, ticker):
    if ticker == "BRK" or ticker == "BF":
        data = data[data["SHRCLS"] == "B"]
    return data


def find_permco(tickers, data_path):
    data = pd.read_csv(data_path, low_memory=False)
    print(f"length of data: {len(data)}")
    data["PERMNO"] = data["PERMNO"].astype(str)
    data["TICKER"] = data["TICKER"].astype(str)
    data["SHRCLS"] = data["SHRCLS"].astype(str)
    data["date"] = pd.to_datetime(data["date"])
    stock_info = {}
    for ticker in tickers:
        filter_data = data[data["TICKER"] == ticker]
        filter_data = get_class_B_stocks(filter_data, ticker)
        filter_data = filter_data.sort_values(by="date")
        filter_data = filter_data.tail(10)
        unique_values = filter_data["PERMNO"].unique()
        if len(unique_values) > 1:
            if "A" in filter_data["SHRCLS"].unique():
                filter_data = filter_data[filter_data["SHRCLS"] == "A"]
            elif "V" in filter_data["SHRCLS"].unique():
                filter_data = filter_data[filter_data["SHRCLS"] != "V"]
            else:
                print(
                    f"Ticker {ticker} has {len(unique_values)} permno values, and they are: {unique_values}"
                )
                print(
                    f"Ticker {ticker} has no class A stock or class V, this should not happen!"
                )
                filter_data.to_csv(f"./{ticker}_check.csv", index=False)
                print(f"File {ticker}_check.csv has been saved, please check it!")
            unique_values = filter_data["PERMNO"].unique()
        if len(unique_values) == 1:
            stock_info[ticker] = unique_values[0]
        elif len(unique_values) == 0:
            print(f"Ticker {ticker} permno code not found, this should not happen!")
        else:
            print(
                f"Ticker {ticker} has {len(unique_values)} permno values, and they are: {unique_values}"
            )

        latest_date = filter_data["date"].max()

        if pd.isnull(latest_date):
            print(f"Ticker {ticker} has no data, this should not happen!")
            continue
        if latest_date.year < 2023:
            print(f"Ticker {ticker} has data until {latest_date}, this should not happen!")

    return stock_info


def dict_to_csv(data_dict, file_path):
    df = pd.DataFrame(list(data_dict.items()), columns=["TICKER", "PERMNO"])
    df.to_csv(file_path, index=False)


def filter_large_data(new_data_path):
    old_data_path = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp500_new.csv"
    parameters = [
        "date",
        "TICKER",
        "PERMNO",
        "COMNAM",
        "SHRCLS",
        "NAMEENDT",
        "RET",
        "VOL",
        "sprtrn",
        "PRC",
        "SHROUT",
        "ASK",
        "BID",
    ]
    data = pd.read_csv(old_data_path, low_memory=False)
    data = data[parameters]
    data.to_csv(new_data_path, index=False)


def csv_to_dict(file_path):
    df = pd.read_csv(file_path)
    return dict(zip(df.ticker, df.permco))


def normalize_company_name(name):
    if not isinstance(name, str):
        return ""
    text = str(name).upper()
    for ch in [".", ",", "&", "-", "'", "\""]:
        text = text.replace(ch, " ")
    tokens = text.split()
    suffixes = {
        "INC",
        "INCORPORATED",
        "CORP",
        "CORPORATION",
        "CO",
        "COMPANY",
        "PLC",
        "LTD",
        "LIMITED",
        "SA",
        "NV",
    }
    core_tokens = [t for t in tokens if t not in suffixes]
    return " ".join(core_tokens)


def build_ticker_mapping(valid_tickers):
    mapping = {}
    for ticker in valid_tickers:
        mapping[ticker] = ticker
        base = ticker.split(".")[0]
        if base not in mapping:
            mapping[base] = ticker
    return mapping


def map_data_ticker_to_canonical(ticker, ticker_mapping, valid_tickers):
    if not isinstance(ticker, str):
        return None
    ticker = ticker.strip()
    if not ticker:
        return None
    if ticker in valid_tickers:
        return ticker
    return ticker_mapping.get(ticker)


def load_sp500_metadata(tickers_path, companies_path):
    tickers = read_words_from_file(tickers_path)
    valid_tickers = set(tickers)

    companies_df = pd.read_csv(companies_path)
    columns = list(companies_df.columns)

    symbol_col = None
    for candidate in ["Symbol", "SYMBOL", "Ticker", "TICKER"]:
        if candidate in columns:
            symbol_col = candidate
            break
    if symbol_col is None:
        raise ValueError("Could not find symbol column in sp500_companies.csv")

    name_col = None
    for candidate in ["Company Name", "COMPANY_NAME", "Name", "COMNAM"]:
        if candidate in columns:
            name_col = candidate
            break
    if name_col is None:
        raise ValueError("Could not find company name column in sp500_companies.csv")

    symbol_to_name = {}
    for _, row in companies_df.iterrows():
        symbol = str(row[symbol_col]).strip()
        company_name = str(row[name_col]).strip()
        if symbol:
            symbol_to_name[symbol] = company_name

    ticker_mapping = build_ticker_mapping(valid_tickers)
    return valid_tickers, symbol_to_name, ticker_mapping


def split_sp500_by_permco(data_path, output_dir, chunksize=100000):
    """
    Stream through the CRSP file and build time-contiguous blocks.

    - We never shuffle rows; we keep the original row order.
    - Filter/key by PERMCO: rows without valid PERMCO are skipped.
    - If PERMCO changes but time continues forward (no jump back),
      we KEEP the rows in the same block and update the file name
      to the latest PERMCO.
    - Only when time goes backwards (date decreases) do we slice
      and start a new block.
    - Each block is written to `<latest_permco>.csv` in `output_dir`.
    """
    create_dir(output_dir)

    reader = pd.read_csv(data_path, low_memory=False, chunksize=chunksize)

    try:
        first_chunk = next(reader)
    except StopIteration:
        print(f"No data found in {data_path}")
        return

    columns = list(first_chunk.columns)

    # Detect columns (use PERMCO for filtering and file naming)
    permco_col = None
    for candidate in ["PERMCO", "Permco", "permco"]:
        if candidate in columns:
            permco_col = candidate
            break
    if permco_col is None:
        raise ValueError("Could not find PERMCO column in dataset")

    date_col = None
    for candidate in ["date", "Date", "DATE"]:
        if candidate in columns:
            date_col = candidate
            break
    if date_col is None:
        raise ValueError("Could not find date column in dataset")

    # State for the current time-contiguous block
    current_block_rows = []
    last_date = None
    current_permco = None

    # Ensure proper types in the first chunk
    first_chunk = first_chunk.copy()
    first_chunk[date_col] = pd.to_datetime(first_chunk[date_col])
    first_chunk[permco_col] = first_chunk[permco_col].astype(str)

    def flush_current_block():
        nonlocal current_block_rows, last_date, current_permco
        if not current_block_rows or not current_permco:
            current_block_rows = []
            last_date = None
            current_permco = None
            return

        df_block = pd.DataFrame.from_records(current_block_rows)
        permco_str = str(current_permco).strip()
        if not permco_str or permco_str.lower() == "nan":
            # If we somehow ended up with an invalid PERMCO, drop the block.
            print("Warning: dropping block with invalid PERMCO for output name.")
        else:
            out_path = os.path.join(output_dir, f"{permco_str}.csv")
            header = not os.path.exists(out_path)
            df_block.to_csv(out_path, index=False, mode="a", header=header)

        current_block_rows = []
        last_date = None
        current_permco = None

    def process_chunk(chunk):
        nonlocal current_block_rows, last_date, current_permco
        chunk = chunk.copy()
        chunk[date_col] = pd.to_datetime(chunk[date_col])
        chunk[permco_col] = chunk[permco_col].astype(str)

        for _, row in chunk.iterrows():
            row_date = row[date_col]
            row_permco = str(row[permco_col]).strip()

            if not row_permco or row_permco.lower() == "nan":
                # Skip rows without a valid PERMCO entirely.
                continue

            if last_date is not None and row_date < last_date:
                # Time jumped backwards: close the current block and start a new one.
                flush_current_block()

            # We always append the row to the current block; this preserves order.
            current_block_rows.append(row.to_dict())
            last_date = row_date
            # Update current_permco to the latest PERMCO we see in this block.
            current_permco = row_permco

    process_chunk(first_chunk)
    for chunk in reader:
        process_chunk(chunk)

    # Flush whatever remains
    flush_current_block()


def split_sp500_by_ticker(
    data_path,
    tickers_path,
    companies_path,
    output_dir,
    gap_days=365,
    chunksize=100000,
):
    create_dir(output_dir)

    valid_tickers, symbol_to_name, ticker_mapping = load_sp500_metadata(
        tickers_path, companies_path
    )

    reader = pd.read_csv(data_path, low_memory=False, chunksize=chunksize)

    first_chunk = next(reader)
    columns = list(first_chunk.columns)

    date_col = None
    for candidate in ["date", "Date", "DATE"]:
        if candidate in columns:
            date_col = candidate
            break
    if date_col is None:
        raise ValueError("Could not find date column in dataset")

    ticker_col = None
    for candidate in ["TICKER", "Ticker", "SYMBOL", "Symbol"]:
        if candidate in columns:
            ticker_col = candidate
            break
    if ticker_col is None:
        raise ValueError("Could not find ticker column in dataset")

    name_col = None
    for candidate in ["COMNAM", "Company Name", "COMPANY_NAME", "Name"]:
        if candidate in columns:
            name_col = candidate
            break

    first_chunk[date_col] = pd.to_datetime(first_chunk[date_col])

    current_block_rows = []
    last_ticker = None
    last_name = None
    last_date = None

    def flush_current_block():
        nonlocal current_block_rows, last_ticker, last_name, last_date
        if not current_block_rows:
            return

        canonical_ticker = map_data_ticker_to_canonical(
            last_ticker, ticker_mapping, valid_tickers
        )
        if canonical_ticker is None:
            print(
                f"Skipping block with unmapped ticker '{last_ticker}' and last name '{last_name}'"
            )
            current_block_rows = []
            last_ticker = None
            last_name = None
            last_date = None
            return

        expected_name = symbol_to_name.get(canonical_ticker)
        if expected_name is not None and normalize_company_name(
            expected_name
        ) != normalize_company_name(last_name):
            print(
                f"Warning: name mismatch for ticker {canonical_ticker}: block name '{last_name}' vs reference '{expected_name}'"
            )

        df_block = pd.DataFrame.from_records(current_block_rows)
        out_path = os.path.join(output_dir, f"{canonical_ticker}.csv")
        header = not os.path.exists(out_path)
        df_block.to_csv(out_path, index=False, mode="a", header=header)

        current_block_rows = []
        last_ticker = None
        last_name = None
        last_date = None

    def process_chunk(chunk):
        nonlocal current_block_rows, last_ticker, last_name, last_date
        chunk[date_col] = pd.to_datetime(chunk[date_col])
        for _, row in chunk.iterrows():
            row_date = row[date_col]
            row_ticker = (
                str(row[ticker_col]).strip() if not pd.isna(row[ticker_col]) else ""
            )
            row_name = (
                str(row[name_col]).strip()
                if name_col is not None and not pd.isna(row[name_col])
                else ""
            )

            if last_date is not None:
                date_diff = (row_date - last_date).days
                ticker_changed = row_ticker != last_ticker
                name_changed = normalize_company_name(row_name) != normalize_company_name(
                    last_name
                )

                # 1) If time goes backwards, always start a new company block.
                if row_date < last_date:
                    print(
                        f"Time moved backwards from {last_date.date()} to {row_date.date()} "
                        f"for ticker '{row_ticker}' (prev '{last_ticker}'), starting new block."
                    )
                    flush_current_block()

                # 2) If there is a huge forward gap and both ticker and name change,
                #    treat it as a new company.
                elif date_diff >= gap_days and ticker_changed and name_changed:
                    flush_current_block()

                # 3) If there is a huge forward gap but ticker/name stay the same,
                #    keep the same company but log for later investigation.
                elif date_diff >= gap_days and not (ticker_changed or name_changed):
                    print(
                        f"Long gap ({date_diff} days) for ticker '{row_ticker}' "
                        f"from {last_date.date()} to {row_date.date()} with same name, keeping block."
                    )

            current_block_rows.append(row.to_dict())
            last_ticker = row_ticker
            last_name = row_name
            last_date = row_date

    process_chunk(first_chunk)
    for chunk in reader:
        process_chunk(chunk)

    flush_current_block()


def run_old_pipeline():
    test_year = 2022
    data_path = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp500_new.csv"
    company_ticker_file = (
        "/Users/zimenglyu/Documents/code/git/CRSP_Processor/selected_tickers_50.txt"
    )
    permco_csv_path = "./sp_500_permco_info.csv"

    parameters = [
        "date",
        "TICKER",
        "PERMNO",
        "COMNAM",
        "SHRCLS",
        "NAMEENDT",
        "RET",
        "VOL_CHANGE",
        "BA_SPREAD",
        "ILLIQUIDITY",
        "sprtrn",
        "TURNOVER",
        "PRC",
        "SHROUT",
        "MARKET_CAP",
        "TRAN_COST",
        "ASK",
        "BID",
    ]
    input_parameters = [
        "RET",
        "VOL_CHANGE",
        "BA_SPREAD",
        "ILLIQUIDITY",
        "sprtrn",
        "TURNOVER",
    ]
    start_train = "1990-01-01"
    end_train = f"{test_year-2}-12-31"
    start_validation = f"{test_year-1}-01-01"
    end_validation = f"{test_year-1}-12-31"
    start_test = f"{test_year}-01-01"
    end_test = f"{test_year}-12-31"

    result_dir = f"./{test_year}_sp_500_select_50"
    create_dir(result_dir)
    create_dir(os.path.join(result_dir, "train"))
    create_dir(os.path.join(result_dir, "validation"))
    create_dir(os.path.join(result_dir, "test"))
    create_dir(os.path.join(result_dir, "raw_data"))

    company_tickers = read_words_from_file(company_ticker_file)
    print(company_tickers)
    print(f"length of company_tickers: {len(company_tickers)}")
    permco_info = find_permco(company_tickers, data_path)
    dict_to_csv(permco_info, permco_csv_path)
    print(f"size of permco_info: {len(permco_info)}")
    print(f"Prepare the data for {test_year}")
    data_loader = DataLoader(data_path, company_ticker_file, permco_info)
    data_loader.create_for_portfolio()
    data_loader.add_predictors()
    data_loader.select_columns(parameters)
    data_loader.remove_nan(input_parameters)
    data_loader.save_raw_data(result_dir)
    data_loader.set_train_validation_test_dates(
        start_train,
        end_train,
        start_validation,
        end_validation,
        start_test,
        end_test,
    )
    data_loader.split_train_validation_test()
    data_loader.save_stock_data(result_dir)

    data_loader.save_combined_returns(result_dir)
    crsp_parameters = ["RET", "VOL_CHANGE", "ASK", "BID", "sprtrn"]
    predictors = ["BA_SPREAD", "ILLIQUIDITY", "TURNOVER", "VOL_CHANGE", "RET", "sprtrn"]
    data_loader.save_combined_parameters(result_dir, crsp_parameters, "parameters")
    data_loader.save_combined_parameters(result_dir, predictors, "predictors")
    print("finished saving data")


if __name__ == "__main__":
    data_path = "datasets/sp-500-24.csv"
    output_dir = os.path.join("datasets", "sp500_by_permco")

    split_sp500_by_permco(
        data_path=data_path,
        output_dir=output_dir,
        chunksize=100000,
    )


