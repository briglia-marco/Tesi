"""
Module for processing wallet transactions, including splitting transactions
into time-based chunks, counting transactions per chunk, and generating Excel
reports. Functions also handle directory creation, file reading, and saving
chunked data to disk.
"""

import os
import json
import pandas as pd


# _________________________________________________________________________________________________


def find_global_start_time(files, input_dir):
    """
    Find the global start time for a wallet's transactions based on the earliest
    timestamp across all files.

    Args:
        files (list): List of transaction file names.
        input_dir (str): Directory containing the input JSON files.

    Returns:
        pd.Timestamp: The global start time for the wallet's transactions.
    """
    start_time = None
    for file_name in files:
        with open(
            os.path.join(input_dir, file_name), "r", encoding="utf-8"
        ) as f:
            data = json.load(f)
            transactions = (
                data
                if isinstance(data, list)
                else data.get("transactions", [])
            )
            timestamps = [tx["time"] for tx in transactions if "time" in tx]
            if timestamps:
                min_time = min(timestamps)
                if start_time is None or min_time < start_time:
                    start_time = min_time
    return pd.to_datetime(start_time, unit="s") if start_time else None


# _________________________________________________________________________________________________


def create_output_dirs(wallet_id, output_base_dir, intervals_months):
    """
    Create output directories for chunked transactions based on specified intervals.

    Args:
        wallet_id (str): The wallet ID being processed.
        output_base_dir (str): Base directory for output files.
        intervals_months (list): List of intervals in months for chunking.
    """
    for interval in intervals_months:
        os.makedirs(
            os.path.join(output_base_dir, f"{wallet_id}/{interval}_months"),
            exist_ok=True,
        )


# _________________________________________________________________________________________________


def process_transaction_file(
    file_name,
    input_dir,
    start_time,
    intervals_months,
    chunk_data,
    wallet_id,
    output_base_dir,
):
    """Process a transaction file and chunk its data into specified intervals.

    Args:
        file_name (str): The name of the transaction file to process.
        input_dir (str): Directory containing the input JSON files.
        start_time (pd.Timestamp): The global start time for the wallet's transactions.
        intervals_months (list): List of intervals in months for chunking.
        chunk_data (dict): Dictionary to store chunked transactions.
        wallet_id (str): The wallet ID being processed.
        output_base_dir (str): Base directory for output files.
    """
    with open(os.path.join(input_dir, file_name), "r", encoding="utf-8") as f:
        data = json.load(f)
        transactions = (
            data if isinstance(data, list) else data.get("transactions", [])
        )

    for tx in transactions:
        if "time" not in tx:
            continue

        tx_time = pd.to_datetime(tx["time"], unit="s")
        months_since_start = (tx_time.year - start_time.year) * 12 + (
            tx_time.month - start_time.month
        )

        for interval in intervals_months:
            period_index = months_since_start // interval
            period_start = start_time + pd.DateOffset(
                months=period_index * interval
            )
            period_end = (
                period_start
                + pd.DateOffset(months=interval)
                - pd.Timedelta(seconds=1)
            )
            period_label = (
                f"{period_start.strftime('%Y-%m-%d')}_to_"
                f"{period_end.strftime('%Y-%m-%d')}"
            )
            out_dir = os.path.join(
                output_base_dir, f"{wallet_id}/{interval}_months"
            )
            out_file = os.path.join(out_dir, f"{period_label}.json")

            if out_file not in chunk_data[interval]:
                chunk_data[interval][out_file] = []

            chunk_data[interval][out_file].append(tx)


# _________________________________________________________________________________________________


def save_chunks_to_disk(chunk_data):
    """
    Save the chunked transactions to disk.

    Args:
        chunk_data (dict): Dictionary containing chunked transactions.
    """

    for _, files_dict in chunk_data.items():
        for out_file, tx_list in files_dict.items():
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(tx_list, f, indent=4)
            print(f"Saved {len(tx_list)} txs to {out_file}")


# _________________________________________________________________________________________________


def split_transactions_into_chunks(
    wallet_id, input_dir, output_base_dir, intervals_months
):
    """
    Split transactions of a wallet into chunks based on specified intervals.

    Args:
        wallet_id (str): The wallet ID to process.
        input_dir (str): Directory containing the input JSON files.
        output_base_dir (str): Base directory for output files.
        intervals_months (list): List of intervals in months for chunking.
    """
    print(f"Scanning files for {wallet_id}...")

    files = sorted(
        [
            f
            for f in os.listdir(input_dir)
            if f.startswith(f"{wallet_id}_transactions_")
            and f.endswith(".json")
        ],
        key=lambda x: int(
            x.replace(f"{wallet_id}_transactions_", "").replace(".json", "")
        ),
    )

    start_time = find_global_start_time(files, input_dir)
    if not start_time:
        print("No transactions found for this wallet.")
        return

    create_output_dirs(wallet_id, output_base_dir, intervals_months)
    chunk_data = {interval: {} for interval in intervals_months}

    for file_name in files:
        print(f"Processing {file_name}...")
        process_transaction_file(
            file_name,
            input_dir,
            start_time,
            intervals_months,
            chunk_data,
            wallet_id,
            output_base_dir,
        )

    save_chunks_to_disk(chunk_data)
    print("Chunking complete.")


# _________________________________________________________________________________________________


def count_transactions_in_chunks(directory_input):
    """
    Count the number of transactions in each chunk for a given wallet ID.

    Args:
        directory_input (str): Directory containing the chunked JSON files.

    Returns:
        pd.DataFrame: DataFrame containing the count of transactions in each chunk.
    """
    files = [f for f in os.listdir(directory_input) if f.endswith(".json")]

    chunk_counts = []
    for file_name in files:
        with open(
            os.path.join(directory_input, file_name), "r", encoding="utf-8"
        ) as f:
            data = json.load(f)
            transactions = (
                data
                if isinstance(data, list)
                else data.get("transactions", [])
            )
            chunk_counts.append(
                {"chunk": file_name, "count": len(transactions)}
            )
    df_chunk_counts = pd.DataFrame(chunk_counts)
    df_chunk_counts["chunk"] = df_chunk_counts["chunk"].str.replace(
        ".json", ""
    )

    return df_chunk_counts


# _________________________________________________________________________________________________


def generate_chunk_transaction_reports(base_chunk_dir, intervals, output_dir):
    """
    Conta il numero di transazioni in ogni file di ogni periodo (chunk)
    e salva i risultati in file Excel per ciascun intervallo di mesi.

    Args:
        wallet_id (str): Il wallet di riferimento.
        base_chunk_dir (str): Directory base dove si trovano i chunk.
        intervals (list): Lista di intervalli temporali (in mesi).
        output_dir (str): Directory dove salvare i file Excel risultanti.
    """
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(base_chunk_dir, exist_ok=True)
    df_chunks = {}

    for interval in intervals:
        print(f"[INFO] Counting transactions for {interval} months chunks...")
        df_chunk = count_transactions_in_chunks(
            directory_input=os.path.join(base_chunk_dir, f"{interval}_months"),
        )
        df_chunk = df_chunk.sort_values(by="count", ascending=False)
        df_chunks[interval] = df_chunk

    for interval, df_chunk in df_chunks.items():
        output_path = os.path.join(output_dir, f"{interval}_months.xlsx")
        df_chunk.to_excel(output_path, index=False)
        print(f"[INFO] Saved Excel report: {output_path}")

    print("[INFO] All reports generated.")


# _________________________________________________________________________________________________
