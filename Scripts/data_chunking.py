import os
import json
import pandas as pd
from tqdm import tqdm
from datetime import datetime

#_______________________________________________________________________________________________________________________

def find_global_start_time(files, input_dir):
    """
    Find the global start time for a wallet's transactions based on the earliest timestamp across all files.

    Args:
        files (list): List of transaction file names.
        input_dir (str): Directory containing the input JSON files.

    Returns:
        pd.Timestamp: The global start time for the wallet's transactions.
    """
    start_time = None
    for file_name in files:
        with open(os.path.join(input_dir, file_name), "r") as f:
            data = json.load(f)
            transactions = data if isinstance(data, list) else data.get("transactions", [])
            timestamps = [tx["time"] for tx in transactions if "time" in tx]
            if timestamps:
                min_time = min(timestamps)
                if start_time is None or min_time < start_time:
                    start_time = min_time
    return pd.to_datetime(start_time, unit="s") if start_time else None

#_______________________________________________________________________________________________________________________

def create_output_dirs(wallet_id, output_base_dir, intervals_months):
    """
    Create output directories for chunked transactions based on specified intervals.

    Args:
        wallet_id (str): The wallet ID being processed.
        output_base_dir (str): Base directory for output files.
        intervals_months (list): List of intervals in months for chunking.
    """
    for interval in intervals_months:
        os.makedirs(os.path.join(output_base_dir, f"{wallet_id}/{interval}_months"), exist_ok=True)

#_______________________________________________________________________________________________________________________

def process_transaction_file(file_name, input_dir, start_time, intervals_months, chunk_data, wallet_id, output_base_dir):
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
    with open(os.path.join(input_dir, file_name), "r") as f:
        data = json.load(f)
        transactions = data if isinstance(data, list) else data.get("transactions", [])

    for tx in transactions:
        if "time" not in tx:
            continue

        tx_time = pd.to_datetime(tx["time"], unit="s")
        months_since_start = (tx_time.year - start_time.year) * 12 + (tx_time.month - start_time.month)

        for interval in intervals_months:
            period_index = months_since_start // interval
            out_dir = os.path.join(output_base_dir, f"{wallet_id}/{interval}_months")
            out_file = os.path.join(out_dir, f"{period_index}.json")

            if out_file not in chunk_data[interval]:
                chunk_data[interval][out_file] = []

            chunk_data[interval][out_file].append(tx)

#_______________________________________________________________________________________________________________________

def save_chunks_to_disk(chunk_data):
    """
    Save the chunked transactions to disk.
    
    Args:
        chunk_data (dict): Dictionary containing chunked transactions.
    """

    for interval, files_dict in chunk_data.items():
        for out_file, tx_list in files_dict.items():
            with open(out_file, "w") as f:
                json.dump(tx_list, f, indent=4)
            print(f"Saved {len(tx_list)} txs to {out_file}")

#_______________________________________________________________________________________________________________________

def split_transactions_into_chunks(wallet_id, input_dir, output_base_dir, intervals_months):
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
        [f for f in os.listdir(input_dir)
        if f.startswith(f"{wallet_id}_transactions_") and f.endswith(".json")],
        key=lambda x: int(x.replace(f"{wallet_id}_transactions_", "").replace(".json", ""))
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
            file_name, input_dir, start_time,
            intervals_months, chunk_data,
            wallet_id, output_base_dir
        )

    save_chunks_to_disk(chunk_data)
    print("Chunking complete.")

#_______________________________________________________________________________________________________________________
