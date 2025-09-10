"""
Module for downloading, processing, and chunking wallet addresses
and transactions from WalletExplorer. Functions include merging JSON
files, splitting them into smaller chunks, and checking file existence.
"""

import os
import json
from Scripts.utils.fetch_utils import fetch_all_addresses, fetch_wallet_transactions

# _________________________________________________________________________________________________


def download_wallet_addresses(
    wallet_ids: list[str], directory_raw_addresses: str
) -> None:
    """
    Download all addresses associated with a list of wallet IDs by querying the
    WalletExplorer API (or another service). Downloads are skipped only if the
    first address chunk already exists.

    Args:
        wallet_ids (list[str]): List of wallet IDs to fetch addresses for.
        directory_raw_addresses (str): Directory where address JSON files will be saved.
    """
    os.makedirs(directory_raw_addresses, exist_ok=True)

    for wallet_id in wallet_ids:
        address_file = f"{directory_raw_addresses}/{wallet_id}_addresses_1.json"
        if not os.path.exists(address_file):
            print(f"Downloading all addresses for {wallet_id}...")
            try:
                fetch_all_addresses(wallet_id, directory_raw_addresses)
            except OSError as e:
                print(f"Failed to download addresses for {wallet_id}: {e}")
        else:
            print(f"Addresses for {wallet_id} already exist, skipping download.")


# _________________________________________________________________________________________________


def download_wallet_transactions(
    wallet_ids: list[str], directory_raw_transactions: str
) -> None:
    """
    Download all transactions associated with a list of wallet IDs by querying the
    WalletExplorer API (or another service). Downloads are skipped if the first
    transaction chunk already exists in the target directory.

    Args:
        wallet_ids (list[str]): List of wallet IDs to fetch transactions for.
        directory_raw_transactions (str): Dir where transaction JSON files will be saved
    """
    os.makedirs(directory_raw_transactions, exist_ok=True)

    for wallet_id in wallet_ids:
        tx_file = f"{directory_raw_transactions}/{wallet_id}_transactions_1.json"
        if not os.path.exists(tx_file):
            print(f"Downloading all transactions for {wallet_id}...")
            try:
                fetch_wallet_transactions(wallet_id, directory_raw_transactions)
            except OSError as e:
                print(f"Failed to download transactions for {wallet_id}: {e}")
        else:
            print(f"Transactions for {wallet_id} already exist, skipping download.")


# _________________________________________________________________________________________________


def merge_wallet_json_files(
    wallet_id: str,
    directory_input: str,
    directory_output: str,
    output_suffix: str,
    data_field: str,
    count_field: str,
) -> None:
    """
    Merge all JSON files for a given wallet containing either addresses or transactions
    into a single JSON file. The data_field and count_field are customizable.
    Supports both dict-based and pure list JSON files.

    Args:
        wallet_id (str): The wallet ID to merge files for.
        directory_input (str): Directory containing the input JSON files.
        directory_output (str): Directory to save the merged output file.
        output_suffix (str): Suffix for the output file name.
        data_field (str): The field in the JSON to merge.
        count_field (str): The field to count items in the JSON.
    """
    merged_data = {
        "found": True,
        "label": wallet_id,
        "wallet_id": wallet_id,
        count_field: 0,
        data_field: [],
    }

    files = [
        f
        for f in os.listdir(directory_input)
        if f.startswith(wallet_id) and f.endswith(".json")
    ]
    files.sort()

    for file_name in files:
        file_path = os.path.join(directory_input, file_name)
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict) and data_field in data:
            merged_data[data_field].extend(data[data_field])
            merged_data[count_field] += data.get(count_field, len(data[data_field]))
        elif isinstance(data, list):
            merged_data[data_field].extend(data)
            merged_data[count_field] += len(data)
        else:
            print(f"Skipped {file_path}, unexpected format")

    os.makedirs(directory_output, exist_ok=True)
    output_file_name = f"{wallet_id}_{output_suffix}.json"
    output_path = os.path.join(directory_output, output_file_name)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=4)

    print(f"Merged file saved to {output_path}")


# _________________________________________________________________________________________________


def split_json_file(
    wallet_id: str,
    directory_input: str,
    directory_output: str,
    data_field: str,
    chunk_size: int = 100000,
    suffix: str = "addresses",
) -> None:
    """
    Split a merged JSON file containing either addresses or transactions
    into multiple JSON files each containing up to chunk_size objects.

    Args:
        wallet_id (str): Wallet ID of the file to split.
        directory_input (str): Directory containing the merged JSON file.
        directory_output (str): Directory where split files will be saved.
        data_field (str): Key name of the list in the JSON.
        chunk_size (int, optional): Number of objects per file. Default is 100,000.
        suffix (str, optional): Suffix for the output file.
    """
    input_path = os.path.join(directory_input, f"{wallet_id}_{suffix}.json")
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get(data_field, [])
    total_items = len(items)
    os.makedirs(directory_output, exist_ok=True)

    file_index = 1
    for i in range(0, total_items, chunk_size):
        chunk = items[i : i + chunk_size]
        output_path = os.path.join(
            directory_output, f"{wallet_id}_{suffix}_{file_index}.json"
        )

        with open(output_path, "w", encoding="utf-8") as f_out:
            json.dump(chunk, f_out, indent=4)

        print(f"Saved {len(chunk)} items to {output_path}")
        file_index += 1

    print(f"Splitting complete for {wallet_id} ({suffix})")


# _________________________________________________________________________________________________


def split_all_wallet_files(
    wallet_ids: list[str],
    dir_processed_addresses: str,
    dir_processed_transactions: str,
    dir_raw_addresses: str,
    dir_raw_transactions: str,
) -> None:
    """
    Split all merged JSON files for a list of wallet IDs into chunks of 100,000 objects,
    for both addresses and transactions.

    Args:
        wallet_ids (list): List of wallet IDs to process.
        dir_processed_addresses (str): Directory with merged addresses JSON files.
        dir_processed_transactions (str): Directory with merged transactions JSON files.
        dir_raw_addresses (str): Output directory for split addresses JSON files.
        dir_raw_transactions (str): Output directory for split transactions JSON files.
    """
    for wallet_id in wallet_ids:
        print(f"\nSplitting addresses for {wallet_id}...")
        split_json_file(
            wallet_id=wallet_id,
            directory_input=dir_processed_addresses,
            directory_output=dir_raw_addresses,
            data_field="addresses",
            chunk_size=100000,
            suffix="addresses",
        )

        print(f"Splitting transactions for {wallet_id}...")
        split_json_file(
            wallet_id=wallet_id,
            directory_input=dir_processed_transactions,
            directory_output=dir_raw_transactions,
            data_field="transactions",
            chunk_size=100000,
            suffix="transactions",
        )

    print("\nAll files split successfully.")


# _________________________________________________________________________________________________


def all_files_exist(folder: str, wallet_ids: list[str]) -> bool:
    """
    Check if all expected JSON files for the given wallet IDs exist in the directory.

    Args:
        folder (str): The directory to check for the files.
        wallet_ids (list): List of wallet IDs to check.
    Returns:
        bool: True if all files exist, False otherwise.
    """
    for wid in wallet_ids:
        expected_file = os.path.join(folder, f"{wid}.json")
        if not os.path.exists(expected_file):
            return False
    return True
