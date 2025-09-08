"""
This module is responsable of merging raw transaction/address files
"""

import os
from Scripts.data_processing import merge_wallet_json_files


def merge_addresses(wallet_ids, directory_raw, directory_processed):
    """
    Merge raw address JSON files for each wallet into a single consolidated file.

    Args:
    wallet_ids (list): List of wallet IDs to process.
    directory_raw (str): Directory containing raw address JSON files.
    directory_processed (str): Directory where merged files will be saved.
    """

    os.makedirs(directory_processed, exist_ok=True)
    existing = (
        set(os.listdir(directory_processed))
        if os.path.exists(directory_processed)
        else set()
    )

    for wallet_id in wallet_ids:
        merged_file = f"{wallet_id}_addresses.json"
        if merged_file not in existing:
            merge_wallet_json_files(
                wallet_id=wallet_id,
                directory_input=directory_raw,
                directory_output=directory_processed,
                output_suffix="addresses",
                data_field="addresses",
                count_field="addresses_count",
            )


def merge_transactions(wallet_ids, directory_raw, directory_processed):
    """
    Merge raw transaction JSON files for each wallet into a single consolidated file.

    Args:
        wallet_ids (list): List of wallet IDs to process.
        directory_raw (str): Directory containing raw transaction JSON files.
        directory_processed (str): Directory where merged files will be saved.
    """
    os.makedirs(directory_processed, exist_ok=True)
    existing = (
        set(os.listdir(directory_processed))
        if os.path.exists(directory_processed)
        else set()
    )

    for wallet_id in wallet_ids:
        merged_file = f"{wallet_id}_transactions.json"
        if merged_file not in existing:
            merge_wallet_json_files(
                wallet_id=wallet_id,
                directory_input=directory_raw,
                directory_output=directory_processed,
                output_suffix="transactions",
                data_field="transactions",
                count_field="transactions_count",
            )


def merge_files(
    wallet_ids,
    DIRECTORY_RAW_ADDRESSES,
    DIRECTORY_PROCESSED_ADDR,
    DIRECTORY_RAW_TRANSACTIONS,
    DIRECTORY_PROCESSED_TXS,
):
    """
    Run both address and transaction merging for the given wallet IDs.

    Args:
        wallet_ids (list): List of wallet IDs to process.
        DIRECTORY_RAW_ADDRESSES (str): Dir containing raw address JSON files.
        DIRECTORY_PROCESSED_ADDR (str): Dir where merged address files will be saved.
        DIRECTORY_RAW_TRANSACTIONS (str): Dir containing raw transaction JSON files.
        DIRECTORY_PROCESSED_TXS (str): Dir where merged transaction files will be saved.
    """

    merge_addresses(wallet_ids, DIRECTORY_RAW_ADDRESSES, DIRECTORY_PROCESSED_ADDR)
    merge_transactions(wallet_ids, DIRECTORY_RAW_TRANSACTIONS, DIRECTORY_PROCESSED_TXS)
    print("All JSON files merged.")
