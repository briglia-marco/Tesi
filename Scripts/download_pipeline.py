"""
This module manages the entire wallet data preparation pipeline.
Specifically:
- Downloads the first 100 wallet addresses if not already present
- Retrieves wallet information and saves it in JSON format
- Applies a weighting system to process known wallets and build a DataFrame
- Selects a subset of wallets and downloads their addresses and raw transactions
- (Optional) merges the downloaded files into the structured output directory

The flow is designed for analyzing Bitcoin gambling services (SatoshiDice, BitZillions)
"""

import os
import config
from Scripts.utils.ranking_utils import process_wallet_dataframe
from Scripts.utils.merge_utils import merge_files
from Scripts.utils.wallet_explorer_api_utils import (
    download_first_100_addresses,
    get_all_wallets_info,
)
from Scripts.utils.data_processing_utils import (
    download_wallet_addresses,
    download_wallet_transactions,
    all_files_exist,
)


def run_download_pipeline(do_merge: bool = False) -> None:
    """
    Runs the pipeline for downloading, processing, and analyzing wallets.

    Steps:
    1. Download first 100 addresses (if not already present).
    2. Collect wallet info (transactions, addresses, etc.).
    3. Process wallet dataframe with weights and known services.
    4. Download full addresses and transactions (if not already present).
    5. Optionally merge raw files into processed JSON files.

    Args:
        do_merge (bool): If True, performs merging of raw files.
    """

    # Step 1: First 100 addresses
    os.makedirs(config.DIRECTORY_PROCESSED_100_ADDRESSES, exist_ok=True)
    if len(os.listdir(config.DIRECTORY_PROCESSED_100_ADDRESSES)) == 0:
        download_first_100_addresses(
            directory_addresses=config.DIRECTORY_PROCESSED_100_ADDRESSES,
        )
    else:
        print("First 100 addresses are already downloaded.")

    # Step 2: Wallet info
    os.makedirs(config.DIRECTORY_PRO_INFO, exist_ok=True)
    if len(os.listdir(config.DIRECTORY_PRO_INFO)) == 0:
        get_all_wallets_info(
            directory=config.DIRECTORY_PROCESSED_100_ADDRESSES,
            output_file=os.path.join(config.DIRECTORY_PRO_INFO, "wallets_info.json"),
        )
    print("Wallet info processed.")

    # Step 3: Process dataframe
    df_wallets = process_wallet_dataframe(
        wallets_info_path=f"{config.DIRECTORY_PRO_INFO}/wallets_info.json",
        directory_addresses=config.DIRECTORY_PROCESSED_100_ADDRESSES,
        known_services=config.KNOWN_SERVICES,
        w1=config.W1,
        w2=config.W2,
        w3=config.W3,
        w4=config.W4,
        w5=config.W5,
    )

    wallet_ids = df_wallets["wallet_id"].iloc[:5].tolist()  # you need to remove this
    # wallet_ids = ["DiceNow.com", ...] # if you want to analyze specified service(s)

    # Step 4: Download addresses & transactions
    os.makedirs(config.DIRECTORY_RAW_ADDRESSES, exist_ok=True)
    if not all_files_exist(config.DIRECTORY_RAW_ADDRESSES, wallet_ids):
        download_wallet_addresses(wallet_ids, config.DIRECTORY_RAW_ADDRESSES)

    os.makedirs(config.DIRECTORY_RAW_TRANSACTIONS, exist_ok=True)
    if not all_files_exist(config.DIRECTORY_RAW_TRANSACTIONS, wallet_ids):
        download_wallet_transactions(wallet_ids, config.DIRECTORY_RAW_TRANSACTIONS)

    # Step 5: Optional merge
    if do_merge:
        merge_files(
            wallet_ids,
            config.DIRECTORY_RAW_ADDRESSES,
            config.DIRECTORY_PROCESSED_ADDR,
            config.DIRECTORY_RAW_TRANSACTIONS,
            config.DIRECTORY_PROCESSED_TXS,
        )
