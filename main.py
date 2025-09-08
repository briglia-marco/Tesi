"""
This script orchestrates the entire process of downloading, processing,
analyzing, and detecting gambling patterns in cryptocurrency wallet data.
"""

import os
import pandas as pd
from Scripts.ranking import process_wallet_dataframe
from Scripts.graph import build_graphs_for_wallet
from Scripts.data_processing import (
    download_wallet_transactions,
    download_wallet_addresses,
    all_files_exist,
)
from Scripts.wallet_explorer_api import (
    download_first_100_addresses,
    get_all_wallets_info,
)
from Scripts.data_chunking import (
    split_transactions_into_chunks,
    generate_chunk_transaction_reports,
)
from Scripts.metrics import analyze_chunk_metrics, calculate_chunk_global_metrics
from Scripts.rolling_analisys import run_rolling_window_analysis
from Scripts.gambling_analysis import run_gambling_detection
from Scripts.merge import merge_files


def main():
    """
    Entry point of the program.

    Sets the target service and threshold parameters, then starts
    the pipeline for downloading, processing, and analyzing
    transactions of the selected wallet(s).
    """

    # SET THE TARGET SERVICE AND THE THRESHOLD PARAMETERS

    # SatoshiDice.com-original, (100000, 1000)
    # BitZillions.com, (10000, 200)
    SERVICE = "SatoshiDice.com-original"
    TRANSACTIONS_FOR_CHUNK_THRESHOLD = 100000
    MIN_TRANSACTIONS_TO_ANALYZE_WALLET = 1000

    DIRECTORY_RAW_ADDRESSES = "Data/raw/addresses"
    DIRECTORY_RAW_TRANSACTIONS = "Data/raw/transactions"
    DIRECTORY_PROCESSED_100_ADDRESSES = "Data/processed/first_100_addresses"
    DIRECTORY_PROCESSED_ADDR = "Data/processed/addresses"
    DIRECTORY_PROCESSED_TXS = "Data/processed/transactions"
    DIRECTORY_PRO_INFO = "Data/processed/info"
    SERVICE_DIR = f"Data/chunks/{SERVICE}"
    DIRECTORY_CHUNKS = f"{SERVICE_DIR}/3_months"
    DIRECTORY_XLSX = f"{SERVICE_DIR}/xlsx"

    # _____________________________________________________________________________________________

    # DOWNLOAD DATA

    os.makedirs(DIRECTORY_PROCESSED_100_ADDRESSES, exist_ok=True)
    if len(os.listdir(DIRECTORY_PROCESSED_100_ADDRESSES)) == 0:
        download_first_100_addresses(
            directory_addresses=DIRECTORY_PROCESSED_100_ADDRESSES,
        )
    else:
        print("First 100 addresses are already downloaded.")

    os.makedirs(DIRECTORY_PRO_INFO, exist_ok=True)
    if len(os.listdir(DIRECTORY_PRO_INFO)) == 0:
        get_all_wallets_info(
            directory=DIRECTORY_PROCESSED_100_ADDRESSES,
            output_file=os.path.join(DIRECTORY_PRO_INFO, "wallets_info.json"),
        )
    print("Wallet info processed.")

    # _____________________________________________________________________________________________

    # DOWNLOAD DATA

    w1, w2, w3, w4, w5 = (
        0.35,  # total transactions
        0.03,  # total addresses
        0.25,  # transactions per address
        0.35,  # first 100 transactions/total transactions
        0.02,  # notoriety
    )

    known_services = [
        "SatoshiDice.com-original",
        "SatoshiDice.com",
        "BitZillions.com",
        "999Dice.com",
        "Betcoin.ag",
        "CloudBet.com",
    ]

    df_wallets = process_wallet_dataframe(
        wallets_info_path=f"{DIRECTORY_PRO_INFO}/wallets_info.json",
        directory_addresses=DIRECTORY_PROCESSED_100_ADDRESSES,
        known_services=known_services,
        w1=w1,
        w2=w2,
        w3=w3,
        w4=w4,
        w5=w5,
    )

    wallet_ids = df_wallets["wallet_id"].iloc[:5].tolist()

    os.makedirs(DIRECTORY_RAW_ADDRESSES, exist_ok=True)
    if not all_files_exist(DIRECTORY_RAW_ADDRESSES, wallet_ids):
        download_wallet_addresses(wallet_ids, DIRECTORY_RAW_ADDRESSES)

    os.makedirs(DIRECTORY_RAW_TRANSACTIONS, exist_ok=True)
    if not all_files_exist(DIRECTORY_RAW_TRANSACTIONS, wallet_ids):
        download_wallet_transactions(wallet_ids, DIRECTORY_RAW_TRANSACTIONS)

    # _____________________________________________________________________________________________

    # MERGE JSON FILES

    DO_MERGE = False

    if DO_MERGE is True:
        merge_files(
            wallet_ids,
            DIRECTORY_RAW_ADDRESSES,
            DIRECTORY_PROCESSED_ADDR,
            DIRECTORY_RAW_TRANSACTIONS,
            DIRECTORY_PROCESSED_TXS,
        )

    # _____________________________________________________________________________________________

    # CHUNKING DATA

    intervals = [3, 6, 12, 24]  # months

    if os.path.exists(SERVICE_DIR):
        existing_chunk_files = set(os.listdir(SERVICE_DIR))
    else:
        existing_chunk_files = set()

    for interval in intervals:
        if not f"{interval}_months" in existing_chunk_files:
            split_transactions_into_chunks(
                wallet_id=SERVICE,
                input_dir=DIRECTORY_RAW_TRANSACTIONS,
                output_base_dir="Data/chunks",
                intervals_months=[interval],
            )

    os.makedirs(DIRECTORY_XLSX, exist_ok=True)
    all_reports_exist = all(
        os.path.exists(os.path.join(DIRECTORY_XLSX, f"{interval}_months.xlsx"))
        for interval in intervals
    )

    if not all_reports_exist:
        generate_chunk_transaction_reports(
            base_chunk_dir=SERVICE_DIR,
            intervals=intervals,
            output_dir=DIRECTORY_XLSX,
        )
    else:
        print("[INFO] All chunk reports already exist, skipping generation.")

    # _____________________________________________________________________________________________

    # GRAPH AND METRICS FOR CHUNKS

    CHUNK_METRICS_DIRECTORY = f"{DIRECTORY_XLSX}/chunk_metrics"

    chunks = pd.read_excel(f"{DIRECTORY_XLSX}/3_months.xlsx")
    chunks = chunks[chunks["count"] > TRANSACTIONS_FOR_CHUNK_THRESHOLD]

    for chunk in chunks["chunk"].tolist():
        print(f"Processing chunk: {chunk}")
        build_graphs_for_wallet(chunk, DIRECTORY_CHUNKS, SERVICE)
        analyze_chunk_metrics(
            chunk,
            DIRECTORY_CHUNKS,
            output_dir=CHUNK_METRICS_DIRECTORY,
        )

    # _____________________________________________________________________________________________

    # CHUNK GLOBAL METRICS

    chunk_metrics_files = os.listdir(CHUNK_METRICS_DIRECTORY)
    df_chunk_global_metrics = pd.DataFrame()

    for chunk_file in chunk_metrics_files:
        if not chunk_file.endswith(".xlsx"):
            continue
        chunk_file_name = chunk_file.split(".")[0]
        df_chunk_global_metrics = calculate_chunk_global_metrics(
            chunk_file_path=os.path.join(CHUNK_METRICS_DIRECTORY, chunk_file),
            global_metrics_df=df_chunk_global_metrics,
            chunk_file_name=chunk_file_name,
        )

    df_chunk_global_metrics.to_excel(
        f"{DIRECTORY_XLSX}/chunk_global_metrics.xlsx", index=False
    )

    # _____________________________________________________________________________________________

    # ROLLING WINDOW ANALYSIS

    LOGS_DIR = f"{SERVICE_DIR}/logs"
    WINDOW_SIZE = 10
    VAR_THRESHOLD = 10

    run_rolling_window_analysis(
        SERVICE,
        CHUNK_METRICS_DIRECTORY,
        DIRECTORY_CHUNKS,
        LOGS_DIR,
        min_transactions=MIN_TRANSACTIONS_TO_ANALYZE_WALLET,
        window_size=WINDOW_SIZE,
        var_threshold=VAR_THRESHOLD,
    )

    # _____________________________________________________________________________________________

    # DETECTION OF GAMBLING PATTERN

    RESULTS_DIR = f"Data/Results/{SERVICE}"
    PERCENT_LOW_VAR_THRESHOLD = 0.50

    run_gambling_detection(
        LOGS_DIR,
        RESULTS_DIR,
        DIRECTORY_CHUNKS,
        threshold=PERCENT_LOW_VAR_THRESHOLD,
    )


if __name__ == "__main__":
    main()
