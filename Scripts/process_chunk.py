"""
Module for processing transaction chunks for a given service/wallet.

This module organizes raw transactions into time-based chunks, generates
aggregated statistics for each interval, and prepares the data for further
analysis such as rolling window metrics and gambling pattern detection.
"""

import os
import config
from Scripts.utils.data_chunking_utils import (
    split_transactions_into_chunks,
    generate_chunk_transaction_reports,
)


def process_chunks() -> None:
    """
    Process and generate transaction chunks for analysis.

    Steps performed:
    1. Check if chunk directories exist for each configured time interval.
    2. Split raw transactions into chunks for missing intervals.
    3. Generate Excel reports summarizing transactions per chunk.
    4. Skip report generation if all reports are already available.

    This function ensures that transaction data is properly organized
    into time windows to facilitate downstream analyses such as
    rolling window metrics computation and pattern detection.
    """
    if os.path.exists(config.DIRECTORY_SERVICE):
        existing_chunk_files = set(os.listdir(config.DIRECTORY_SERVICE))
    else:
        existing_chunk_files = set()

    for interval in config.INTERVALS:
        if not f"{interval}_months" in existing_chunk_files:
            split_transactions_into_chunks(
                wallet_id=config.SERVICE,
                input_dir=config.DIRECTORY_RAW_TRANSACTIONS,
                output_base_dir="Data/chunks",
                intervals_months=[interval],
            )

    os.makedirs(config.DIRECTORY_XLSX, exist_ok=True)
    all_reports_exist = all(
        os.path.exists(os.path.join(config.DIRECTORY_XLSX, f"{interval}_months.xlsx"))
        for interval in config.INTERVALS
    )

    if not all_reports_exist:
        generate_chunk_transaction_reports(
            base_chunk_dir=config.DIRECTORY_SERVICE,
            intervals=config.INTERVALS,
            output_dir=config.DIRECTORY_XLSX,
        )
    else:
        print("[INFO] All chunk reports already exist, skipping generation.")
