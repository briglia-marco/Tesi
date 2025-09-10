"""
Module for detecting gambling patterns in cryptocurrency wallets.

This module provides functionality to load selected wallets based on
variance thresholds and analyze their transaction behavior over specified periods.
The results of the analysis are stored in the configured results directory.
"""

import os
import config
from Scripts.utils.gambling_utils import load_selected_wallets, analyze_period


def run_gambling_detection() -> None:
    """
    Execute the gambling detection workflow on selected wallets.

    The function performs the following steps:
        1. Load wallets that meet the low variance threshold criteria.
        2. Analyze each period's transactions for gambling patterns.
        3. Save the analysis results in the designated output directory.
    """
    selected_wallets = load_selected_wallets(
        config.DIRECTORY_LOGS, config.PERCENT_LOW_VAR_THRESHOLD
    )

    empty_result_files = []

    for period, wallets in selected_wallets.items():
        analyze_period(
            period, wallets, config.DIRECTORY_RESULTS, config.DIRECTORY_CHUNKS
        )
        result_file = os.path.join(config.DIRECTORY_RESULTS, f"{period}_results.json")
        if not os.path.exists(result_file) or os.path.getsize(result_file) == 0:
            empty_result_files.append(period)

    if empty_result_files:
        print(
            "\n-> Warning: These periods did not generate results or have empty files:"
        )
        for period in empty_result_files:
            print(f"  - {period}")
        print(
            f"Check the transaction chunks and wallet selection criteria.\n"
            f"You might need to adjust 'PERCENT_LOW_VAR_THRESHOLD'."
            f"'PERCENT_LOW_VAR_THRESHOLD' is now {config.PERCENT_LOW_VAR_THRESHOLD}"
        )

    print(
        f"\nGambling analysis completed. Results saved in {config.DIRECTORY_RESULTS}."
    )
