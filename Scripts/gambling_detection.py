"""
Module for detecting gambling patterns in cryptocurrency wallets.

This module provides functionality to load selected wallets based on
variance thresholds and analyze their transaction behavior over specified periods.
The results of the analysis are stored in the configured results directory.
"""

import os
import json
import config
from Scripts.utils.window_analysis_utils import load_all_wallet_bets
from Scripts.utils.gambling_utils import (
    load_selected_wallets,
    summarize_gambling_results,
    analyze_wallet,
)


def run_gambling_detection() -> None:
    """
    Execute the gambling detection workflow on selected wallets.

    The function performs the following steps:
        1. Load wallets that meet the low variance threshold criteria.
        2. Analyze each period's transactions for gambling patterns.
        3. Save the analysis results in the designated output directory.
    """
    selected_wallets = load_selected_wallets(config.DIRECTORY_LOGS)
    final_results = []
    final_dir = os.path.join(config.DIRECTORY_RESULTS, "final")
    os.makedirs(final_dir, exist_ok=True)
    results_file_path = os.path.join(final_dir, "results_bet_analysis.json")

    if not os.path.exists(results_file_path):
        wallets_dict = load_all_wallet_bets(selected_wallets, config.DIRECTORY_CHUNKS)

        for wallet_id, wallet_txs in wallets_dict.items():
            result = analyze_wallet(wallet_id, wallet_txs)

            if result:
                final_results.append(result)
                with open(results_file_path, "w", encoding="utf-8") as f:
                    json.dump(final_results, f, indent=4)

    summarize_gambling_results(final_dir, results_file_path)
