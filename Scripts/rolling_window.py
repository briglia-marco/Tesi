"""
Module for performing rolling window analysis on transaction metrics.

This module iterates through all metrics files for a given service,
applies rolling window computations to detect patterns in wallet activity,
and logs the results for later review. It also skips files that have
already been analyzed to avoid redundant computation.
"""

import config
from Scripts.utils.window_analysis_utils import (
    list_metrics_files,
    build_log_file_path,
    should_skip_analysis,
    analyze_wallets_for_file,
    save_log,
)


def run_rolling_window_analysis() -> None:
    """
    Execute rolling window analysis on all metrics files in the service directory.

    Steps performed:
    1. List all metrics files in the configured chunk metrics directory.
    2. For each file, build a corresponding log file path.
    3. Skip the file if a log already exists or if the file does not meet
       the minimum transaction threshold.
    4. Analyze wallets using rolling window metrics and generate a report.
    5. Save the analysis log to the configured logs directory.
    """
    for metrics_file in list_metrics_files(config.DIRECTORY_CHUNK_METRICS):
        print(f"\nAnalisi periodo: {metrics_file}")

        log_file_path = build_log_file_path(config.DIRECTORY_LOGS, metrics_file)

        if should_skip_analysis(
            log_file_path, config.MIN_TRANSACTIONS_TO_ANALYZE_WALLET
        ):
            print(f" -> Log already present {metrics_file}")
            continue

        log_report = analyze_wallets_for_file(
            metrics_file,
            config.DIRECTORY_CHUNK_METRICS,
            config.DIRECTORY_CHUNKS,
            config.SERVICE,
            config.WINDOW_SIZE,
            config.VAR_THRESHOLD,
            config.MIN_TRANSACTIONS_TO_ANALYZE_WALLET,
        )
        save_log(log_file_path, log_report)

    print(f"\nAnalisys complete. Log saved in {config.DIRECTORY_LOGS}.")
