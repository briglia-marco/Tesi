"""
This script orchestrates the entire process of downloading, processing,
analyzing, and detecting gambling patterns in cryptocurrency wallet data.
"""

import config
from Scripts.build_graph import process_selected_chunks
from Scripts.global_metrics import process_chunk_global_metrics
from Scripts.rolling_window import run_rolling_window_analysis
from Scripts.gambling_detection import run_gambling_detection
from Scripts.download_pipeline import run_download_pipeline
from Scripts.process_chunk import process_chunks


def main() -> None:
    """
    Entry point of the program.

    Sets the target service and threshold parameters, then starts
    the pipeline for downloading, processing, and analyzing
    transactions of the selected wallets.
    """

    # DOWNLOAD DATA
    run_download_pipeline(config.DO_MERGE)

    # CHUNKING DATA
    process_chunks()

    # GRAPH AND METRICS
    interval = config.INTERVALS[0]  # 3 months
    process_selected_chunks(interval)

    # CHUNK GLOBAL METRICS
    process_chunk_global_metrics()

    # ROLLING WINDOW ANALYSIS
    run_rolling_window_analysis()

    # DETECTION OF GAMBLING PATTERN
    run_gambling_detection()


if __name__ == "__main__":
    main()
