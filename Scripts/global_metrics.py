"""
Module for computing and aggregating global metrics from chunk files.

This module reads all chunk metric Excel files, computes global metrics
for each chunk, and aggregates the results into a single Excel file.
"""

import os
import pandas as pd
import config
from Scripts.utils.metrics_utils import calculate_chunk_global_metrics


def process_chunk_global_metrics() -> None:
    """
    Compute and aggregate global metrics for all chunks.

    The function performs the following steps:
        1. Lists all Excel files in the chunk metrics directory.
        2. Computes global metrics for each chunk using the helper function.
        3. Aggregates all chunk metrics into a single DataFrame.
        4. Saves the aggregated metrics to an Excel file in the configured directory.
    """
    chunk_metrics_files = os.listdir(config.DIRECTORY_CHUNK_METRICS)
    df_chunk_global_metrics = pd.DataFrame()

    for chunk_file in chunk_metrics_files:
        if not chunk_file.endswith(".xlsx"):
            continue
        chunk_file_name = chunk_file.split(".")[0]
        df_chunk_global_metrics = calculate_chunk_global_metrics(
            chunk_file_path=os.path.join(config.DIRECTORY_CHUNK_METRICS, chunk_file),
            global_metrics_df=df_chunk_global_metrics,
            chunk_file_name=chunk_file_name,
        )

    df_chunk_global_metrics.to_excel(
        f"{config.DIRECTORY_XLSX}/chunk_global_metrics.xlsx", index=False
    )
