"""
Module for processing selected transaction chunks.

This module provides functionality to process transaction data chunks
by building graphs and analyzing metrics for each chunk that meets
a specified transaction count threshold.
"""

import pandas as pd
import config
from Scripts.utils.metrics_utils import analyze_chunk_metrics
from Scripts.utils.graph_utils import build_graphs_for_wallet


def process_selected_chunks(selected_chunk):
    """
    Process transaction chunks for a given interval.

    This function reads the chunk summary file corresponding to the specified
    interval, filters chunks based on the transaction count threshold defined
    in the configuration, and for each selected chunk:
        1. Builds the wallet graph.
        2. Computes and stores the chunk metrics.

    Args:
        selected_chunk (str): Name of the interval or chunk file prefix
        (e.g., "3_months") to process.
    """
    chunks = pd.read_excel(f"{config.DIRECTORY_XLSX}/{selected_chunk}_months.xlsx")
    chunks = chunks[chunks["count"] > config.TRANSACTIONS_FOR_CHUNK_THRESHOLD]

    for chunk in chunks["chunk"].tolist():
        print(f"Processing chunk: {chunk}")
        build_graphs_for_wallet(chunk, config.DIRECTORY_CHUNKS, config.SERVICE)
        analyze_chunk_metrics(
            chunk,
            config.DIRECTORY_CHUNKS,
            output_dir=config.DIRECTORY_CHUNK_METRICS,
        )
