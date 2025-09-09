"""
This module provides functions to analyze and compute metrics for wallet
transactions within specified chunks.
It includes functionality to build dataframes with wallet metrics,
calculate time variance statistics, and save the results to Excel files.
"""

import os
import json
import pandas as pd

# _________________________________________________________________________________________________


def analyze_chunk_metrics(
    chunk_to_process: str, directory_chunks: str, output_dir: str
) -> None:
    """
    Analyze metrics for a specific chunk of transactions.
    This function processes a chunk file, builds a DataFrame with wallet
    metrics, calculates average amounts, net balances, and
    time variance statistics for each wallet.
    It saves the results to an Excel file in the specified output directory.

    Args:
        chunk_to_process (str): The specific chunk file to process.
        directory_chunks (str): Dir containing the chunked transaction data.
        output_dir (str): Dir to save the metrics results.
    """
    chunk_file = f"{chunk_to_process}.json"
    metrics_file = os.path.join(output_dir, f"{chunk_file}_metrics.xlsx")

    if os.path.exists(metrics_file):
        print(f"Metrics file already exists for {chunk_to_process}, skipping.")
        return

    wallet_df = build_chunk_metrics_dataframe(
        directory_input=directory_chunks, chunk_to_process=chunk_file
    )

    if wallet_df.empty:
        print(f"No data found for {chunk_to_process}. Skipping metrics.")
        return

    wallet_df = wallet_df[wallet_df["in_degree"] > 10]
    wallet_df["average_amount"] = (
        wallet_df["total_btc_received"] / wallet_df["in_degree"]
    )
    wallet_df["net_balance"] = (
        wallet_df["total_btc_received"] - wallet_df["total_btc_sent"]
    )

    chunk_path = os.path.join(directory_chunks, chunk_file)
    transactions = load_chunk_transactions(chunk_path)
    if not transactions:
        print(f"No transactions found in {chunk_file}.")
        return

    columns_to_update = [
        "time_variance",
        "mean_time_diff",
        "std_dev_time_diff",
        "min_time_diff",
        "max_time_diff",
    ]

    for wallet_id in wallet_df["wallet_id"]:
        time_metrics = calculate_time_variance(wallet_id, transactions)
        if time_metrics:
            for col in columns_to_update:
                mask = wallet_df["wallet_id"] == wallet_id
                wallet_df.loc[mask, col] = time_metrics[col]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    wallet_df.to_excel(metrics_file, index=False)
    print(f"Metrics saved for {chunk_to_process}.")


# _________________________________________________________________________________________________


def build_chunk_metrics_dataframe(
    directory_input: str, chunk_to_process: str
) -> pd.DataFrame:
    """
    Count wallet transactions in a specific time period.

    Args:
        directory_input (str): The input directory containing transaction data.
        chunk_to_process (str): The specific chunk file to process.

    Returns:
        pd.DataFrame: DataFrame with wallet IDs and their transaction counts
        (received/sent).
    """
    records = []
    chunk_path = os.path.join(directory_input, chunk_to_process)

    if os.path.exists(chunk_path):
        with open(chunk_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        wallet_stats = {}

        for transaction in data:
            wallet_id, amount, tx_type = parse_transaction(transaction)
            if not tx_type:
                continue

            stats = wallet_stats.get(
                wallet_id,
                {
                    "in_degree": 0,
                    "out_degree": 0,
                    "total_btc_received": 0,
                    "total_btc_sent": 0,
                },
            )

            if tx_type == "received":
                stats["in_degree"] += 1
                stats["total_btc_received"] += amount
            elif tx_type == "sent":
                stats["out_degree"] += 1
                stats["total_btc_sent"] += amount

            wallet_stats[wallet_id] = stats

        for wallet_id, stats in wallet_stats.items():
            records.append({"wallet_id": wallet_id, **stats})

    df = pd.DataFrame(records)
    df.sort_values(by="out_degree", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# _________________________________________________________________________________________________


def parse_transaction(transaction: dict) -> tuple:
    """
    Parse a transaction to extract wallet ID, amount, and type.

    Args:
        transaction (dict): The transaction data.

    Returns:
        tuple: A tuple containing the wallet ID, amount, and transaction type.
    """
    if transaction["type"] == "received":
        return transaction["wallet_id"], transaction["amount"], "received"

    if transaction["type"] == "sent":
        if transaction["outputs"]:
            return (
                transaction["outputs"][0]["wallet_id"],
                transaction["outputs"][0]["amount"],
                "sent",
            )
        return "Unknown", 0, "sent"

    return None, 0, None


# _________________________________________________________________________________________________


def load_chunk_transactions(chunk_path: str) -> list:
    """
    Load transactions from a JSON file.

    Args:
        chunk_path (str): Path to the chunk file.

    Returns:
        list: List of transactions.
    """
    if not os.path.exists(chunk_path):
        print(f"Chunk file {chunk_path} does not exist.")
        return []
    with open(chunk_path, "r", encoding="utf-8") as f:
        return json.load(f)


# _________________________________________________________________________________________________


def get_wallet_transactions(wallet_id: str, transactions: list) -> list:
    """
    Get transactions for a specific wallet from the list of transactions.

    Args:
        wallet_id (str): The wallet ID to filter transactions.
        transactions (list): List of all transactions.

    Returns:
        list: List of transactions for the specified wallet.
    """
    wallet_txs = []
    for tx in transactions:
        if tx["type"] == "received" and tx["wallet_id"] == wallet_id:
            wallet_txs.append(
                {
                    "time": tx["time"],
                    "amount": tx["amount"],
                    "type": "received",
                }
            )
        elif tx["type"] == "sent" and tx["outputs"]:
            if tx["outputs"][0]["wallet_id"] == wallet_id:
                wallet_txs.append(
                    {
                        "time": tx["time"],
                        "amount": tx["outputs"][0]["amount"],
                        "type": "sent",
                    }
                )
    return wallet_txs


# _________________________________________________________________________________________________


def compute_time_differences(wallet_txs: list) -> list:
    """
    Compute time differences between transactions for a specific wallet.

    Args:
        wallet_txs (list): List of transactions for a specific wallet.

    Returns:
        list: List of time differences in seconds between consecutive
        transactions.
    """
    if not wallet_txs:
        return []

    for tx in wallet_txs:
        tx["time"] = pd.to_datetime(tx["time"], unit="s")

    wallet_txs.sort(key=lambda x: x["time"])

    time_diffs = []
    for i in range(1, len(wallet_txs)):
        time_diff = wallet_txs[i]["time"] - wallet_txs[i - 1]["time"]
        time_diffs.append(time_diff.total_seconds())

    return time_diffs


# _________________________________________________________________________________________________


def compute_time_statistics(time_diffs: list) -> dict | None:
    """
    Compute time variance statistics for a specific wallet in a given chunk.

    Args:
        time_diffs (list): List of time differences in seconds between
        consecutive transactions.

    Returns:
        dict: Dictionary containing time variance statistics.
    """
    if not time_diffs:
        return None

    series = pd.Series(time_diffs)
    stats = {
        "time_variance": series.var(),
        "mean_time_diff": series.mean(),
        "std_dev_time_diff": series.std(),
        "min_time_diff": series.min(),
        "max_time_diff": series.max(),
    }
    return stats


# _________________________________________________________________________________________________


def update_dataframe_with_stats(
    dataframe: pd.DataFrame, wallet_id: str, stats: dict
) -> None:
    """
    Update the DataFrame with the calculated statistics.

    Args:
        dataframe (pd.DataFrame): The DataFrame to update.
        wallet_id (str): The wallet ID to update.
        stats (dict): Dictionary containing the statistics to add.
    """
    if stats is None:
        return
    for key, value in stats.items():
        dataframe.loc[wallet_id, key] = value


# _________________________________________________________________________________________________


def calculate_time_variance(wallet_id: str, transactions: list) -> dict | None:
    """
    Calculate time variance statistics for a specific wallet in a given chunk.

    Args:
        wallet_id (str): The wallet ID to analyze.
        transactions (list): List of all transactions in the chunk.

    Returns:
        dict: Dictionary containing time variance statistics.
        None if no transactions found.
    """
    wallet_txs = get_wallet_transactions(wallet_id, transactions)
    if not wallet_txs:
        print(f"No transactions found for wallet {wallet_id}.")
        return None

    time_diffs = compute_time_differences(wallet_txs)
    if not time_diffs:
        print(f"No time differences found for wallet {wallet_id}.")
        return None

    print("Calculating time variance statistics for wallet:", wallet_id)
    stats = compute_time_statistics(time_diffs)
    return stats


# _________________________________________________________________________________________________


def calculate_chunk_global_metrics(
    chunk_file_path: str, global_metrics_df: pd.DataFrame, chunk_file_name: str
) -> pd.DataFrame:
    """
    Calculate global metrics for a chunk and update the global metrics
    DataFrame.

    Args:
        chunk_file_path (str): Path to the chunk file.
        global_metrics_df (pd.DataFrame): DataFrame to store global metrics.
        chunk_file_name (str): Name of the chunk file (without extension).

    Returns:
        pd.DataFrame: Updated global metrics DataFrame.
    """
    if not os.path.exists(chunk_file_path):
        print(f"Chunk file {chunk_file_path} does not exist.")
        return global_metrics_df

    chunk_df = pd.read_excel(chunk_file_path)

    total_txs = chunk_df["in_degree"].sum() + chunk_df["out_degree"].sum()
    unique_wallets = chunk_df["wallet_id"].nunique()
    total_btc_received = chunk_df["total_btc_received"].sum()
    mean_net_balance = chunk_df["net_balance"].mean()
    variance_net_balance = chunk_df["net_balance"].var()
    mean_time_variance = chunk_df["time_variance"].mean()
    variance_time_variance = chunk_df["time_variance"].var()

    new_row = pd.DataFrame(
        [
            {
                "chunk": chunk_file_name,
                "total_transactions": total_txs,
                "unique_wallets": unique_wallets,
                "total_btc_received": total_btc_received,
                "mean_net_balance": mean_net_balance,
                "variance_net_balance": variance_net_balance,
                "mean_time_variance": mean_time_variance,
                "variance_time_variance": variance_time_variance,
            }
        ]
    )

    to_concat = [global_metrics_df, new_row]
    global_metrics_df = pd.concat(to_concat, ignore_index=True)
    return global_metrics_df
