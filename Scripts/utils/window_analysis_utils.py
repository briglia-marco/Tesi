"""
Rolling window analysis of betting behavior for wallets.
This script analyzes the betting behavior of wallets over a specified period
using rolling window statistics. It computes rolling mean and variance of
time differences between bets, identifies patterns, and generates plots.
"""

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy.lib.stride_tricks import sliding_window_view

# _________________________________________________________________________________________________


def load_wallet_payouts(wallet_id: str, payouts_file: list[dict]) -> list[dict]:
    """
    Load wallet payouts for a specific period.

    Args:
        wallet_id (str): The ID of the wallet.
        payouts_file (list): List of all payouts in the period.

    Returns:
        list: A list of payouts for the specified wallet.
    """
    payouts_wallet = [
        payout
        for payout in payouts_file
        if payout["type"] == "sent"
        and payout["outputs"]
        and payout["outputs"][0]["wallet_id"] == wallet_id
    ]
    print(f"Wallet {wallet_id} payouts loaded.")
    payouts_wallet_sorted = sorted(payouts_wallet, key=lambda x: x["time"])
    return payouts_wallet_sorted


# _________________________________________________________________________________________________


def load_wallet_bets(wallet_id: str, txs_file: list[dict]) -> list[dict]:
    """
    Load wallet bets for a specific period.

    Args:
        wallet_id (str): The ID of the wallet.
        txs_file (list): List of all transactions in the period.

    Returns:
        list: A list of transactions for the specified wallet.
    """
    txs_wallet = [
        tx
        for tx in txs_file
        if tx["type"] == "received" and tx.get("wallet_id") == wallet_id
    ]
    print(f"Wallet {wallet_id} bet loaded.")
    txs_wallet_sorted = sorted(txs_wallet, key=lambda x: x["time"])
    return txs_wallet_sorted


# _________________________________________________________________________________________________


def load_all_wallet_bets(
    selected_wallets: list[str], dir_chunks: str
) -> dict[str, list[dict]]:
    """
    Load all transactions for all selected wallets into a single dictionary in RAM.

    Args:
        selected_wallets (list[str]): List of wallet IDs to include.
        dir_chunks (str): Directory containing chunk JSON files.

    Returns:
        dict: Keys are wallet IDs, values are lists of transactions.
    """
    wallets_dict = {wallet_id: [] for wallet_id in selected_wallets}

    for file in os.listdir(dir_chunks):
        if not file.endswith(".json"):
            continue

        file_path = os.path.join(dir_chunks, file)
        with open(file_path, "r", encoding="utf-8") as f:
            txs_file = json.load(f)

        for wallet_id in selected_wallets:
            wallets_dict[wallet_id].extend(load_wallet_bets(wallet_id, txs_file))
        print(f"\n\n\nPROCESSED FILE: {file}\n\n\n")

    for wallet_id, txs in wallets_dict.items():
        if not txs:
            print(f"Wallet {wallet_id} has no bets in any file.")
        else:
            print(f"Wallet {wallet_id} bets loaded from all files ({len(txs)} txs).")

    return wallets_dict


# _________________________________________________________________________________________________


def compute_time_differences(txs_wallet: list[dict]) -> np.ndarray:
    """
    Compute time differences between consecutive transactions in seconds.

    Args:
        txs_wallet (list): List of transactions for a specific wallet.

    Returns:
        np.ndarray: Array of time differences in seconds.
    """
    timestamps = np.array([tx["time"] for tx in txs_wallet], dtype=float)
    time_diffs = np.diff(timestamps)
    return time_diffs


# _________________________________________________________________________________________________


def compute_rolling_metrics(
    time_diffs: np.ndarray, window_size: int = 10
) -> tuple[np.ndarray, np.ndarray]:
    """
    Compute rolling mean and variance for time differences using numpy.

    Args:
        time_diffs (np.ndarray): Array of time differences.
        window_size (int): Size of the rolling window.

    Returns:
        tuple: (rolling_mean, rolling_var) as numpy arrays.
    """
    if len(time_diffs) < window_size:
        return np.array([]), np.array([])

    windows = sliding_window_view(time_diffs, window_shape=window_size)
    rolling_mean = windows.mean(axis=1)
    rolling_var = windows.var(axis=1, ddof=1)
    return rolling_mean, rolling_var


# _________________________________________________________________________________________________


def summarize_wallet_behavior(
    wallet_id: str,
    txs_wallet: list[dict],
    time_diffs: np.ndarray,
    rolling_var: np.ndarray,
    low_var_threshold: float,
) -> dict:
    """
    Summarize the behavior of a wallet based on its transaction history using numpy.

    Args:
        wallet_id (str): Wallet ID.
        txs_wallet (list): List of transactions.
        time_diffs (np.ndarray): Array of time differences.
        rolling_var (np.ndarray): Array of rolling variance.
        low_var_threshold (float): Threshold for low variance windows.

    Returns:
        dict: Summary of wallet behavior.
    """
    if len(rolling_var) == 0:
        percent_low_var_windows = 0.0
        longest_low_var_streak = 0
    else:
        low_var_mask = rolling_var < low_var_threshold
        percent_low_var_windows = round(float(low_var_mask.sum() / len(rolling_var)), 2)

        streaks = (
            np.diff(np.where(np.concatenate(([0], low_var_mask.astype(int), [0])) == 0))
            - 1
        )
        if len(streaks) > 0:
            longest_low_var_streak = int(streaks.max())
        else:
            longest_low_var_streak = 0

    mean_time_diff = round(float(time_diffs.mean()), 2) if len(time_diffs) > 0 else 0.0
    std_time_diff = round(float(time_diffs.std()), 2) if len(time_diffs) > 0 else 0.0

    return {
        "wallet_id": str(wallet_id),
        "n_tx": int(len(txs_wallet)),
        "percent_low_var_windows": percent_low_var_windows,
        "longest_low_var_streak": longest_low_var_streak,
        "mean_time_diff": mean_time_diff,
        "std_time_diff": std_time_diff,
    }


# _________________________________________________________________________________________________


def plot_rolling_metrics(
    wallet_id: str,
    rolling_mean: np.ndarray,
    rolling_var: np.ndarray,
    service: str,
) -> None:
    """
    Plot the rolling mean and variance of time differences for a wallet.

    Args:
        wallet_id (str): The ID of the wallet.
        rolling_mean (np.ndarray): Rolling mean of time differences.
        rolling_var (np.ndarray): Rolling variance of time differences.
        service (str): Service name.
    """
    _, axs = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    axs[0].plot(rolling_mean, label="Rolling Mean (sec)", color="blue")
    axs[0].set_ylabel("Mean Time")
    axs[0].set_title(f"Rolling Mean - Wallet {wallet_id}")
    axs[0].legend()
    axs[0].grid(True)

    axs[1].plot(rolling_var, label="Rolling Variance (sec²)", color="orange")
    axs[1].set_ylabel("Variance")
    axs[1].set_xlabel("Window index")
    axs[1].set_title(f"Rolling Variance - Wallet {wallet_id}")
    axs[1].legend()
    axs[1].grid(True)

    os.makedirs(f"Data/chunks/{service}/plots", exist_ok=True)
    plt.savefig(f"Data/chunks/{service}/plots/rolling_metrics_{wallet_id}.png")
    plt.close()


# _________________________________________________________________________________________________


def analyze_wallet(
    wallet_id: str,
    txs_file: dict,
    service: str,
    window_size: int = 10,
    var_threshold: float = 10,
) -> dict:
    """
    Analyze a wallet's betting behavior over a specified period.

    Args:
        wallet_id (str): The wallet ID to analyze.
        txs_file (dict): Preloaded JSON of transactions.
        df (pd.DataFrame): Preloaded metrics DataFrame.
        service (str): Service name.
        window_size (int, optional): Size of the rolling window.
        var_threshold (float, optional): Threshold for low variance.

    Returns:
        dict: A summary of the wallet's behavior.
    """
    txs_wallet = load_wallet_bets(wallet_id, txs_file)
    time_diff = compute_time_differences(txs_wallet)
    rolling_mean, rolling_var = compute_rolling_metrics(time_diff, window_size)
    summary = summarize_wallet_behavior(
        wallet_id, txs_wallet, time_diff, rolling_var, var_threshold
    )

    plot_rolling_metrics(wallet_id, rolling_mean, rolling_var, service)

    return summary


# _________________________________________________________________________________________________


def get_wallets_meeting_criteria(df: pd.DataFrame, min_tx: int) -> list[str]:
    """
    Get a list of wallet IDs that meet the specified criteria.

    Args:
        metrics_path (str): The path to the metrics file.
        min_tx (int): The minimum number of transactions required.

    Returns:
        list: A list of wallet IDs that meet the criteria.
    """
    filtered = df[df["in_degree"] >= min_tx]
    return filtered["wallet_id"].tolist()


# _________________________________________________________________________________________________


def list_metrics_files(metrics_dir: str) -> list[str]:
    """
    Returns a list of metric files in the specified directory.

    Args:
        metrics_dir (str): The directory containing metric files.

    Returns:
        list: A list of metric file names.
    """
    return [
        f
        for f in os.listdir(metrics_dir)
        if f.endswith(".xlsx") and "json_metrics" in f
    ]


# _________________________________________________________________________________________________


def build_log_file_path(log_dir: str, metrics_file: str) -> str:
    """
    Builds the log file path based on the metrics file name.

    Args:
        log_dir (str): The directory where logs are stored.
        metrics_file (str): The name of the metrics file.

    Returns:
        str: The full path to the log file.
    """
    os.makedirs(log_dir, exist_ok=True)
    file_name = f"{metrics_file.split('.')[0]}.json"
    return os.path.join(log_dir, file_name)


# _________________________________________________________________________________________________


def should_skip_analysis(log_file_path: str, min_tx: int) -> bool:
    """
    Check if the analysis should be skipped based on an existing log file.

    Args:
        log_file_path (str): The path to the log file.
        min_tx (int): The minimum number of transactions required.

    Returns:
        bool: True if analysis should be skipped, False otherwise.
    """
    if not os.path.exists(log_file_path):
        return False

    try:
        with open(log_file_path, "r", encoding="utf-8") as log_file:
            existing = json.load(log_file)
            existing_min_tx = existing.get("min_transactions")

            if existing_min_tx == min_tx:
                return True
            else:
                print(
                    f"-> Existing log '{log_file_path}' "
                    f"has min_transactions={existing_min_tx}, "
                    f"but current threshold is {min_tx}. Rerunning analysis."
                )
                return False
    except json.JSONDecodeError:
        print(f" -> File {log_file_path} not valid JSON, rerunning analysis.")
        return False


# _________________________________________________________________________________________________


def analyze_wallets_for_file(
    metrics_file: str,
    metrics_dir: str,
    json_dir: str,
    service: str,
    window_size: int,
    var_threshold: float,
    min_tx: int,
) -> dict:
    """
    Analyze all wallets in a metrics file that meet the specified criteria.

    Args:
        metrics_file (str): The name of the metrics file.
        metrics_dir (str): The directory containing the metrics files.
        json_dir (str): The directory containing the JSON files.
        service (str): The service name.
        window_size (int): The size of the rolling window.
        var_threshold (float): The threshold for low variance.
        min_tx (int): The minimum number of transactions required.

    Returns:
        dict: A log report containing the analysis results.
    """

    period_name = os.path.splitext(metrics_file)[0] + ".json"
    txs_file_path = os.path.join(json_dir, period_name)
    token = txs_file_path.split(".")
    txs_file_path = token[0] + "." + token[1] + "." + token[3]
    with open(txs_file_path, "r", encoding="utf-8") as f:
        txs_file = json.load(f)

    df = pd.read_excel(f"{metrics_dir}/{metrics_file}")
    df_sorted = df.sort_values(by="in_degree", ascending=False)

    wallet_ids = get_wallets_meeting_criteria(df_sorted, min_tx=min_tx)

    log_report = {"min_transactions": min_tx, "wallets": []}

    if not wallet_ids:
        return log_report

    for wallet_id in wallet_ids:
        summary = analyze_wallet(
            wallet_id=wallet_id,
            txs_file=txs_file,
            service=service,
            window_size=window_size,
            var_threshold=var_threshold,
        )
        if summary.get("n_tx", 0) >= min_tx:
            log_report["wallets"].append(summary)

    return log_report


# _________________________________________________________________________________________________


def save_log(log_file_path: str, log_report: dict) -> None:
    """
    Save the log report to a JSON file.

    Args:
        log_file_path (str): The path to the log file.
        log_report (dict): The log report to save.
    """
    if log_report["wallets"]:
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            json.dump(log_report, log_file, indent=4)
