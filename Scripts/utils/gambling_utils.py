"""
Gambling strategy detection module.
Detects patterns such as Martingale, d'Alembert, and flat betting in wallet
transactions using pandas and numpy for data manipulation and analysis.
"""

import os
import json
import pandas as pd
import numpy as np
from Scripts.utils.window_analysis_utils import load_wallet_bets

# _________________________________________________________________________________________________


def detect_martingale(
    df_txs_wallet: pd.DataFrame, tol: float = 0.05, min_prev_amount: float = 0.00001
) -> dict:
    """
    Detect Martingale betting strategy patterns in the wallet transactions.

    Args:
        df_txs_wallet (pd.DataFrame): DataFrame containing wallet transaction data.
        tol (float): Tolleranza per confronti float.
        min_prev_amount (float): Minimo importo per evitare divisione per zero.

    Returns:
        dict: Contiene martingale_ratio, martingale_max_streak, martingale_flag.
    """
    df_txs_wallet = df_txs_wallet.sort_values("time").reset_index(drop=True)

    amounts = pd.to_numeric(df_txs_wallet["amount"], errors="coerce").to_numpy()
    n = len(amounts)

    if n < 2:
        return {
            "n_bets": n,
            "martingale_ratio": 0.0,
            "martingale_max_streak": 0,
            "martingale_flag": False,
        }

    prev = amounts[:-1]
    curr = amounts[1:]

    valid = prev > min_prev_amount
    ratios = np.full(prev.shape, np.nan)
    ratios[valid] = curr[valid] / prev[valid]

    martingale_mask = np.isfinite(ratios) & np.isclose(ratios, 2.0, rtol=tol)

    martingale_count = int(np.sum(martingale_mask))

    # Metrics
    martingale_ratio = float(martingale_count / martingale_mask.size)
    martingale_max_streak = max_consecutive_true(martingale_mask)

    # Boolean flag indicating potential Martingale strategy
    martingale_flag = martingale_ratio > 0.3  # Threshold can be adjusted

    return {
        "martingale_ratio": round(martingale_ratio, 2),
        "martingale_max_streak": round(martingale_max_streak, 2),
        "martingale_flag": martingale_flag,
    }


# _________________________________________________________________________________________________


def detect_dAlembert(df_txs_wallet: pd.DataFrame, tol: float = 0.01) -> dict:
    """
    Detect d'Alembert betting strategy patterns in the wallet transactions.

    Args:
        df_txs_wallet (pd.DataFrame): DataFrame containing wallet transaction data.
        tol (float): tolerance for float comparisons.
        min_prev_amount (float): minimum amount to avoid division by zero.

    Returns:
        dict: contains dalembert_ratio, dalembert_max_streak, dalembert_flag
    """
    df_txs_wallet = df_txs_wallet.sort_values("time").reset_index(drop=True)

    amounts = pd.to_numeric(df_txs_wallet["amount"], errors="coerce").to_numpy()
    n = len(amounts)

    if n < 2:
        return {
            "n_bets": n,
            "dalembert_ratio": 0.0,
            "dalembert_max_streak": 0,
            "dalembert_flag": False,
        }

    prev = amounts[:-1]
    curr = amounts[1:]
    diffs = curr - prev

    dalembert_mask = np.isclose(diffs, 1.0, rtol=tol) | np.isclose(
        diffs, -1.0, rtol=tol
    )

    dalembert_count = int(np.sum(dalembert_mask))

    # Metrics
    dalembert_ratio = float(dalembert_count / dalembert_mask.size)
    dalembert_max_streak = max_consecutive_true(dalembert_mask)

    # Boolean flag indicating potential d'Alembert strategy
    dalembert_flag = dalembert_ratio > 0.3  # Threshold can be adjusted

    return {
        "dalembert_ratio": round(dalembert_ratio, 2),
        "dalembert_max_streak": round(dalembert_max_streak, 2),
        "dalembert_flag": dalembert_flag,
    }


# _________________________________________________________________________________________________


def detect_flat_betting(df_txs_wallet: pd.DataFrame, tol: float = 0.01) -> dict:
    """
    Detect flat betting pattern in wallet transactions.
    A flat bettor consistently bets the same amount.

    Args:
        df_txs_wallet (pd.DataFrame): Wallet bets with 'amount' and 'time'.
        tol (float): Tolerance for float comparison (relative).

    Returns:
        dict: Contains flat_ratio, flat_max_streak, flat_flag.
    """
    df_txs_wallet = df_txs_wallet.sort_values("time").reset_index(drop=True)

    amounts = pd.to_numeric(df_txs_wallet["amount"], errors="coerce").to_numpy()
    n = len(amounts)

    if n == 0:
        return {
            "n_bets": 0,
            "flat_ratio": 0.0,
            "flat_max_streak": 0,
            "flat_flag": False,
        }

    prev = amounts[:-1]
    curr = amounts[1:]

    flat_mask = np.isclose(curr, prev, rtol=tol)

    # Metrics
    flat_ratio = (
        float(np.sum(flat_mask) / flat_mask.size) if flat_mask.size > 0 else 1.0
    )
    flat_max_streak = max_consecutive_true(flat_mask)

    # Boolean flag indicating potential flat betting strategy
    flat_flag = flat_ratio > 0.3  # Threshold can be adjusted

    return {
        "flat_ratio": round(flat_ratio, 2),
        "flat_max_streak": round(flat_max_streak, 2),
        "flat_flag": flat_flag,
    }


# _________________________________________________________________________________________________


def max_consecutive_true(mask: np.ndarray) -> int:
    """
    Count the maximum number of consecutive True values in a boolean array.

    Args:
        mask (np.ndarray): Boolean array.

    Returns:
        int: Length of the longest sequence of True values.
    """
    m = mask.astype(int)
    padded = np.concatenate(([0], m, [0]))
    diff = np.diff(padded)
    starts = np.where(diff == 1)[0]
    ends = np.where(diff == -1)[0]
    lengths = ends - starts
    return int(lengths.max()) if lengths.size else 0


# _________________________________________________________________________________________________


def load_selected_wallets(logs_dir: str, threshold: float) -> dict:
    """
    Load wallets from log files that meet a low variance threshold.

    Args:
        logs_dir (str): Path to the directory containing log JSON files.
        threshold (float): Minimum percentage of low-variance windows for a wallet
        to be selected.

    Returns:
        dict: A dictionary where keys are log file names (without extension) and
        values are lists of wallet IDs that meet the threshold.
    """
    selected_wallets = {}
    for log_file in os.listdir(logs_dir):
        if not log_file.endswith(".json"):
            continue
        log_path = os.path.join(logs_dir, log_file)
        with open(log_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df_log = pd.DataFrame(data["wallets"])
        mask = df_log["percent_low_var_windows"] >= threshold
        df_log = df_log[mask]
        key = log_file.split(".")[0]
        selected_wallets[key] = df_log["wallet_id"].tolist()
    return selected_wallets


# _________________________________________________________________________________________________


def analyze_wallet(wallet_id: str, data: list[dict]) -> dict | None:
    """
    Analyze the betting patterns of a wallet using different strategies.

    This function loads all transactions for a given wallet, then applies
    detection methods for Martingale, d'Alembert, and flat betting patterns.
    It returns a dictionary summarizing the wallet's activity and the results
    of each detection method.

    Args:
        wallet_id (str): The unique identifier of the wallet to analyze.
        data (list[dict]): The dataset containing transaction information.

    Returns:
        dict or None: A dictionary containing the calculated metrics in the modules
    """
    txs_wallet = load_wallet_bets(wallet_id, data)
    if not txs_wallet:
        print(f"Wallet {wallet_id} has no transactions. Skipping.")
        return None
    df_txs_wallet = pd.DataFrame(txs_wallet)
    martingale_results = detect_martingale(df_txs_wallet)
    d_alembert_results = detect_dAlembert(df_txs_wallet)
    flat_results = detect_flat_betting(df_txs_wallet)
    return {
        "wallet_id": wallet_id,
        "n_tx": len(txs_wallet),
        **martingale_results,
        **d_alembert_results,
        **flat_results,
    }


# _________________________________________________________________________________________________


def analyze_period(
    period: str, wallets: list[str], results_dir: str, dir_chunks: str
) -> None:
    """
    Analyze the wallets of a given period and save the results as JSON files.

    Args:
        period (str): The time period identifier (e.g., chunk name).
        wallets (list[str]): List of wallet IDs to analyze.
        results_dir (str): Directory where results JSON files will be saved.
        dir_chunks (str): Directory containing the transaction chunk files.
    """
    json_file_path = os.path.join(dir_chunks, f"{period}.json")
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    period_results = []
    for wallet_id in wallets:
        result = analyze_wallet(wallet_id, data)
        if result:
            period_results.append(result)

    results_file_path = os.path.join(results_dir, f"{period}_bet_analysis.json")
    os.makedirs(results_dir, exist_ok=True)
    with open(results_file_path, "w", encoding="utf-8") as f:
        json.dump(period_results, f, indent=4)
