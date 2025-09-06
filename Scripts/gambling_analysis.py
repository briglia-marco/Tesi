import pandas as pd
import numpy as np
import os


def detect_martingale(df_txs_wallet, tol=0.05, min_prev_amount=0.00001):
    """Detect Martingale betting strategy patterns in the wallet transactions.

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
    martingale_flag = martingale_ratio > 0.5  # Threshold can be adjusted

    return {
        "martingale_ratio": martingale_ratio,
        "martingale_max_streak": martingale_max_streak,
        "martingale_flag": martingale_flag,
    }


# _______________________________________________________________________________________________________________________


def detect_dAlembert(df_txs_wallet, tol=0.01, min_prev_amount=1e-12):
    """Detect d'Alembert betting strategy patterns in the wallet transactions.

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
    dalembert_flag = dalembert_ratio > 0.5  # Threshold can be adjusted

    return {
        "dalembert_ratio": dalembert_ratio,
        "dalembert_max_streak": dalembert_max_streak,
        "dalembert_flag": dalembert_flag,
    }


# _______________________________________________________________________________________________________________________


def detect_flat_betting(df_txs_wallet, tol=0.01):
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
    flat_flag = flat_ratio > 0.5  # Threshold can be adjusted

    return {
        "flat_ratio": flat_ratio,
        "flat_max_streak": flat_max_streak,
        "flat_flag": flat_flag,
    }


# _______________________________________________________________________________________________________________________


def max_consecutive_true(mask):
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
