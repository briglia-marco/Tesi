"""
Gambling strategy detection module.
Detects patterns such as Martingale, d'Alembert, and flat betting in wallet
transactions using pandas and numpy for data manipulation and analysis.
"""

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns

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

    return {
        "dalembert_ratio": round(dalembert_ratio, 2),
        "dalembert_max_streak": round(dalembert_max_streak, 2),
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

    return {
        "flat_ratio": round(flat_ratio, 2),
        "flat_max_streak": round(flat_max_streak, 2),
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


def load_selected_wallets(logs_dir: str) -> dict:
    """
    Load wallets from log files that meet a low variance threshold.

    Args:
        logs_dir (str): Path to the directory containing log JSON files.

    Returns:
        dict: A dictionary where keys are log file names (without extension) and
        values are lists of wallet IDs that meet the threshold.
    """
    selected_wallets = set()
    for log_file in os.listdir(logs_dir):
        if not log_file.endswith(".json"):
            continue
        log_path = os.path.join(logs_dir, log_file)
        with open(log_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df_log = pd.DataFrame(data["wallets"])

        mean_mean_time_diff = df_log["mean_time_diff"].mean()
        mean_std_time_diff = df_log["std_time_diff"].mean()

        mask = (df_log["mean_time_diff"] <= mean_mean_time_diff) & (
            df_log["std_time_diff"] <= mean_std_time_diff
        )
        df_log = df_log[mask]

        selected_wallets.update(df_log["wallet_id"].tolist())

    return list(selected_wallets)


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
    df_txs_wallet = pd.DataFrame(data)
    martingale_results = detect_martingale(df_txs_wallet)
    d_alembert_results = detect_dAlembert(df_txs_wallet)
    flat_results = detect_flat_betting(df_txs_wallet)
    return {
        "wallet_id": wallet_id,
        "n_tx": len(data),
        **martingale_results,
        **d_alembert_results,
        **flat_results,
    }


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

    return {
        "martingale_ratio": round(martingale_ratio, 2),
        "martingale_max_streak": round(martingale_max_streak, 2),
    }


# _________________________________________________________________________________________________


def summarize_gambling_results(results_dir: str, results_file_path: str) -> None:
    """
    Summarize and visualize gambling detection results across all periods.

    Args:
        results_dir (str): Directory containing the results JSON files.
    """
    all_results = []
    with open(results_file_path, "r", encoding="utf-8") as f:
        all_results.extend(json.load(f))

    if not all_results:
        print("No gambling analysis results found.")
        return

    df = pd.DataFrame(all_results)

    # ---------- Statistics ----------

    summary = {
        "n_wallets": len(df),
        "martingale_mean_streak": df["martingale_max_streak"].mean(),
        "dalembert_mean_streak": df["dalembert_max_streak"].mean(),
        "flat_mean_streak": df["flat_max_streak"].mean(),
        "martingale_max_streak": df["martingale_max_streak"].max(),
        "dalembert_max_streak": df["dalembert_max_streak"].max(),
        "flat_max_streak": df["flat_max_streak"].max(),
    }

    for key, value in summary.items():
        summary[key] = round(value, 2) if isinstance(value, float) else value
        print(f"{key}: {summary[key]}")

    # ---------- Ratio Distribution ----------

    algos = ["martingale_ratio", "dalembert_ratio", "flat_ratio"]
    colors = ["Blues", "Greens", "Reds"]

    for algo, cmap in zip(algos, colors):
        data = df[algo].dropna()
        counts, bins = np.histogram(data, bins=30)
        colors_gradient = plt.cm.get_cmap(cmap)(counts / counts.max())
        plt.figure(figsize=(7, 5))
        for i in range(len(bins) - 1):
            plt.bar(
                bins[i],
                counts[i],
                width=bins[1] - bins[0],
                color=colors_gradient[i],
                edgecolor="black",
            )

        sns.kdeplot(data, color="black", linewidth=1.2)

        plt.title(f"Distribution of {algo}", fontsize=14)
        plt.xlabel("Ratio")
        plt.ylabel("Count")
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(results_dir, f"{algo}_distribution.png"))
        plt.close()

    # ---------- Boxplot streak ----------

    plt.figure(figsize=(8, 6))
    streaks = df[["martingale_max_streak", "dalembert_max_streak", "flat_max_streak"]]
    streaks_melted = streaks.melt(var_name="Algorithm", value_name="Max Streak")

    sns.boxplot(
        data=streaks_melted,
        x="Algorithm",
        y="Max Streak",
        hue="Algorithm",
        palette="muted",
    )
    plt.yscale("log")
    plt.title("Distribution of Longest Streaks per Algorithm")
    plt.ylabel("Streak length (log scale)")
    plt.xlabel("")
    plt.tight_layout()
    plt.savefig(os.path.join(results_dir, "streaks_boxplot_with_fliers.png"))
    plt.close()

    # ---------- Istogram streak ----------

    bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, float("inf")]
    labels = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11-20", "21+"]

    df["streak_bucket"] = pd.cut(
        df["martingale_max_streak"], bins=bins, labels=labels, right=True
    )

    counts = df["streak_bucket"].value_counts().sort_index()
    counts_values = counts.values

    norm = (counts_values - counts_values.min()) / (
        counts_values.max() - counts_values.min()
    )
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "martingale_blue", ["#cce5ff", "#004c99"]
    )
    colors = cmap(norm)

    plt.figure(figsize=(7, 5))
    plt.bar(counts.index.astype(str), counts_values, color=colors, edgecolor="black")
    plt.xlabel("Max streak")
    plt.ylabel("Number of wallets")
    plt.title("Distribution of streaks for Martingale betting")
    plt.xticks(rotation=45)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(results_dir, "martingale_streaks_histogram.png"))
    plt.close()

    # ---------- Istogram streak ----------

    df["streak_bucket"] = pd.cut(
        df["dalembert_max_streak"], bins=bins, labels=labels, right=True
    )

    counts = df["streak_bucket"].value_counts().sort_index()
    counts_values = counts.values

    norm = (counts_values - counts_values.min()) / (
        counts_values.max() - counts_values.min()
    )
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "dalembert_green", ["#ccffcc", "#006600"]
    )
    colors = cmap(norm)

    plt.figure(figsize=(7, 5))
    plt.bar(counts.index.astype(str), counts_values, color=colors, edgecolor="black")
    plt.xlabel("Max streak")
    plt.ylabel("Number of wallets")
    plt.title("Distribution of streaks for D'Alembert betting")
    plt.xticks(rotation=45)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(results_dir, "dalembert_streaks_histogram.png"))
    plt.close()

    # ---------- Istogram streak ----------

    bins = [0, 5, 10, 20, 50, 100, 200, 500, 1000, float("inf")]
    labels = [
        "0-4",
        "5-9",
        "10-19",
        "20-49",
        "50-99",
        "100-199",
        "200-499",
        "500-999",
        "1000+",
    ]

    df["streak_bucket"] = pd.cut(
        df["flat_max_streak"], bins=bins, labels=labels, right=True
    )

    counts = df["streak_bucket"].value_counts().sort_index()
    counts_values = counts.values

    norm = (counts_values - counts_values.min()) / (
        counts_values.max() - counts_values.min()
    )
    cmap = mcolors.LinearSegmentedColormap.from_list("flat_red", ["#ffcccc", "#cc0000"])
    colors = cmap(norm)

    plt.figure(figsize=(7, 5))
    plt.bar(counts.index.astype(str), counts_values, color=colors, edgecolor="black")
    plt.xlabel("Max streak")
    plt.ylabel("Number of wallets")
    plt.title("Distribution of streaks for Flat betting")
    plt.xticks(rotation=45)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(results_dir, "flat_streaks_histogram.png"))
    plt.close()
