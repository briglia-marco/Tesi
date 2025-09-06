import os
import json
import pandas as pd
import matplotlib.pyplot as plt

# _______________________________________________________________________________________________________________________


def load_wallet_payouts(wallet_id, payouts_file):
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
    print("Wallet payouts loaded.")
    payouts_wallet_sorted = sorted(payouts_wallet, key=lambda x: x["time"])
    return payouts_wallet_sorted


# ______________________________________________________________________________________________________________________


def load_wallet_bets(wallet_id, txs_file):
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
    print("Wallet transactions loaded.")
    txs_wallet_sorted = sorted(txs_wallet, key=lambda x: x["time"])
    return txs_wallet_sorted


# _______________________________________________________________________________________________________________________


def compute_time_differences(txs_wallet):
    """
    Compute time differences between consecutive transactions.

    Args:
        txs_wallet (list): List of transactions for a specific wallet.

    Returns:
        pd.Series: A pandas Series containing the time differences in seconds.
    """
    timestamps = pd.to_datetime([tx["time"] for tx in txs_wallet], unit="s")
    time_diffs = timestamps.diff().total_seconds().dropna()
    return pd.Series(time_diffs)


def compute_rolling_metrics(time_diffs_series, window_size=10):
    """
    Compute rolling metrics for time differences.

    Args:
        time_diffs_series (pd.Series): A pandas Series containing time differences.
        window_size (int, optional): The size of the rolling window. Defaults to 10.

    Returns:
        tuple: A tuple containing the rolling mean and rolling variance.
    """
    rolling_mean = time_diffs_series.rolling(window_size).mean()
    rolling_var = time_diffs_series.rolling(window_size).var()
    return rolling_mean, rolling_var


# _______________________________________________________________________________________________________________________


def summarize_wallet_behavior(
    wallet_id, txs_wallet, time_diffs_series, rolling_var, low_var_threshold
):
    """
    Summarize the behavior of a wallet based on its transaction history.

    Args:
        wallet_id (_type_): _description_
        txs_wallet (_type_): _description_
        time_diffs_series (_type_): _description_
        rolling_var (_type_): _description_
        low_var_threshold (_type_): _description_

    Returns:
        _type_: _description_
    """
    low_var_mask = (rolling_var < low_var_threshold).astype(int)
    groups = low_var_mask.groupby((low_var_mask != low_var_mask.shift()).cumsum())
    long_streaks = groups.sum().sort_values(
        ascending=False
    )  # Get the longest streaks of low variance

    return {
        "wallet_id": str(wallet_id),
        "n_tx": int(len(txs_wallet)),
        "percent_low_var_windows": round(
            float((rolling_var < low_var_threshold).sum() / len(rolling_var)), 2
        ),
        "longest_low_var_streak": (
            int(long_streaks.iloc[0]) if not long_streaks.empty else 0
        ),
        "mean_time_diff": round(float(time_diffs_series.mean()), 2),
        "std_time_diff": round(float(time_diffs_series.std()), 2),
    }


# _______________________________________________________________________________________________________________________


def plot_rolling_metrics(
    wallet_id, rolling_mean, rolling_var, low_var_threshold, service
):
    """
    Plot the rolling mean and variance of time differences for a wallet.

    Args:
        wallet_id (str): The ID of the wallet.
        rolling_mean (pd.Series): The rolling mean of time differences.
        rolling_var (pd.Series): The rolling variance of time differences.
        low_var_threshold (float): The threshold for low variance.
    """
    fig, axs = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    axs[0].plot(rolling_mean, label="Rolling Mean (sec)", color="blue")
    axs[0].set_ylabel("Tempo medio")
    axs[0].set_title(f"Rolling Mean - Wallet {wallet_id}")
    # axs[0].set_yscale("log")
    axs[0].legend()
    axs[0].grid(True)

    axs[1].plot(rolling_var, label="Rolling Variance (sec²)", color="orange")
    axs[1].axhline(
        y=low_var_threshold,
        color="red",
        linestyle="--",
        label=f"Soglia = {low_var_threshold}",
    )

    below_threshold = rolling_var < low_var_threshold
    axs[1].fill_between(
        rolling_var.index,
        rolling_var,
        low_var_threshold,
        where=below_threshold,
        interpolate=True,
        color="red",
        alpha=0.3,
        label="Sotto soglia",
    )

    axs[1].set_ylabel("Varianza")
    axs[1].set_xlabel("Indice finestra")
    axs[1].set_title(f"Rolling Variance - Wallet {wallet_id}")
    axs[1].set_yscale("log")
    axs[1].legend()
    axs[1].grid(True)

    os.makedirs(f"Data/chunks/{service}/plots", exist_ok=True)
    plt.savefig(f"Data/chunks/{service}/plots/rolling_metrics_{wallet_id}.png")
    plt.close()
    # plt.tight_layout()
    # plt.show()


# _______________________________________________________________________________________________________________________


def analyze_wallet(
    period_metrics_file,
    metrics_dir,
    json_dir,
    service,
    wallet_index=0,
    wallet_id_override=None,
    window_size=10,
    var_threshold=10,
):
    """
    Analyze a wallet's betting behavior over a specified period.

    Args:
        period_metrics_file (str): The file containing metrics for the period.
        metrics_dir (str): Directory containing the metrics files.
        json_dir (str): Directory containing the JSON files for the period.
        wallet_index (int, optional): Index of the wallet to analyze. Defaults to 0.
        wallet_id_override (str, optional): Direct wallet ID to analyze. Overrides index-based selection.
        window_size (int, optional): Size of the rolling window. Defaults to 10.
        var_threshold (float, optional): Threshold for low variance. Defaults to 10.

    Returns:
        dict: A summary of the wallet's behavior.
    """
    df = pd.read_excel(f"{metrics_dir}/{period_metrics_file}")
    df_sorted = df.sort_values(by="out_degree", ascending=False)

    if wallet_id_override is not None:
        wallet_id = wallet_id_override
    else:
        wallet_id = df_sorted.iloc[wallet_index]["wallet_id"]

    period_name = os.path.splitext(period_metrics_file)[0] + ".json"
    txs_file_path = os.path.join(json_dir, period_name)
    token = txs_file_path.split(".")
    txs_file_path = token[0] + "." + token[1] + "." + token[3]
    with open(txs_file_path, "r") as f:
        txs_file = json.load(f)

    txs_wallet = load_wallet_bets(wallet_id, txs_file)
    time_diffs = compute_time_differences(txs_wallet)
    rolling_mean, rolling_var = compute_rolling_metrics(time_diffs, window_size)
    summary = summarize_wallet_behavior(
        wallet_id, txs_wallet, time_diffs, rolling_var, var_threshold
    )

    plot_rolling_metrics(wallet_id, rolling_mean, rolling_var, var_threshold, service)

    return summary


# _______________________________________________________________________________________________________________________


def get_wallets_meeting_criteria(metrics_path, min_tx=1000):
    """
    Get a list of wallet IDs that meet the specified criteria.

    Args:
        metrics_path (str): The path to the metrics file.
        min_tx (int): The minimum number of transactions required.

    Returns:
        list: A list of wallet IDs that meet the criteria.
    """
    print(f"Loading metrics from: {metrics_path}")
    df = pd.read_excel(metrics_path)
    filtered = df[df["out_degree"] >= min_tx]
    return filtered["wallet_id"].tolist()
