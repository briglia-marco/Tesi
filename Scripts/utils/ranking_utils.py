"""
Module for ranking wallets based on various metrics.
Metrics are normalized and combined into a score.
"""

import os
import json
from datetime import datetime, timezone
from collections import Counter
import pandas as pd

# _________________________________________________________________________________________________


def process_wallet_dataframe(
    wallets_info_path: str,
    directory_addresses: str,
    known_services: list,
    w1: float,
    w2: float,
    w3: float,
    w4: float,
    w5: float,
) -> pd.DataFrame:
    """
    Build dataframe, normalize columns and calculate scores.

    Args:
        wallets_info_path (str): Path to the wallets info JSON file.
        directory_addresses (str): Directory containing wallet address files.
        known_services (list): List of known gambling services.
        w1, w2, w3, w4, w5 (float): Weights for the scoring system.

    Returns:
        pd.DataFrame: Dataframe with wallet statistics and scores.
    """
    df = build_wallets_dataframe(
        wallets_info_path=wallets_info_path,
        dir_addresses=directory_addresses,
        known_services=known_services,
    )

    columns_to_normalize = [
        "total_transactions",
        "total_addresses",
        "transactions_per_address",
        "first_100_transactions",
    ]
    df = normalize_columns(df, columns_to_normalize)
    df = calculate_scores(df, w1, w2, w3, w4, w5)

    return df


# _________________________________________________________________________________________________


def build_wallets_dataframe(
    wallets_info_path: str,
    dir_addresses: str,
    known_services: list,
) -> pd.DataFrame:
    """
    Build the initial dataframe with wallet statistics.

    Args:
        wallets_info_path (str): Path to the wallets info JSON file.
        directory_addresses (str): Directory containing wallet address files.
        known_services (list): List of known gambling services.

    Returns:
        pd.DataFrame: Dataframe with wallet statistics.
    """
    df = pd.DataFrame(
        columns=[
            "wallet_id",
            "total_transactions",
            "total_addresses",
            "transactions_per_address",
            "first_100_transactions",
            "notoriety",
        ]
    )

    with open(wallets_info_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for wallet in data:
        wallet_id = wallet["wallet_id"]
        total_transactions = wallet["total_transactions"]
        total_addresses = wallet["total_addresses"]
        transactions_per_address = (
            total_transactions / total_addresses if total_addresses > 0 else 0
        )
        notoriety = 1 if wallet_id in known_services else 0

        file_path = os.path.join(dir_addresses, f"{wallet_id}_addresses.json")
        with open(file_path, "r", encoding="utf-8") as f:
            addresses_data = json.load(f)
            first_100_addresses = addresses_data["addresses"][:100]
            first_100_transactions = sum(
                address["incoming_txs"] for address in first_100_addresses
            )

        df.loc[len(df)] = {
            "wallet_id": wallet_id,
            "total_transactions": total_transactions,
            "total_addresses": total_addresses,
            "transactions_per_address": transactions_per_address,
            "first_100_transactions": first_100_transactions,
            "notoriety": notoriety,
        }

    return df


# _________________________________________________________________________________________________


def get_wallet_activity_stats(
    wallet_id: str,
    directory_transactions: str,
    coverage_threshold: float = 0.8,
) -> tuple:
    """
    Return first and last transaction date, peak year
    and activity concentration span for a wallet.

    Args:
        wallet_id (str): Wallet ID.
        directory_transactions (str): Dir containing transaction JSON files.
        coverage_threshold (float): Target coverage fraction for concentrated
        activity window (default 80%).

    Returns:
        tuple: (first_date, last_date, peak_year, activity_span_years)
    """
    timestamps = []

    for file_name in os.listdir(directory_transactions):
        is_main_file = file_name == f"{wallet_id}_transactions.json"
        is_chunked_file = file_name.startswith(f"{wallet_id}_transactions_")
        if is_main_file or is_chunked_file:
            file_path = os.path.join(directory_transactions, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "transactions" in data:
                    timestamps.extend(
                        tx["time"]
                        for tx in data["transactions"]
                        if "time" in tx  # some txs might lack a timestamp
                    )

    if not timestamps:
        return (None, None, None, None)

    dates = [datetime.fromtimestamp(t, tz=timezone.utc) for t in timestamps]
    years = [d.year for d in dates]

    year_counts = Counter(years)
    total_txs = sum(year_counts.values())

    peak_year, _ = year_counts.most_common(1)[0]

    span = 0
    while True:
        years_in_span = [y for y in years if abs(y - peak_year) <= span]
        coverage = len(years_in_span) / total_txs
        if coverage >= coverage_threshold or span > (max(years) - min(years)):
            break
        span += 1

    first_date = min(dates).date()
    last_date = max(dates).date()

    return first_date, last_date, peak_year, span


# _________________________________________________________________________________________________


def calculate_wallet_activity(
    df_wallets: pd.DataFrame,
    directory_transactions: str,
) -> pd.DataFrame:
    """
    Add activity stats columns (first/last tx date, peak year, year variance)
    to an existing wallets dataframe.

    Args:
        df_wallets (pd.DataFrame): DataFrame with wallet IDs.
        directory_transactions (str): Dir containing transaction JSON files.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    first_dates = []
    last_dates = []
    peak_years = []
    activity_spans = []

    for wallet_id in df_wallets["wallet_id"]:
        (
            first_date,
            last_date,
            peak_year,
            activity_span,
        ) = get_wallet_activity_stats(wallet_id, directory_transactions)
        first_dates.append(first_date)
        last_dates.append(last_date)
        peak_years.append(peak_year)
        activity_spans.append(activity_span)

    df_wallets["first_tx_date"] = first_dates
    df_wallets["last_tx_date"] = last_dates
    df_wallets["peak_activity_year"] = peak_years
    df_wallets["activity_span_years"] = activity_spans

    return df_wallets


# _________________________________________________________________________________________________


def normalize_columns(
    df: pd.DataFrame,
    columns: list,
) -> pd.DataFrame:
    """
    Normalize the specified columns of a dataframe using min-max scaling.

    Args:
        df (pd.DataFrame): The dataframe to normalize.
        columns (list): List of column names to normalize.

    Returns:
        pd.DataFrame: The dataframe with normalized columns.
    """
    for col in columns:
        min_val = df[col].min()
        max_val = df[col].max()
        df[f"{col}_norm"] = (df[col] - min_val) / (max_val - min_val)
    return df


# _________________________________________________________________________________________________


def calculate_scores(
    df: pd.DataFrame,
    w1: float,
    w2: float,
    w3: float,
    w4: float,
    w5: float,
) -> pd.DataFrame:
    """
    Calculate the weighted score for each wallet based on normalized columns.

    Args:
        df (pd.DataFrame): The dataframe with normalized columns.
        w1, w2, w3, w4, w5 (float): Weights for each column.

    Returns:
        pd.DataFrame: The dataframe with a new 'score' column.
    """
    df["score"] = (
        w1 * df["total_transactions_norm"]
        + w2 * df["total_addresses_norm"]
        + w3 * df["transactions_per_address_norm"]
        + w4 * df["first_100_transactions_norm"]
        + w5 * df["notoriety"]
    )
    df = df.sort_values(by="score", ascending=False).reset_index(drop=True)
    return df
