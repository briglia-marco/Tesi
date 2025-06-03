import json
import pandas as pd
import os
from datetime import datetime, timezone
from collections import Counter

#_______________________________________________________________________________________________________________________

def process_wallet_dataframe(wallets_info_path, directory_addresses, known_services, w1, w2, w3, w4, w5):
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
        directory_addresses=directory_addresses,
        known_services=known_services
    )

    columns_to_normalize = ["total_transactions", "total_addresses", "transactions_per_address", "first_100_transactions"]
    df = normalize_columns(df, columns_to_normalize)
    df = calculate_scores(df, w1, w2, w3, w4, w5)

    return df

#_______________________________________________________________________________________________________________________

def build_wallets_dataframe(wallets_info_path, directory_addresses, known_services):
    """
    Build the initial dataframe with wallet statistics.
    
    Args:
        wallets_info_path (str): Path to the wallets info JSON file.
        directory_addresses (str): Directory containing wallet address files.
        known_services (list): List of known gambling services.
        
    Returns:
        pd.DataFrame: Dataframe with wallet statistics.
    """
    df = pd.DataFrame(columns=[
        "wallet_id", "total_transactions", "total_addresses",
        "transactions_per_address", "first_100_transactions", "notoriety"
    ])

    with open(wallets_info_path, "r") as f:
        data = json.load(f)

    for wallet in data:
        wallet_id = wallet["wallet_id"]
        total_transactions = wallet["total_transactions"]
        total_addresses = wallet["total_addresses"]
        transactions_per_address = total_transactions / total_addresses if total_addresses > 0 else 0
        notoriety = 1 if wallet_id in known_services else 0

        with open(f"{directory_addresses}/{wallet_id}_addresses.json", "r") as f:
            addresses_data = json.load(f)
            first_100_transactions = sum(address["incoming_txs"] for address in addresses_data["addresses"][:100])

        df.loc[len(df)] = {
            "wallet_id": wallet_id,
            "total_transactions": total_transactions,
            "total_addresses": total_addresses,
            "transactions_per_address": transactions_per_address,
            "first_100_transactions": first_100_transactions,
            "notoriety": notoriety
        }

    return df

#_______________________________________________________________________________________________________________________

def get_wallet_activity_stats(wallet_id, directory_transactions, coverage_threshold=0.8):
    """
    Return first and last transaction date, peak year and activity concentration span for a wallet.

    Args:
        wallet_id (str): Wallet ID.
        directory_transactions (str): Directory containing transaction JSON files.
        coverage_threshold (float): Target coverage fraction for concentrated activity window (default 80%).

    Returns:
        tuple: (first_date, last_date, peak_year, activity_span_years)
    """
    timestamps = []

    for file_name in os.listdir(directory_transactions):
        if file_name == f"{wallet_id}_transactions.json" or file_name.startswith(f"{wallet_id}_transactions_"):
            print(file_name)
            with open(os.path.join(directory_transactions, file_name), "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "transactions" in data:
                    timestamps.extend(tx["time"] for tx in data["transactions"] if "time" in tx)

    if not timestamps:
        return (None, None, None, None)

    dates = [datetime.fromtimestamp(t, tz=timezone.utc) for t in timestamps]
    years = [d.year for d in dates]

    year_counts = Counter(years)
    total_txs = sum(year_counts.values())

    peak_year, peak_count = year_counts.most_common(1)[0]

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

#_______________________________________________________________________________________________________________________

def calculate_wallet_activity(df_wallets, directory_transactions):
    """
    Add activity stats columns (first/last tx date, peak year, year variance) to an existing wallets dataframe.

    Args:
        df_wallets (pd.DataFrame): DataFrame with wallet IDs.
        directory_transactions (str): Directory containing transaction JSON files.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    first_dates = []
    last_dates = []
    peak_years = []
    activity_spans = []

    for wallet_id in df_wallets["wallet_id"]:
        first_date, last_date, peak_year, activity_span = get_wallet_activity_stats(wallet_id, directory_transactions)
        first_dates.append(first_date)
        last_dates.append(last_date)
        peak_years.append(peak_year)
        activity_spans.append(activity_span)

    df_wallets["first_tx_date"] = first_dates
    df_wallets["last_tx_date"] = last_dates
    df_wallets["peak_activity_year"] = peak_years
    df_wallets["activity_span_years"] = activity_spans

    return df_wallets
#_______________________________________________________________________________________________________________________

def normalize_columns(df, columns):
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

#_______________________________________________________________________________________________________________________

def calculate_scores(df, w1, w2, w3, w4, w5):
    """
    Calculate the weighted score for each wallet based on normalized columns.
    
    Args:
        df (pd.DataFrame): The dataframe with normalized columns.
        w1, w2, w3, w4, w5 (float): Weights for each column.
        
    Returns:
        pd.DataFrame: The dataframe with a new 'score' column.
    """
    df["score"] = (
        w1 * df["total_transactions_norm"] +
        w2 * df["total_addresses_norm"] +
        w3 * df["transactions_per_address_norm"] +
        w4 * df["first_100_transactions_norm"] +
        w5 * df["notoriety"]
    )
    df = df.sort_values(by="score", ascending=False).reset_index(drop=True)
    return df

#_______________________________________________________________________________________________________________________
