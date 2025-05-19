import json
import pandas as pd
import os
from datetime import datetime, timezone


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

def get_wallet_activity_period(wallet_id, directory_transactions):
    """
    Return the first and last transaction date for a given wallet's transactions.

    Args:
        wallet_id (str): Wallet ID.
        directory_transactions (str): Directory containing transaction JSON files.

    Returns:
        tuple: (first_date, last_date) as datetime.date objects, or (None, None) if no transactions found.
    """
    timestamps = []

    for file_name in os.listdir(directory_transactions):
        if file_name.startswith(wallet_id) and file_name.endswith(".json"):
            with open(os.path.join(directory_transactions, file_name), "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "transactions" in data:
                    timestamps.extend(tx["time"] for tx in data["transactions"] if "time" in tx)

    if not timestamps:
        return (None, None)

    first_date = datetime.fromtimestamp(min(timestamps), tz=timezone.utc).date()
    last_date = datetime.fromtimestamp(max(timestamps), tz=timezone.utc).date()

    return first_date, last_date

#_______________________________________________________________________________________________________________________

def calculate_wallet_activity(df_wallets, directory_transactions):
    """
    Add first and last transaction date columns to an existing wallets dataframe.

    Args:
        df_wallets (pd.DataFrame): DataFrame with wallet IDs.
        directory_transactions (str): Directory containing transaction JSON files.

    Returns:
        pd.DataFrame: DataFrame updated with first_tx_date and last_tx_date columns.
    """
    first_dates = []
    last_dates = []

    for wallet_id in df_wallets["wallet_id"]:
        first_date, last_date = get_wallet_activity_period(wallet_id, directory_transactions)
        first_dates.append(first_date)
        last_dates.append(last_date)

    df_wallets["first_tx_date"] = first_dates
    df_wallets["last_tx_date"] = last_dates

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