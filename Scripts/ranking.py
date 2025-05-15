import json
import pandas as pd

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