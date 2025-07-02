import matplotlib.pyplot as plt
import pandas as pd
import os
import json

#_______________________________________________________________________________________________________________________

def build_chunk_metrics_dataframe(directory_input, chunk_to_process):
    """
    Count wallet transactions in a specific time period.

    Args:
        directory_input (str): The input directory containing transaction data.
        chunk_to_process (str): The specific chunk file to process.
        
    Returns:
        pd.DataFrame: DataFrame with wallet IDs and their transaction counts (received/sent).
    """
    records = [] 

    chunk_path = os.path.join(directory_input, chunk_to_process)
    if os.path.exists(chunk_path):
        with open(chunk_path, "r") as f:
            data = json.load(f)
        
        wallet_stats = {}

        for transaction in data:
            if transaction["type"] == "received":
                wallet_id = transaction["wallet_id"]
                amount = transaction["amount"]
                stats = wallet_stats.get(wallet_id, {"in_degree": 0, "out_degree": 0, "total_btc_received": 0, "total_btc_sent": 0})
                stats["in_degree"] += 1
                stats["total_btc_received"] += amount
                wallet_stats[wallet_id] = stats
            elif transaction["type"] == "sent":
                if transaction["outputs"]:
                    wallet_id = transaction["outputs"][0]["wallet_id"]
                    amount = transaction["outputs"][0]["amount"]
                stats = wallet_stats.get(wallet_id, {"in_degree": 0, "out_degree": 0, "total_btc_received": 0, "total_btc_sent": 0})
                stats["out_degree"] += 1
                stats["total_btc_sent"] += amount
                wallet_stats[wallet_id] = stats

        for wallet_id, stats in wallet_stats.items():
            records.append({"wallet_id": wallet_id, **stats})

    df = pd.DataFrame(records)
    df.sort_values(by="in_degree", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

#_______________________________________________________________________________________________________________________

def load_chunk_transactions(chunk_path):
    """
    Load transactions from a JSON file.
    """
    if not os.path.exists(chunk_path):
        print(f"Chunk file {chunk_path} does not exist.")
        return []
    with open(chunk_path, "r") as f:
        return json.load(f)
    
#_______________________________________________________________________________________________________________________

def get_wallet_transactions(wallet_id, transactions):
    """
    Get transactions for a specific wallet from the list of transactions.
    """
    wallet_txs = []
    for tx in transactions:
        if tx["type"] == "received" and tx["wallet_id"] == wallet_id:
            wallet_txs.append({"time": tx["time"], "amount": tx["amount"], "type": "received"})
        elif tx["type"] == "sent" and tx["outputs"]:
            if tx["outputs"][0]["wallet_id"] == wallet_id:
                wallet_txs.append({"time": tx["time"], "amount": tx["outputs"][0]["amount"], "type": "sent"})
    return wallet_txs

#_______________________________________________________________________________________________________________________

def compute_time_differences(wallet_txs):
    """
    Compute time differences between transactions for a specific wallet.
    """
    if not wallet_txs:
        return []

    for tx in wallet_txs:
        tx["time"] = pd.to_datetime(tx["time"], unit='s')

    wallet_txs.sort(key=lambda x: x["time"])

    time_diffs = []
    for i in range(1, len(wallet_txs)):
        time_diff = wallet_txs[i]["time"] - wallet_txs[i - 1]["time"]
        time_diffs.append(time_diff.total_seconds())
    
    return time_diffs

#_______________________________________________________________________________________________________________________

def compute_time_statistics(time_diffs):
    """
    Compute time variance statistics for a specific wallet in a given chunk.
    """
    if not time_diffs:
        return None

    series = pd.Series(time_diffs)
    stats = {
        "time_variance": series.var(),
        "mean_time_diff": series.mean(),
        "std_dev_time_diff": series.std(),
        "min_time_diff": series.min(),
        "max_time_diff": series.max()
    }
    return stats

#_______________________________________________________________________________________________________________________

def update_dataframe_with_stats(dataframe, wallet_id, stats):
    """
    Update the DataFrame with the calculated statistics.
    """
    if stats is None:
        return
    for key, value in stats.items():
        dataframe.loc[wallet_id, key] = value
        
#_______________________________________________________________________________________________________________________

def calculate_time_variance(wallet_id, transactions):
    """
    Calculate time variance statistics for a specific wallet in a given chunk.
    
    Args:
        wallet_id (str): The wallet ID to analyze.
        directory_input (str): Directory containing the chunk files.
        chunk_to_process (str): The specific chunk file to process.
        
    Returns:
        None
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

#_______________________________________________________________________________________________________________________

