import os
from Scripts.fetch import *
from Scripts.ranking import *

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

def download_wallet_addresses(wallet_ids, directory_raw_addresses):
    """
    Download all addresses associated with a list of wallet IDs by querying the WalletExplorer API.
    Downloads are skipped if the first address chunk already exists in the target directory.

    Args:
        wallet_ids (list): List of wallet IDs to fetch addresses for.
        directory_raw_addresses (str): Directory where the address JSON files will be saved.

    Returns:
        None
    """
    for wallet_id in wallet_ids:
        address_file = f"{directory_raw_addresses}/{wallet_id}_addresses_1.json"
        if not os.path.exists(address_file):
            print(f"Downloading all addresses for {wallet_id}...")
            fetch_all_addresses(wallet_id, directory_raw_addresses)

#________________________________________________________________________________________________________________________

def download_wallet_transactions(wallet_ids, directory_raw_transactions):
    """
    Download all transactions associated with a list of wallet IDs by querying the WalletExplorer API.
    Downloads are skipped if the first transaction chunk already exists in the target directory.

    Args:
        wallet_ids (list): List of wallet IDs to fetch transactions for.
        directory_raw_transactions (str): Directory where the transaction JSON files will be saved.

    Returns:
        None
    """
    for wallet_id in wallet_ids:
        tx_file = f"{directory_raw_transactions}/{wallet_id}_transactions_1.json"
        if not os.path.exists(tx_file):
            print(f"Downloading all transactions for {wallet_id}...")
            fetch_wallet_transactions(wallet_id, directory_raw_transactions)
            
#_______________________________________________________________________________________________________________________

def merge_wallet_json_files(wallet_id, directory_input, directory_output, output_suffix, data_field, count_field):
    """
    Merge all JSON files for a given wallet ID containing either addresses or transactions
    into a single JSON file. The data_field and count_field are customizable.
    Supports both dict-based and pure list JSON files.
    Args:
        wallet_id (str): The wallet ID to merge files for.
        directory_input (str): Directory containing the input JSON files.
        directory_output (str): Directory to save the merged output file.
        output_suffix (str): Suffix for the output file name.
        data_field (str): The field in the JSON to merge (e.g., "addresses" or "transactions").
        count_field (str): The field to count items in the JSON.
    Returns:
        None
    """
    merged_data = {
        "found": True,
        "label": wallet_id,
        "wallet_id": wallet_id,
        count_field: 0,
        data_field: []
    }

    files = [f for f in os.listdir(directory_input) if f.startswith(wallet_id) and f.endswith(".json")]
    files.sort()

    for file_name in files:
        file_path = os.path.join(directory_input, file_name)
        with open(file_path, "r") as f:
            data = json.load(f)

        if isinstance(data, dict) and data_field in data:
            merged_data[data_field].extend(data[data_field])
            merged_data[count_field] += data.get(count_field, len(data[data_field]))
        elif isinstance(data, list):
            merged_data[data_field].extend(data)
            merged_data[count_field] += len(data)
        else:
            print(f"Skipped {file_path}, unexpected format")

    os.makedirs(directory_output, exist_ok=True)
    output_file_name = f"{wallet_id}_{output_suffix}.json"
    output_path = os.path.join(directory_output, output_file_name)
    with open(output_path, "w") as f:
        json.dump(merged_data, f, indent=4)

    print(f"Merged file saved to {output_path}")

#_______________________________________________________________________________________________________________________

