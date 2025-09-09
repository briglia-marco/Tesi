"""
This module fetch wallet transactions and addresses from WalletExplorer API
and saves the data in chunks to be better manipulated
"""

import os
import json
import time
import requests
from tqdm import tqdm

# _________________________________________________________________________________________________


def save_transactions_chunk(transactions, wallet_id, file_index, output_dir):
    """
    Save a chunk of transactions to a JSON file.

    Args:
        transactions (list): List of transactions to save.
        wallet_id (str): The wallet ID associated with the transactions.
        file_index (int): The index for the output file.
        output_dir (str): The directory where the JSON file will be saved.
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{wallet_id}_transactions_{file_index}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(transactions, f, indent=4)
    print(f"Saved chunk {file_index} with {len(transactions)} transactions.")


# _________________________________________________________________________________________________


def fetch_wallet_transactions(wallet_id, output_dir="Data/raw/transactions"):
    """
    Fetches all transactions associated with a wallet ID from the WalletExplorer API.

    Args:
        wallet_id (str): The wallet ID to fetch transactions for.
        output_dir (str): The directory where the JSON files will be saved.

    Returns:
        list: A list of all transactions associated with the wallet ID.
    """
    base_url = "https://www.walletexplorer.com/api/1/wallet"
    from_index = 0
    count = 100
    headers = {
        "User-Agent": "Mozilla/5.0"
        + "(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
        + "Chrome/58.0.3029.110 Safari/537.3"
    }
    chunk_size = 100_000
    file_index = 1
    all_transactions = []
    os.makedirs(output_dir, exist_ok=True)

    print(f"Starting download for wallet: {wallet_id}")

    pbar = tqdm(desc=f"📦 {wallet_id} transactions", unit="tx", dynamic_ncols=True)

    while True:
        url = f"{base_url}?wallet={wallet_id}&from={from_index}&count={count}"

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting 5 seconds...")
                time.sleep(5)
                continue
            print(f"HTTP error {response.status_code}: {e}")
            break
        except requests.exceptions.ConnectionError:
            print("Connection error. Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except requests.exceptions.Timeout:
            print("Request timeout. Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except requests.exceptions.RequestException as e:
            print(f"Unexpected requests error: {e}")
            break

        data = response.json()
        if not data.get("found"):
            print("No transactions found.")
            break

        transactions = data.get("txs", [])
        if not transactions:
            print("No transactions in this response.")
            break

        all_transactions.extend(transactions)
        from_index += count
        pbar.update(len(transactions))

        while len(all_transactions) >= chunk_size:
            chunk = all_transactions[:chunk_size]
            save_transactions_chunk(chunk, wallet_id, file_index, output_dir)
            file_index += 1
            all_transactions = all_transactions[chunk_size:]

        time.sleep(0.6)

    if all_transactions:
        save_transactions_chunk(all_transactions, wallet_id, file_index, output_dir)

    pbar.close()
    print(f"Completed download for {wallet_id}")
    return all_transactions


# _________________________________________________________________________________________________


def fetch_first_100_addresses(wallet_id, output_dir):
    """
    Fetches the first 100 addresses of a given wallet ID and saves them to a JSON file.

    Args:
        wallet_id (str): The wallet ID to fetch addresses for.
        output_dir (str): The directory where the JSON file will be saved.

    Returns:
        dict: The JSON response containing the first 100 addresses.
    """
    base_url = "https://www.walletexplorer.com/api/1/wallet-addresses"
    url = f"{base_url}?wallet={wallet_id}&from=0&count=100"
    response = requests.get(url, timeout=10)

    if response.status_code == 429:
        print("Rate limit exceeded. Waiting for 5 seconds...")
        time.sleep(5)
        return None

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return None

    data = response.json()

    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Save the JSON response to a file
    with open(f"{output_dir}/{wallet_id}_addresses.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    return data


# _________________________________________________________________________________________________


def save_addresses_chunk(addresses, wallet_id, label, file_index, output_dir):
    """
    Save a chunk of addresses to a JSON file.

    Args:
        addresses (list): List of addresses to save.
        wallet_id (str): The wallet ID associated with the addresses.
        label (str): The label associated with the wallet.
        file_index (int): The index for the output file.
        output_dir (str): The directory where the JSON file will be saved.
    """
    chunk_data = {
        "found": True,
        "label": label,
        "wallet_id": wallet_id,
        "addresses_count": len(addresses),
        "addresses": addresses,
    }
    file_path = os.path.join(output_dir, f"{wallet_id}_addresses_{file_index}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(chunk_data, f, indent=4)
    print(f"Saved chunk {file_index} with {len(addresses)} addresses.")


# _________________________________________________________________________________________________


def fetch_all_addresses(wallet_id, output_dir="Data/raw/addresses"):
    """
    Fetch all addresses associated with a given wallet ID from the WalletExplorer API.

    Args:
        wallet_id (str): The wallet ID to fetch addresses for.
        output_dir (str): The directory where the JSON files will be saved.
    """
    base_url = "https://www.walletexplorer.com/api/1/wallet-addresses"
    all_addresses = []
    from_index = 0
    count = 100
    chunk_size = 100_000
    file_index = 1
    label = None

    os.makedirs(output_dir, exist_ok=True)

    print(f"Starting address download for wallet: {wallet_id}")

    pbar = tqdm(desc=f"📦 {wallet_id} addresses", unit="addr", dynamic_ncols=True)

    while True:
        url = f"{base_url}?wallet={wallet_id}&from={from_index}&count={count}"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print("⚠️ Rate limit exceeded. Waiting 5 seconds...")
                time.sleep(5)
                continue
            print(f"HTTP error {response.status_code}: {e}")
            break
        except requests.exceptions.ConnectionError:
            print("Connection error. Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except requests.exceptions.Timeout:
            print("Request timeout. Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except requests.exceptions.RequestException as e:
            print(f"Unexpected requests error: {e}")
            break

        data = response.json()
        addresses = data.get("addresses", [])
        if not addresses:
            print("No more addresses found.")
            break

        if label is None:
            label = data.get("label", "unknown")

        all_addresses.extend(addresses)
        from_index += count

        pbar.update(len(addresses))

        while len(all_addresses) >= chunk_size:
            chunk = all_addresses[:chunk_size]
            save_addresses_chunk(chunk, wallet_id, label, file_index, output_dir)
            file_index += 1
            all_addresses = all_addresses[chunk_size:]

        time.sleep(0.6)

    if all_addresses:
        save_addresses_chunk(all_addresses, wallet_id, label, file_index, output_dir)

    pbar.close()
    print(f"Completed address download for {wallet_id}\n")
