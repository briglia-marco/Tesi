import requests
import json
import time
import os
from bs4 import BeautifulSoup

#_______________________________________________________________________________________________________________________

def get_wallet_trx(wallet_id):
    """
    Fetches all transactions associated with a given wallet ID from the WalletExplorer API.
    Args:
        wallet_id (str): The wallet ID to fetch transactions for.
    Returns:
        list: A list of transactions associated with the wallet ID.
    """
    base_url = "https://www.walletexplorer.com/api/1/wallet"
    from_index = 0
    count = 100
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    all_transactions = []
    chunk_size = 100000
    file_index = 0

    output_dir = "Data/raw/transactions"
    os.makedirs(output_dir, exist_ok=True)

    while True:
        url = f"{base_url}?wallet={wallet_id}&from={from_index}&count={count}"

        try:
            response = requests.get(url, headers=headers, timeout=10)
        except requests.exceptions.ConnectionError as e:
            print("Connection error:", e)
            print("Waiting 5 seconds and retrying...")
            time.sleep(5)
            continue
        except requests.exceptions.Timeout:
            print("Request timeout, retrying in 5 seconds...")
            time.sleep(5)
            continue
        except Exception as e:
            print("Unexpected error:", e)
            break

        if response.status_code == 429:
            print("Rate limit exceeded. Waiting 5 seconds...")
            time.sleep(5)
            continue

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
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

        print(f"Downloaded {len(transactions)} transactions, total accumulated: {len(all_transactions)}")

        # When reaching 100,000 transactions, save the chunk
        while len(all_transactions) >= chunk_size:
            chunk = all_transactions[:chunk_size]
            file_path = os.path.join(output_dir, f"{wallet_id}_transactions_{file_index}.json")
            with open(file_path, "w") as f:
                json.dump(chunk, f, indent=4)
            print(f"Saved chunk {file_index} with {len(chunk)} transactions.")
            file_index += 1
            all_transactions = all_transactions[chunk_size:]

        time.sleep(0.6)

    # Save any remaining transactions at the end
    if all_transactions:
        file_path = os.path.join(output_dir, f"{wallet_id}_transactions_{file_index}.json")
        with open(file_path, "w") as f:
            json.dump(all_transactions, f, indent=4)
        print(f"Saved last chunk {file_index} with {len(all_transactions)} transactions.")

    return all_transactions

#_______________________________________________________________________________________________________________________

def fetch_first_100_addresses(wallet_id, output_dir):
    """
    Fetches the first 100 addresses associated with a given wallet ID and saves them to a JSON file.
    Args:
        wallet_id (str): The wallet ID to fetch addresses for.
        output_dir (str): The directory where the JSON file will be saved.
    Returns:
        dict: The JSON response containing the first 100 addresses.
    """
    base_url = "https://www.walletexplorer.com/api/1/wallet-addresses"
    url = f"{base_url}?wallet={wallet_id}&from=0&count=100"
    response = requests.get(url)

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
    with open(f"{output_dir}/{wallet_id}_addresses.json", "w") as f:
        json.dump(data, f, indent=4)

    return data

#_______________________________________________________________________________________________________________________

def fetch_all_addresses(wallet_id, output_dir="Data/raw/addresses"):
    """
    Fetches all addresses associated with a given wallet ID from the WalletExplorer API.
    Args:
        wallet_id (str): The wallet ID to fetch addresses for.
        output_dir (str): The directory where the JSON files will be saved.
    Returns:
        None
    """
    base_url = "https://www.walletexplorer.com/api/1/wallet-addresses"
    all_addresses = []
    from_index = 0
    count = 100
    chunk_size = 100_000
    file_index = 1
    label = None 

    while True:
        url = f"{base_url}?wallet={wallet_id}&from={from_index}&count={count}"
        response = requests.get(url)

        if response.status_code == 429:
            print("Rate limit exceeded. Waiting for 5 seconds...")
            time.sleep(5)
            continue

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break

        data = response.json()
        addresses = data.get("addresses", [])
        if not addresses:
            break

        if label is None:
            label = data.get("label", "unknown")

        all_addresses.extend(addresses)
        from_index += count

        print(f"Fetched {len(addresses)} addresses, total accumulated: {len(all_addresses)}")

        # When reaching 100,000 addresses, save the chunk
        while len(all_addresses) >= chunk_size:
            chunk = all_addresses[:chunk_size]
            chunk_data = {
                "found": True,
                "label": label,
                "wallet_id": wallet_id,
                "addresses_count": len(chunk),
                "addresses": chunk
            }
            file_path = f"{output_dir}/{wallet_id}_addresses_{file_index}.json"
            with open(file_path, "w") as f:
                json.dump(chunk_data, f, indent=4)
            print(f"Saved chunk {file_index} with {len(chunk)} addresses.")
            file_index += 1
            all_addresses = all_addresses[chunk_size:]

        time.sleep(0.6)

    # Save any remaining addresses at the end
    if all_addresses:
        chunk_data = {
            "found": True,
            "label": label,
            "wallet_id": wallet_id,
            "addresses_count": len(all_addresses),
            "addresses": all_addresses
        }
        file_path = f"{output_dir}/{wallet_id}_addresses_{file_index}.json"
        with open(file_path, "w") as f:
            json.dump(chunk_data, f, indent=4)
        print(f"Saved last chunk {file_index} with {len(all_addresses)} addresses.")
        
