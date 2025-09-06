import requests
import json
import os
from bs4 import BeautifulSoup

# _______________________________________________________________________________________________________________________


def get_wallet_ids(wallet_ids):
    """
    Function to get the wallet IDs from the WalletExplorer website.
    It scrapes the website to find the gambling services and their wallet IDs.

    Args:
        wallet_ids (list): A list to store the wallet IDs.

    Returns:
        list: A list of wallet IDs associated with gambling services.
    """
    base_url = "https://www.walletexplorer.com/"
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", class_="serviceslist")
    tds = table.find_all("td")
    for td in tds:
        if td.h3.text == "Gambling:":
            for i in td.find_all("li"):
                wallet_id = i.a.text
                wallet_ids.append(wallet_id)
    return wallet_ids


# _______________________________________________________________________________________________________________________


def download_first_100_addresses(
    directory_addresses, get_wallet_ids, fetch_first_100_addresses
):
    """
    Download the first 100 addresses for each wallet in the given directory.
    If no wallets are present, fetch wallet IDs using get_wallet_ids.

    Args:
        directory_addresses (str): Directory where the address files will be saved.
        get_wallet_ids (function): Function to fetch wallet IDs.
        fetch_first_100_addresses (function): Function to fetch the first 100 addresses for a wallet.

    Returns:
        None
    """
    if (
        not os.path.exists(directory_addresses)
        or len(os.listdir(directory_addresses)) == 0
    ):
        wallet_ids = []
        get_wallet_ids(wallet_ids)
    else:
        wallet_ids = [f.split("_")[0] for f in os.listdir(directory_addresses)]

    for wallet_id in wallet_ids:
        address_file = os.path.join(directory_addresses, f"{wallet_id}_addresses.json")
        if not os.path.exists(address_file):
            fetch_first_100_addresses(wallet_id, directory_addresses)
        else:
            continue

    print("First 100 addresses for selected wallets downloaded.")


# _______________________________________________________________________________________________________________________


def get_transaction_count(wallet_id):
    """
    Function to get the total number of transactions for a given wallet ID.

    Args:
        wallet_id (str): The ID of the wallet.

    Returns:
        int: The total number of transactions for the wallet.
    """

    base_url = "https://www.walletexplorer.com/"
    response = requests.get(f"{base_url}/wallet/{wallet_id}")
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return 0

    soup = BeautifulSoup(response.text, "html.parser")
    div = soup.find("div", class_="paging")
    small = div.select_one("small")
    if small:
        transaction_count = (
            small.text.strip().split(" ")[-1].replace(",", "").replace(")", "")
        )
        return int(transaction_count)
    return 0


# _______________________________________________________________________________________________________________________


def get_all_wallets_info(
    directory="Data/processed/first_100_addresses",
    output_file="Data/processed/info/wallets_info.json",
):
    """
    Function to get wallet information for all wallets found in the specified directory.

    Args:
        directory (str): The directory where the address files are stored.
        output_file (str): The path to the output file where the wallets information will be saved.

    Returns:
        list: A list of dictionaries containing wallet information.
    """

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    data = []

    for file_name in os.listdir(directory):
        if file_name.endswith("_addresses.json"):
            wallet_id = file_name.replace("_addresses.json", "")

            with open(os.path.join(directory, file_name), "r") as f:
                addresses = json.load(f)
                if not addresses.get("found", False):
                    print(f"Error: Wallet {wallet_id} not found.")
                    continue

            total_transactions = get_transaction_count(wallet_id)

            wallet_info = {
                "wallet_id": wallet_id,
                "total_addresses": addresses.get("addresses_count", 0),
                "total_transactions": total_transactions,
            }

            data.append(wallet_info)
            print(f"Added info for {wallet_id}")

    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Saved wallet info to {output_file}")

    return data


# _______________________________________________________________________________________________________________________
