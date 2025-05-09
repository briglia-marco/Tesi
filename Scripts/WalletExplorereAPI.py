import requests
import json
import time
import os
from bs4 import BeautifulSoup

# funzione che mi fa la classifica dei servizi di gambling di wallter explorer secondo questi criteri:
# 1. numero di transazioni
# 2. numero di indirizzi
# 3. somma incoming_txs primi 100 indirizzi
# 4. notorietà del servizio
# 5. media valore blocco utilizzato per poter stimare il periodo 

def get_wallet_ids(wallet_ids):
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

def get_transaction_count(wallet_id):
    base_url = "https://www.walletexplorer.com/"
    response = requests.get(f"{base_url}/wallet/{wallet_id}")
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return 0
    
    soup = BeautifulSoup(response.text, "html.parser")
    div = soup.find("div", class_="paging")
    small = div.select_one("small")
    if small:
        transaction_count = small.text.strip().split(" ")[-1].replace(",", "").replace(")", "")
        return int(transaction_count)
    return 0

def get_wallet_info(wallet_id, directory="Data/processed/addresses", output_file="Data/processed/info/wallets_info.json"):

    with open(f"{directory}/{wallet_id}_addresses.json", "r") as f:
        addresses = json.load(f)

    total_transactions = get_transaction_count(wallet_id)
    wallet_info = {
        "wallet_id": wallet_id,
        "total_addresses": addresses.get("addresses_count", 0),
        "total_transactions": total_transactions
    }

    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            data = json.load(f)
    else:
        data = []

    data.append(wallet_info)
    
    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)

    return wallet_info

