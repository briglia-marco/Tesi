# 🧾 1. Get transaction info

# 📍 https://www.walletexplorer.com/api/1/tx?txid=TXID
# ➡️ Restituisce i dettagli di una specifica transazione dato il suo txid (ID della transazione).

# ⸻

# 🧾 2. Get address transactions

# 📍 https://www.walletexplorer.com/api/1/address?address=ADDRESS&from=FROM&count=100
# ➡️ Ti dà le transazioni collegate a un indirizzo specifico.
# Puoi usarla per:
# 	•	vedere tutte le scommesse fatte da un certo utente
# 	•	capire quanto ha giocato e quando

# ⸻

# 🧾 3. Get address by prefix

# 📍 https://www.walletexplorer.com/api/1/firstbits?prefix=ADDRESS_PREFIX
# ➡️ Se hai solo l’inizio di un indirizzo Bitcoin, ti restituisce l’indirizzo completo (utile se lo trovi tagliato in qualche leak o forum).

# ⸻

# 🧾 4. Get wallet by address

# 📍 https://www.walletexplorer.com/api/1/address-lookup?address=ADDRESS
# ➡️ Ti dice a quale wallet è associato un certo indirizzo.
# Utilissimo per vedere se un certo utente è legato a un servizio (es. casinò, exchange…).

# ⸻

# 🧾 5. Get wallet addresses

# 📍 https://www.walletexplorer.com/api/1/wallet-addresses?wallet=WALLET_ID&from=FROM&count=100
# ➡️ Ti elenca gli indirizzi contenuti in un wallet specifico.
# Utile se vuoi ottenere tutti gli indirizzi di un servizio di gambling.

# ⸻

# 🧾 6. Get wallet transactions

# 📍 https://www.walletexplorer.com/api/1/wallet?wallet=WALLET_ID&from=0&count=100
# ➡️ Ti restituisce le transazioni di un intero wallet, quindi puoi analizzare direttamente tutta l’attività di SatoshiDice o 999Dice ad esempio.

# ⸻

# 🧾 7. Get alternative names for a service

# 📍 https://www.walletexplorer.com/api/1/alternatives?service=SERVICE_NAME
# ➡️ Ti fornisce varianti del nome del servizio. Utile se stai cercando “Betcoin” ma è registrato come “Betcoin.ag”.

# ⸻

# 🧾 8. Search addresses in XPUB

# 📍 https://www.walletexplorer.com/api/1/xpub-addresses?pub=XPUB&gap_limit=GAP_LIMIT
# ➡️ Ti permette di esplorare gli indirizzi derivati da un determinato XPUB (chiave pubblica estesa di un HD wallet).

# ⸻

# 🧾 9. Get transactions from XPUB addresses

# 📍 https://www.walletexplorer.com/api/1/xpub-txs?pub=XPUB&gap_limit=GAP_LIMIT
# ➡️ Restituisce tutte le transazioni collegate agli indirizzi generati da un XPUB.

import requests
import json
import time
import os
from bs4 import BeautifulSoup

def get_wallet_addresses(wallet_id):
    """
    Fetches all addresses associated with a given wallet ID from the WalletExplorer API.
    Args:
        wallet_id (str): The wallet ID to fetch addresses for.
    Returns:
        list: A list of addresses associated with the wallet ID.
    """
    all_addresses = []
    base_url = "https://www.walletexplorer.com/api/1/wallet-addresses"
    from_index = 0
    count = 100
    
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
        if not data.get("found"):
            print("No more addresses found.")
            break

        addresses = data.get("addresses", [])
        if not addresses:
            print("No addresses found in this response.")
            break
        
        all_addresses.extend(addresses) 
        from_index += count 
        print(f"Fetched {len(addresses)} addresses, total: {len(all_addresses)}")
        time.sleep(0.6) # To avoid hitting the rate limit
        
    # dump the addresses to a JSON file
    with open(f"Data/raw/addresses/{wallet_id}_addresses.json", "w") as f:
        json.dump(all_addresses, f, indent=4)

    return all_addresses

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
    file_index = 13

    output_dir = "Data/raw/transactions"
    os.makedirs(output_dir, exist_ok=True)

    while True:
        url = f"{base_url}?wallet={wallet_id}&from={from_index}&count={count}"

        try:
            response = requests.get(url, headers=headers, timeout=10)
        except requests.exceptions.ConnectionError as e:
            print("Errore di connessione:", e)
            print("Aspetto 5 secondi e riprovo...")
            time.sleep(5)
            continue
        except requests.exceptions.Timeout:
            print("Timeout della richiesta, riprovo tra 5 secondi...")
            time.sleep(5)
            continue
        except Exception as e:
            print("Errore imprevisto:", e)
            break

        if response.status_code == 429:
            print("Rate limit superato. Aspetto 5 secondi...")
            time.sleep(5)
            continue

        if response.status_code != 200:
            print(f"Errore: {response.status_code}")
            break

        data = response.json()
        if not data.get("found"):
            print("Nessuna transazione trovata.")
            break

        transactions = data.get("txs", [])
        if not transactions:
            print("Nessuna transazione in questa risposta.")
            break

        all_transactions.extend(transactions)
        from_index += count

        print(f"Scaricate {len(transactions)} transazioni, totale accumulato: {len(all_transactions)}")

        # Quando si raggiungono 100.000 transazioni, salva il blocco
        while len(all_transactions) >= chunk_size:
            chunk = all_transactions[:chunk_size]
            file_path = os.path.join(output_dir, f"{wallet_id}_transactions_{file_index}.json")
            with open(file_path, "w") as f:
                json.dump(chunk, f, indent=4)
            print(f"Salvato blocco {file_index} con {len(chunk)} transazioni.")
            file_index += 1
            all_transactions = all_transactions[chunk_size:]  # Rimuove il blocco salvato

        time.sleep(0.6)

    # Salva le rimanenti (se meno di chunk_size)
    if all_transactions:
        file_path = os.path.join(output_dir, f"{wallet_id}_transactions_{file_index}.json")
        with open(file_path, "w") as f:
            json.dump(all_transactions, f, indent=4)
        print(f"Salvato ultimo blocco {file_index} con {len(all_transactions)} transazioni.")

    return all_transactions

# ______________________________________________________________________________________________________________________________________

# wallet_id = "BitZillions.com"
# wallet_addresses = get_wallet_addresses(wallet_id)
# wallet_transactions = get_wallet_trx(wallet_id)

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

#https://www.walletexplorer.com/wallet/SatoshiDice.com
def get_transaction_count(wallet_id):
    base_url = "https://www.walletexplorer.com/"
    response = requests.get(f"{base_url}/wallet/{wallet_id}")
    soup = BeautifulSoup(response.text, "html.parser")
    div = soup.find("div", class_="paging")
    small = div.select_one("small")
    print(small)
    if small:
        transaction_count = small.text.strip().split(" ")[-1].replace(",", "").replace(")", "")
        return int(transaction_count)
    return 0

def get_wallet_info(wallet_id):
    
    with open(f"Data/processed/addresses/{wallet_id}_addresses.json", "r") as f:
        addresses = json.load(f)
    
    total_transactions = get_transaction_count(wallet_id)
    with open("Data/processed/info/wallets.txt", "a") as f:
        f.write(f"Wallet ID: {wallet_id}\n")
        f.write(f"Total addresses: {addresses['addresses_count']}\n")
        f.write(f"Total transactions: {total_transactions}\n")
        f.write("\n")
    
    
    
    
    