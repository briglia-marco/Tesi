# funzione che mi fa la classifica dei servizi di gambling di wallter explorer secondo questi criteri:
# 1. numero di transazioni
# 2. numero di indirizzi
# 3. somma incoming_txs primi 100 indirizzi
# 4. notorietà del servizio
# 5. media valore blocco utilizzato per poter stimare il periodo 

import requests as req
import pandas as pd
import json
from bs4 import BeautifulSoup
from Scripts.WalletExplorereAPI import *
from Scripts.fetch import *

if __name__ == "__main__":
    
    directory_addresses = "Data/processed/addresses"
    if not os.path.exists(directory_addresses) or len(os.listdir(directory_addresses)) == 0:
        wallet_ids = []
        get_wallet_ids(wallet_ids)
    else:
        wallet_ids = [f.split("_")[0] for f in os.listdir(directory_addresses)]

    for wallet_id in wallet_ids:
        if not os.path.exists(f"{directory_addresses}/{wallet_id}_addresses.json"):
            print(f"Downloading {wallet_id}...")
            fetch_first_100_addresses(wallet_id, directory_addresses)
        else:
            print(f"First 100 for {wallet_id} already downloaded")
            continue
    
    directory_raw = "Data/raw/addresses"
    
    # for wallet_id in wallet_ids:
    #     get_wallet_info(wallet_id)
    
    '''
    FUNZIONA PER SCARICARE TUTTI GLI INDIRIZZI
    '''
    # for wallet_id in wallet_ids:
    #     if not os.path.exists(f"{directory_raw}/{wallet_id}_addresses.json"):
    #         print(f"Downloading all addresses for {wallet_id}...")
    #         fetch_all_addresses(wallet_id, directory_raw)
    #     else:
    #         print("All addresses for every wallet already downloaded")
    #         break


#______________________________________________________________________________________________________________________

	# •	Costruire la classifica preliminare tra i servizi di gambling

    # Somma = w1*tot_txs + w2*tot_addresses + w3*tot_txs/tot_addresses + w4*txs_first_100/tot_txs + w5*notoriety

    # w1+w2+w3+w4+w5 = 1
    w1 = 0.35 # weight for total transactions
    w2 = 0.03 # weight for total addresses
    w3 = 0.25 # weight for transactions per address
    w4 = 0.35 # weight for first 100 transactions/total transactions
    w5 = 0.02 # weight for notoriety
    
    known_services = [
    "SatoshiDice.com",
    "999Dice.com",
    "PrimeDice.com",
    "Betcoin.ag",
    "BitZino.com",
    "FortuneJack.com",
    "CloudBet.com",
    "BitcoinVideoCasino.com",
    "NitrogenSports.eu",
    "YABTCL.com",
    "Coinroll.com",
    "777Coin.com",
    "Crypto-Games.net",
    "SwCPoker.eu"
    ]
    
    df_wallets = pd.DataFrame(columns=["wallet_id", "total_transactions", "total_addresses", "transactions_per_address", "first_100_transactions", "notoriety"])
        
    # recupero total_addresses e total_transactions da wallet_info.json
    with open(f"Data/processed/info/wallets_info.json", "r") as f:
        data = json.load(f)
        
    for wallet in data:
        wallet_id = wallet["wallet_id"]
        total_transactions = wallet["total_transactions"]
        total_addresses = wallet["total_addresses"]
        
        # calcolo transactions_per_address
        transactions_per_address = total_transactions / total_addresses if total_addresses > 0 else 0
        
        # calcolo notorietà
        notoriety = 1 if wallet_id in known_services else 0
        
        # calcolo first_100_transactions
        with open(f"{directory_addresses}/{wallet_id}_addresses.json", "r") as f:
            addresses_data = json.load(f)
            first_100_transactions = sum(address["incoming_txs"] for address in addresses_data["addresses"][:100])
        
        # add the row to the dataframe
        df_wallets.loc[len(df_wallets)] = { 
            "wallet_id": wallet_id,
            "total_transactions": total_transactions,
            "total_addresses": total_addresses,
            "transactions_per_address": transactions_per_address,
            "first_100_transactions": first_100_transactions,
            "notoriety": notoriety,
        }
        
    for col in ["total_transactions", "total_addresses", "transactions_per_address", "first_100_transactions"]:
        min_val = df_wallets[col].min()
        max_val = df_wallets[col].max()
        df_wallets[f"{col}_norm"] = (df_wallets[col] - min_val) / (max_val - min_val)
        
    # calculate score with normalized values
    df_wallets["score"] = (w1 * df_wallets["total_transactions_norm"] +
                            w2 * df_wallets["total_addresses_norm"] +
                            w3 * df_wallets["transactions_per_address_norm"] +
                            w4 * df_wallets["first_100_transactions_norm"] +
                            w5 * df_wallets["notoriety"])
    # sort the dataframe by score
    df_wallets = df_wallets.sort_values(by="score", ascending=False)
    df_wallets = df_wallets.reset_index(drop=True)
    
    print(df_wallets)

    # save the dataframe to a CSV file
    df_wallets.to_csv("Data/processed/wallets_score.csv", index=False)
    print("Classifica salvata in Data/processed/wallets_score.csv")
    
    # find known services in the dataframe
    known_services_df = df_wallets[df_wallets["wallet_id"].isin(known_services)]
    print("Servizi di gambling noti:")
    print(known_services_df[["wallet_id", "score"]])