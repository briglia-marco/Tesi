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
from Scripts.ranking import *

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
     
    # build dataframe
    df_wallets = build_wallets_dataframe(
        wallets_info_path="Data/processed/info/wallets_info.json",
        directory_addresses=directory_addresses,
        known_services=known_services
    )

    # normalize numeric columns
    columns_to_normalize = ["total_transactions", "total_addresses", "transactions_per_address", "first_100_transactions"]
    df_wallets = normalize_columns(df_wallets, columns_to_normalize)

    # calculate final scores and rank
    df_wallets = calculate_scores(df_wallets, w1, w2, w3, w4, w5)
    
    print(df_wallets)
    

    
    
    