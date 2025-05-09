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
    
    directory = "Data/processed/addresses"
    if not os.path.exists(directory) or len(os.listdir(directory)) == 0:
        wallet_ids = []
        get_wallet_ids(wallet_ids)
    else:
        wallet_ids = [f.split("_")[0] for f in os.listdir(directory)]

    for wallet_id in wallet_ids:
        if not os.path.exists(f"{directory}/{wallet_id}_addresses.json"):
            print(f"Downloading {wallet_id}...")
            fetch_first_100_addresses(wallet_id, directory)
        else:
            print(f"First 100 for {wallet_id} already downloaded")
            #SISTEMARE STAMPA SENNò 40 VOLTE
            continue
    
    directory_raw = "Data/raw/addresses"
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
    
    # for wallet_id in wallet_ids:
    #     get_wallet_info(wallet_id)

#______________________________________________________________________________________________________________________

	# •	Costruire la classifica preliminare tra i servizi di gambling
	# •	Selezionare i migliori candidati e scaricare tutte le loro transazioni
	# •	Procedere alla parte di analisi comportamentale su mittenti → pattern Martingala/d’Alembert



          
    
    


