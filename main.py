
# funzione che mi fa la classifica dei servizi di gambling di wallter explorer secondo questi criteri:
# 1. numero di transazioni
# 2. numero di indirizzi
# 3. somma incoming_txs primi 100 indirizzi
# 4. notorietà del servizio
# 5. media valore blocco utilizzato per poter stimare il periodo 

import requests as req
import pandas as pd
from bs4 import BeautifulSoup
import json
from Scripts.WalletExplorereAPI import *


if __name__ == "__main__":
    
    wallet_ids = []
    # get_wallet_ids(wallet_ids)

    base_url = "https://www.walletexplorer.com/"
    from_id = 0
    
    # for wallet in wallet_ids:
    #     api_url = (f"api/1/wallet-addresses?wallet={wallet}&from={from_id}&count=100")
    #     response = req.get(base_url + api_url)
    #     data = response.json()
    #     directory = "Data/processed/addresses"
        
    #     if not os.path.exists(directory):
    #         os.makedirs(directory)
    #     with open(f"{directory}/{wallet}_addresses.json", "w") as f:
    #         json.dump(data, f, indent=4)
    
    # funzioni che fanno la classifica dei servizi di gambling secondo i criteri
    # prendere i dati da ogni json e analizzarli file per file assegnando i punti per la classifica
    
    for f in os.listdir("Data/processed/addresses"):
        wallet_id = f.split("_")[0]
        get_wallet_info(wallet_id)




          
    
    


