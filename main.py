from Scripts.WalletExplorereAPI import *
from Scripts.fetch import *
from Scripts.ranking import *
from Scripts.data_processing import *
import os

if __name__ == "__main__":

    directory_raw_addresses = "Data/raw/addresses"
    directory_raw_transactions = "Data/raw/transactions"
    directory_processed_100_addresses = "Data/processed/first_100_addresses"
    directory_processed_addr = "Data/processed/addresses"
    directory_processed_txs = "Data/processed/transactions"
    directory_processed_info = "Data/processed/info"

    if not os.path.exists(directory_processed_100_addresses) or len(os.listdir(directory_processed_100_addresses)) == 0:
        download_first_100_addresses(
            directory_addresses=directory_processed_100_addresses,
            get_wallet_ids=get_wallet_ids,
            fetch_first_100_addresses=fetch_first_100_addresses
        )
    else:
        print("First 100 addresses are already downloaded.")
        
    if not os.path.exists(directory_processed_info) or len(os.listdir(directory_processed_info)) == 0:
        get_wallet_info(
            directory=directory_processed_100_addresses,
            output_file=os.path.join(directory_processed_info, "wallets_info.json"),
        )
    print("Wallet info processed.")

#______________________________________________________________________________________________________________________
    
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
    
    df_wallets = process_wallet_dataframe(
        wallets_info_path="Data/processed/info/wallets_info.json",
        directory_addresses=directory_processed_100_addresses,
        known_services=known_services,
        w1=w1, w2=w2, w3=w3, w4=w4, w5=w5
    )

    wallet_ids = df_wallets["wallet_id"].iloc[:15].tolist()

    download_wallet_addresses(wallet_ids, directory_raw_addresses)
    print("All addresses for selected wallets downloaded.")

    download_wallet_transactions(wallet_ids, directory_raw_transactions)
    print("All transactions for selected wallets downloaded.")

    existing_merged_addresses = set(os.listdir(directory_processed_addr)) if os.path.exists(directory_processed_addr) else set()
    existing_merged_transactions = set(os.listdir(directory_processed_txs)) if os.path.exists(directory_processed_txs) else set()
    
    for wallet_id in wallet_ids:
        merged_address_file = f"{wallet_id}_addresses.json"
        merged_transaction_file = f"{wallet_id}_transactions.json"

        if not merged_address_file in existing_merged_addresses:
            merge_wallet_json_files(
                wallet_id=wallet_id,
                directory_input=directory_raw_addresses,
                directory_output=directory_processed_addr,
                output_suffix="addresses",
                data_field="addresses",
                count_field="addresses_count"
            )

        if not merged_transaction_file in existing_merged_transactions:
            merge_wallet_json_files(
                wallet_id=wallet_id,
                directory_input=directory_raw_transactions,
                directory_output=directory_processed_txs,
                output_suffix="transactions",
                data_field="transactions",
                count_field="transactions_count"
            )
            
    df_wallets = calculate_wallet_activity(df_wallets, directory_processed_txs)

    print("All JSON files merged.")
    print("All data processing complete.")
    
#_______________________________________________________________________________________________________________________




