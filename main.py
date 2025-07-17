from Scripts.WalletExplorereAPI import *
from Scripts.fetch import *
from Scripts.ranking import *
from Scripts.data_processing import *
from Scripts.data_chunking import *
from Scripts.graph import *
from Scripts.metrics import *
from Scripts.plot import *
from Scripts.rolling_analisys import *
import os

if __name__ == "__main__":
    
# DOWNLOAD DATA

    directory_raw_addresses = "Data/raw/addresses"
    directory_raw_transactions = "Data/raw/transactions"
    directory_processed_100_addresses = "Data/processed/first_100_addresses"
    directory_processed_addr = "Data/processed/addresses"
    directory_processed_txs = "Data/processed/transactions"
    directory_processed_info = "Data/processed/info"

    if len(os.listdir(directory_processed_100_addresses)) == 0:
        download_first_100_addresses(
            directory_addresses=directory_processed_100_addresses,
            get_wallet_ids=get_wallet_ids,
            fetch_first_100_addresses=fetch_first_100_addresses
        )
    else:
        print("First 100 addresses are already downloaded.")

    if len(os.listdir(directory_processed_info)) == 0:
        get_all_wallets_info(
            directory=directory_processed_100_addresses,
            output_file=os.path.join(directory_processed_info, "wallets_info.json"),
        )
    print("Wallet info processed.")

#______________________________________________________________________________________________________________________
    
# MERGE JSON FILES

    w1, w2, w3, w4, w5 = 0.35, 0.03, 0.25, 0.35, 0.02  # weights for total transactions, total addresses, transactions per address, first 100 transactions/total transactions, notoriety
    
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

    # download_wallet_addresses(wallet_ids, directory_raw_addresses)
    # download_wallet_transactions(wallet_ids, directory_raw_transactions)

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
            
    #df_wallets = df_wallets[1:15]
    #df_wallets = calculate_wallet_activity(df_wallets, directory_processed_txs)

    print("All JSON files merged.")
    
#_______________________________________________________________________________________________________________________

# CHUNKING DATA

    intervals = [3, 6, 12, 24]  # months
    existing_chunk_files = set(os.listdir("Data/chunks/SatoshiDice.com-original")) if os.path.exists("Data/chunks/SatoshiDice.com-original") else set()

    for interval in intervals:
        if not f"{interval}_months" in existing_chunk_files:
            split_transactions_into_chunks(
                wallet_id="SatoshiDice.com-original",
                input_dir="Data/raw/transactions",
                output_base_dir="Data/chunks",
                intervals_months=[interval]
            )
          
    # UNCOMMENT TO GENERATE CHUNKS REPORTS IN XLSX FORMAT
    
    # generate_chunk_transaction_reports(
    #     wallet_id="SatoshiDice.com-original",
    #     base_chunk_dir="Data/chunks/SatoshiDice.com-original",
    #     intervals=intervals,
    #     output_dir="Data/chunks/SatoshiDice.com-original/xlsx"
    # )
# _______________________________________________________________________________________________________________________

# GRAPH AND METRICS FOR CHUNKS

    directory_chunks = "Data/chunks/SatoshiDice.com-original/3_months"
    chunks_to_process = pd.read_excel("Data/chunks/SatoshiDice.com-original/xlsx/3_months.xlsx")
    chunks_to_process = chunks_to_process[chunks_to_process["count"] > 100000]
    
    for chunk_to_process in chunks_to_process["chunk"].tolist():     
        print(f"Processing chunk: {chunk_to_process}")   
        build_graphs_for_wallet(chunk_to_process, directory_chunks)
        analyze_chunk_metrics(chunk_to_process, directory_chunks, output_dir="Data/chunks/SatoshiDice.com-original/xlsx/chunk_metrics")

#________________________________________________________________________________________________________________________

# CHUNK GLOBAL METRICS

    chunk_metrics_directory = "Data/chunks/SatoshiDice.com-original/xlsx/chunk_metrics"
    chunk_metrics_files = os.listdir(chunk_metrics_directory)
    chunk_global_metrics_df = pd.DataFrame()
    for chunk_file in chunk_metrics_files:
        chunk_file_name = chunk_file.split(".")[0]
        chunk_global_metrics_df = calculate_chunk_global_metrics(
            chunk_file_path=os.path.join(chunk_metrics_directory, chunk_file),
            global_metrics_df=chunk_global_metrics_df,
            chunk_file_name=chunk_file_name
        )

    chunk_global_metrics_df.to_excel(
        "Data/chunks/SatoshiDice.com-original/xlsx/chunk_global_metrics.xlsx",
        index=False
    )
    
    # plot_chunk_global_metrics(chunk_global_metrics_df)

#________________________________________________________________________________________________________________________

# ROLLING WINDOW ANALYSIS

    period_metric_file = "2013-01-18_to_2013-04-18.xlsx"
    metrics_dir = "Data/chunks/SatoshiDice.com-original/xlsx/chunk_metrics"
    json_dir = "Data/chunks/SatoshiDice.com-original/3_months"
    window_size = 10
    var_threshold = 10
    
    summary = analyze_wallet(
        period_metrics_file=period_metric_file,
        metrics_dir=metrics_dir,
        json_dir=json_dir,
        wallet_index=1, # Change index to analyze different wallets
        window_size=window_size,
        var_threshold=var_threshold
    )
    
    for key, value in summary.items():
        print(f"{key}: {value}")
    
    summary = analyze_wallet(
        period_metrics_file=period_metric_file,
        metrics_dir=metrics_dir,
        json_dir=json_dir,
        wallet_index=480,  # Change index to analyze different wallets
        window_size=window_size,
        var_threshold=var_threshold
    )
    
    for key, value in summary.items():
        print(f"{key}: {value}")
    
    summary = analyze_wallet(
        period_metrics_file=period_metric_file,
        metrics_dir=metrics_dir,
        json_dir=json_dir,
        wallet_index=645,  # Change index to analyze different wallets
        window_size=window_size,
        var_threshold=var_threshold
    )
    
    for key, value in summary.items():
        print(f"{key}: {value}")
        
    # _______________________________________________________________________________________________________________________
