from Scripts.WalletExplorereAPI import *
from Scripts.fetch import *
from Scripts.ranking import *
from Scripts.data_processing import *
from Scripts.data_chunking import *
from Scripts.graph import *
from Scripts.metrics import *
from Scripts.plot import *
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":

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
    
    # BOT METRICS

    # time variance -> bassa
    # mean time diff -> bassa
    # std dev time diff -> tende a essere bassa
    # min time diff -> bassa o 0 se meno di 1 secondo


    # HUMAN METRICS

    # time variance -> alta
    # mean time diff -> alta
    # std dev time diff -> tende a essere alta
    # min time diff -> tende a essere alta

#________________________________________________________________________________________________________________________

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

    # Come applichiamo la rolling window e su quale dato:
    # Applichiamo la rolling window sulla serie temporale dei time difference calcolati tra le transazioni consecutive di un wallet. Quindi, per ogni wallet:
    # 	•	ordini le transazioni per timestamp
    # 	•	calcoli la differenza di tempo tra ogni transazione e la precedente
    # 	•	su questa serie di time difference applichi una rolling window (es. di 5 valori alla volta)
    # 	•	calcoli statistiche mobili: media, varianza, min, max, ecc.

    # PRENDO TOP 10 WALLET IN UN PERIODO DI 3 MESI E MI CALCOLO LE METRICHE MOBILI CON LA ROLLING WINDOW DI 5 TRANSAZIONI
    list_of_periods_files = os.listdir("Data/chunks/SatoshiDice.com-original/xlsx/chunk_metrics")
    test_period = list_of_periods_files[0]  # Example: "2023-01-01_2023-03-31.xlsx"
    df_wallets_period = pd.read_excel(f"Data/chunks/SatoshiDice.com-original/xlsx/chunk_metrics/{test_period}")
    df_wallets_period = df_wallets_period.sort_values(by="out_degree", ascending=False)
    
    # ANDARE A PRENDERE LE TRANSAZIONI DAL FILE JSON /3_MONTHS/... 
    top_wallet = df_wallets_period.iloc[1]["wallet_id"] #480 645
    test_period = test_period.split(".")[0]
    test_period = f"{test_period}.json"
    txs_file_path = f"Data/chunks/SatoshiDice.com-original/3_months/{test_period}"
    txs_file = json.load(open(txs_file_path, "r"))
    
    # PRENDERE LE TRANSAZIONI DEL TOP WALLET
    txs_top_wallet = [tx for tx in txs_file if tx["type"] == "sent" and tx["outputs"][0]["wallet_id"] == top_wallet]
    txs_top_wallet = sorted(txs_top_wallet, key=lambda x: x["time"])
    
    # APPLICARE LA ROLLING WINDOW SUI TIME DIFFERENCE
    timestamps = pd.to_datetime([tx["time"] for tx in txs_top_wallet], unit="s") 
    time_diffs = timestamps.diff().total_seconds().dropna()
    time_diffs_series = pd.Series(time_diffs)

    rolling_mean = time_diffs_series.rolling(10).mean()
    rolling_var = time_diffs_series.rolling(10).var()

    rolling_df = pd.DataFrame({
        "mean": rolling_mean,
        "variance": rolling_var
    })
    
    low_var_threshold = 10
    num_low_var = (rolling_var < low_var_threshold).sum()
    perc_low_var = num_low_var / len(rolling_var)
    
    print(f"Number of low variance values: {num_low_var}")
    print(f"Percentage of low variance values: {perc_low_var:.2%}")
    
    # Identify long streaks of low variance
    low_var_mask = (rolling_var < low_var_threshold).astype(int)
    groups = low_var_mask.groupby((low_var_mask != low_var_mask.shift()).cumsum())
    long_streaks = groups.sum().sort_values(ascending=False)
    
    summary = {
        "wallet_id": top_wallet,
        "n_tx": len(txs_top_wallet),
        "percent_low_var_windows": perc_low_var,
        "longest_low_var_streak": long_streaks.iloc[0],
        "mean_time_diff": time_diffs_series.mean(),
        "std_time_diff": time_diffs_series.std()
    }
    
    print("Summary of metrics for top wallet:")
    for key, value in summary.items():
        print(f"{key}: {value}")
        
    # plot section
    fig, axs = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    # Plot rolling mean
    axs[0].plot(rolling_mean, label="Rolling Mean (sec)", color="blue")
    axs[0].set_ylabel("Tempo medio")
    axs[0].set_title(f"Rolling Mean - Wallet {wallet_id}")
    axs[0].legend()
    axs[0].grid(True)

    # Plot rolling variance
    axs[1].plot(rolling_var, label="Rolling Variance (sec²)", color="orange")
    
    # Evidenzia le finestre sotto la soglia
    axs[1].axhline(y=low_var_threshold, color="red", linestyle="--", label=f"Soglia varianza = {low_var_threshold}")
    
    # Evidenzia finestre consecutive sotto soglia
    below_threshold = rolling_var < low_var_threshold
    highlighted = np.zeros_like(below_threshold, dtype=bool)

    count = 0
    for i in range(1, len(below_threshold)):
        if below_threshold.iloc[i] and below_threshold.iloc[i-1]:
            highlighted[i] = True
            highlighted[i-1] = True
            count += 1

    axs[1].fill_between(rolling_var.index, rolling_var, low_var_threshold, where=(rolling_var < low_var_threshold), 
                        interpolate=True, color='red', alpha=0.3, label="Sotto soglia")
    
    axs[1].set_ylabel("Varianza")
    axs[1].set_xlabel("Indice finestra (transazioni)")
    axs[1].set_title(f"Rolling Variance - Wallet {wallet_id}")
    axs[1].legend()
    axs[1].grid(True)

    plt.tight_layout()
    plt.show()