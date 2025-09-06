from Scripts.WalletExplorereAPI import *
from Scripts.fetch import *
from Scripts.ranking import *
from Scripts.data_processing import *
from Scripts.data_chunking import *
from Scripts.graph import *
from Scripts.metrics import *
from Scripts.plot import *
from Scripts.rolling_analisys import *
from Scripts.gambling_analysis import *
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
            fetch_first_100_addresses=fetch_first_100_addresses,
        )
    else:
        print("First 100 addresses are already downloaded.")

    if len(os.listdir(directory_processed_info)) == 0:
        get_all_wallets_info(
            directory=directory_processed_100_addresses,
            output_file=os.path.join(directory_processed_info, "wallets_info.json"),
        )
    print("Wallet info processed.")

    # ______________________________________________________________________________________________________________________

    # MERGE JSON FILES

    w1, w2, w3, w4, w5 = (
        0.35,
        0.03,
        0.25,
        0.35,
        0.02,
    )  # weights for total transactions, total addresses, transactions per address, first 100 transactions/total transactions, notoriety

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
        "SatoshiDice.com-original",
        "Coinroll.com",
        "777Coin.com",
        "Crypto-Games.net",
        "SwCPoker.eu",
    ]

    df_wallets = process_wallet_dataframe(
        wallets_info_path="Data/processed/info/wallets_info.json",
        directory_addresses=directory_processed_100_addresses,
        known_services=known_services,
        w1=w1,
        w2=w2,
        w3=w3,
        w4=w4,
        w5=w5,
    )

    wallet_ids = df_wallets["wallet_id"].iloc[:15].tolist()

    # UNCOMMENT TO DOWNLOAD RAW DATA

    # download_wallet_addresses(wallet_ids, directory_raw_addresses)
    # download_wallet_transactions(wallet_ids, directory_raw_transactions)

    existing_merged_addresses = (
        set(os.listdir(directory_processed_addr))
        if os.path.exists(directory_processed_addr)
        else set()
    )
    existing_merged_transactions = (
        set(os.listdir(directory_processed_txs))
        if os.path.exists(directory_processed_txs)
        else set()
    )

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
                count_field="addresses_count",
            )

        if not merged_transaction_file in existing_merged_transactions:
            merge_wallet_json_files(
                wallet_id=wallet_id,
                directory_input=directory_raw_transactions,
                directory_output=directory_processed_txs,
                output_suffix="transactions",
                data_field="transactions",
                count_field="transactions_count",
            )

    # df_wallets = df_wallets[1:15]
    # df_wallets = calculate_wallet_activity(df_wallets, directory_processed_txs)

    print("All JSON files merged.")

    # _______________________________________________________________________________________________________________________

    # CHUNKING DATA

    service = "SatoshiDice.com-original"  # Example service, can be changed
    transactions_for_chunk_threshold = (
        100000  # Minimum number of transactions to consider for chunking
    )
    min_transactions_for_wallet_to_analyze = 1000  # modify this threshold as needed
    # SatoshiDice.com-original 100000, 1000
    # BitZillions.com, 15000, 1000

    intervals = [3, 6, 12, 24]  # months
    existing_chunk_files = (
        set(os.listdir(f"Data/chunks/{service}"))
        if os.path.exists(f"Data/chunks/{service}")
        else set()
    )

    for interval in intervals:
        if not f"{interval}_months" in existing_chunk_files:
            split_transactions_into_chunks(
                wallet_id=service,
                input_dir="Data/raw/transactions",
                output_base_dir="Data/chunks",
                intervals_months=[interval],
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

    directory_chunks = f"Data/chunks/{service}/3_months"
    chunks_to_process = pd.read_excel(f"Data/chunks/{service}/xlsx/3_months.xlsx")
    chunks_to_process = chunks_to_process[
        chunks_to_process["count"] > transactions_for_chunk_threshold
    ]  # modify this threshold as needed

    for chunk_to_process in chunks_to_process["chunk"].tolist():
        print(f"Processing chunk: {chunk_to_process}")
        build_graphs_for_wallet(chunk_to_process, directory_chunks, service)
        analyze_chunk_metrics(
            chunk_to_process,
            directory_chunks,
            output_dir=f"Data/chunks/{service}/xlsx/chunk_metrics",
        )

    # ________________________________________________________________________________________________________________________

    # CHUNK GLOBAL METRICS

    chunk_metrics_directory = f"Data/chunks/{service}/xlsx/chunk_metrics"
    chunk_metrics_files = os.listdir(chunk_metrics_directory)
    chunk_global_metrics_df = pd.DataFrame()
    for chunk_file in chunk_metrics_files:
        if not chunk_file.endswith(".xlsx"):
            continue
        chunk_file_name = chunk_file.split(".")[0]
        chunk_global_metrics_df = calculate_chunk_global_metrics(
            chunk_file_path=os.path.join(chunk_metrics_directory, chunk_file),
            global_metrics_df=chunk_global_metrics_df,
            chunk_file_name=chunk_file_name,
        )

    chunk_global_metrics_df.to_excel(
        f"Data/chunks/{service}/xlsx/chunk_global_metrics.xlsx", index=False
    )

    # ________________________________________________________________________________________________________________________

    # ROLLING WINDOW ANALYSIS

    metrics_dir = f"Data/chunks/{service}/xlsx/chunk_metrics"
    json_dir = f"Data/chunks/{service}/3_months"

    window_size = 10
    var_threshold = 10

    all_metrics_files = [
        f
        for f in os.listdir(metrics_dir)
        if f.endswith(".xlsx") and "json_metrics" in f
    ]

    for metrics_file in all_metrics_files:
        print(f"\nAnalisi periodo: {metrics_file}")

        metrics_path = os.path.join(metrics_dir, metrics_file)

        log_dir = f"Data/chunks/{service}/logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"{metrics_file.split('.')[0]}.json")

        skip_analysis = False
        if os.path.exists(log_file_path):
            try:
                with open(log_file_path, "r") as log_file:
                    existing = json.load(log_file)
                    logged_min_tx = existing.get("min_transactions")
                    if logged_min_tx == min_transactions_for_wallet_to_analyze:
                        print(f" -> Log already present {metrics_file}")
                        skip_analysis = True
            except json.JSONDecodeError:
                print(f" -> File {log_file_path} not valid, redoing analysis")

        if skip_analysis:
            continue

        wallet_ids = get_wallets_meeting_criteria(
            metrics_path, min_tx=min_transactions_for_wallet_to_analyze
        )

        log_report = {
            "min_transactions": min_transactions_for_wallet_to_analyze,
            "wallets": [],
        }

        for wallet_id in wallet_ids:
            summary = analyze_wallet(
                period_metrics_file=metrics_file,
                metrics_dir=metrics_dir,
                json_dir=json_dir,
                service=service,
                wallet_id_override=wallet_id,
                window_size=window_size,
                var_threshold=var_threshold,
            )
            if summary.get("n_tx", 0) >= min_transactions_for_wallet_to_analyze:
                log_report["wallets"].append(summary)

        if log_report["wallets"]:
            with open(log_file_path, "w") as log_file:
                json.dump(log_report, log_file, indent=4)

    # _______________________________________________________________________________________________________________________

    # DETECTION OF GAMBLING PATTERN

    logs_folder = f"Data/chunks/{service}/logs"
    results_folder = f"Data/Results/{service}"
    percent_low_var_windows_treshold = 0.50
    selected_wallets = {}

    # scan logs to find wallets meeting the low variance windows criteria
    for log_file in os.listdir(logs_folder):
        if not log_file.endswith(".json"):
            continue
        log_path = os.path.join(logs_folder, log_file)
        with open(log_path, "r") as f:
            data = json.load(f)
        df_log = pd.DataFrame(data["wallets"])
        df_log = df_log[
            df_log["percent_low_var_windows"] >= percent_low_var_windows_treshold
        ]
        selected_wallets[f"{log_file.split('.')[0]}"] = df_log["wallet_id"].tolist()

    for period, wallets in selected_wallets.items():
        json_file_path = os.path.join(
            f"Data/chunks/{service}/3_months", f"{period}.json"
        )
        with open(json_file_path, "r") as f:
            data = json.load(f)
        period_results = []
        for wallet_id in wallets:
            txs_wallet = load_wallet_bets(wallet_id, data)
            if not txs_wallet:
                print(f"Wallet {wallet_id} has no transactions in {period}. Skipping.")
                continue
            df_txs_wallet = pd.DataFrame(txs_wallet)
            martingale_results = detect_martingale(df_txs_wallet)
            dAlembert_results = detect_dAlembert(df_txs_wallet)
            flat_results = detect_flat_betting(df_txs_wallet)
            combined_results = {
                "wallet_id": wallet_id,
                "n_tx": len(txs_wallet),
                **martingale_results,
                **dAlembert_results,
                **flat_results,
            }
            period_results.append(combined_results)

        results_path = os.path.join(results_folder, f"{period}_gambling_analysis.json")
        os.makedirs(results_folder, exist_ok=True)
        with open(results_path, "w") as f:
            json.dump(period_results, f, indent=4)
