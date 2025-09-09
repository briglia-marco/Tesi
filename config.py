"""
Config file to setup directories, service and thresholds
"""

# SET THE TARGET SERVICE AND THE THRESHOLD PARAMETERS

# SatoshiDice.com-original, (100000, 1000)
# BitZillions.com, (10000, 200)
SERVICE = "SatoshiDice.com-original"
TRANSACTIONS_FOR_CHUNK_THRESHOLD = 100000
MIN_TRANSACTIONS_TO_ANALYZE_WALLET = 1000

WINDOW_SIZE = 10
VAR_THRESHOLD = 10
PERCENT_LOW_VAR_THRESHOLD = 0.50


DIRECTORY_RAW_ADDRESSES = "Data/raw/addresses"
DIRECTORY_RAW_TRANSACTIONS = "Data/raw/transactions"
DIRECTORY_PROCESSED_100_ADDRESSES = "Data/processed/first_100_addresses"
DIRECTORY_PROCESSED_ADDR = "Data/processed/addresses"
DIRECTORY_PROCESSED_TXS = "Data/processed/transactions"
DIRECTORY_PRO_INFO = "Data/processed/info"
DIRECTORY_SERVICE = f"Data/chunks/{SERVICE}"
DIRECTORY_CHUNKS = f"{DIRECTORY_SERVICE}/3_months"
DIRECTORY_XLSX = f"{DIRECTORY_SERVICE}/xlsx"
DIRECTORY_CHUNK_METRICS = f"{DIRECTORY_XLSX}/chunk_metrics"
DIRECTORY_LOGS = f"{DIRECTORY_SERVICE}/logs"
DIRECTORY_RESULTS = f"Data/Results/{SERVICE}"


DO_MERGE = False

INTERVALS = [3, 6, 12, 24]  # months

W1, W2, W3, W4, W5 = (
    0.35,  # total transactions
    0.03,  # total addresses
    0.25,  # transactions per address
    0.35,  # first 100 transactions/total transactions
    0.02,  # notoriety
)

KNOWN_SERVICES = [
    "SatoshiDice.com-original",
    "SatoshiDice.com",
    "BitZillions.com",
    "999Dice.com",
    "Betcoin.ag",
    "CloudBet.com",
]
