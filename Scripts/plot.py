import matplotlib.pyplot as plt
import seaborn as sns

# _______________________________________________________________________________________________________________________


def plot_wallet_metrics(dataframe):
    """
    Plot various metrics for wallet transactions.

    Args:
        dataframe (pd.DataFrame): DataFrame containing wallet transaction data.
    """
    plot_top_active_wallets(dataframe)
    plot_mean_time_diff_distribution(dataframe)
    plot_time_variance_distribution(dataframe)


# _______________________________________________________________________________________________________________________


def plot_chunk_global_metrics(dataframe):
    """
    Plot global metrics for the dataset.

    Args:
        dataframe (pd.DataFrame): DataFrame containing global metrics data.
    """
    dataframe.sort_values(by="chunk", inplace=True)
    plot_total_transactions(dataframe)
    plot_unique_wallets(dataframe)
    plot_total_btc_received(dataframe)
    plot_net_balance_stats(dataframe)
    # plot_time_variance_stats(dataframe)


# _______________________________________________________________________________________________________________________


def plot_total_transactions(df):
    """
    Plot the total number of transactions over different periods.

    Args:
        df (pd.DataFrame): DataFrame containing total transactions data.
    """
    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df,
        x="chunk",
        y="total_transactions",
        hue="chunk",
        palette="viridis",
        legend=False,
    )
    plt.title("Numero totale di transazioni per periodo")
    plt.xlabel("Periodo")
    plt.ylabel("Numero transazioni")
    plt.xticks(rotation=60)
    plt.tight_layout()
    plt.show()


# _______________________________________________________________________________________________________________________


def plot_unique_wallets(df):
    """
    Plot the number of unique wallets over different periods.

    Args:
        df (pd.DataFrame): DataFrame containing unique wallets data.
    """
    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df,
        x="chunk",
        y="unique_wallets",
        hue="chunk",
        palette="magma",
        legend=False,
    )
    plt.title("Numero di wallet distinti per periodo")
    plt.xlabel("Periodo")
    plt.ylabel("Numero wallet unici")
    plt.xticks(rotation=60)
    plt.tight_layout()
    plt.show()


# _______________________________________________________________________________________________________________________


def plot_total_btc_received(df):
    """
    Plot the total BTC received over different periods.

    Args:
        df (pd.DataFrame): DataFrame containing total BTC received data.
    """
    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df,
        x="chunk",
        y="total_btc_received",
        hue="chunk",
        palette="crest",
        legend=False,
    )
    plt.title("Totale BTC ricevuti per periodo")
    plt.xlabel("Periodo")
    plt.ylabel("BTC ricevuti")
    plt.xticks(rotation=60)
    plt.tight_layout()
    plt.show()


# _______________________________________________________________________________________________________________________


def plot_net_balance_stats(df):
    """
    Plot the mean and variance of net balance over time.

    Args:
        df (pd.DataFrame): DataFrame containing net balance statistics.
    """
    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=df, x="chunk", y="mean_net_balance", label="Media net balance", marker="o"
    )
    sns.lineplot(
        data=df,
        x="chunk",
        y="variance_net_balance",
        label="Varianza net balance",
        marker="o",
    )
    plt.title("Media e Varianza dei guadagni netti per periodo")
    plt.xlabel("Periodo")
    plt.ylabel("Valore")
    plt.xticks(rotation=60)
    plt.legend()
    plt.tight_layout()
    plt.show()


# _______________________________________________________________________________________________________________________


def plot_time_variance_stats(df):
    """
    Plot the time variance statistics over time.

    Args:
        df (pd.DataFrame): DataFrame containing time variance statistics.
    """
    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=df,
        x="chunk",
        y="mean_time_variance",
        label="Media time variance",
        marker="o",
    )
    sns.lineplot(
        data=df,
        x="chunk",
        y="variance_time_variance",
        label="Varianza time variance",
        marker="o",
    )
    plt.title("Media e Varianza della time variance per periodo")
    plt.xlabel("Periodo")
    plt.ylabel("Valore (secondi²)")
    plt.xticks(rotation=60)
    plt.legend()
    plt.tight_layout()
    plt.show()


# _______________________________________________________________________________________________________________________


def plot_time_variance_distribution(df):
    """
    Plot the distribution of time variance.

    Args:
        df (pd.DataFrame): DataFrame containing time variance data.
    """
    plt.figure(figsize=(10, 6))
    plt.hist(df["time_variance"], bins=30, color="blue", alpha=0.7)
    plt.title("Distribution of Time Variance")
    plt.xlabel("Time Variance")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.show()


# _______________________________________________________________________________________________________________________


def plot_top_active_wallets(df, top_n=20):
    """
    Plot the top N active wallets based on transaction counts.

    Args:
        df (pd.DataFrame): DataFrame containing wallet activity data.
        top_n (int): Number of top wallets to display.
    """
    top_wallets = df.head(top_n)

    plt.figure(figsize=(12, 6))
    plt.bar(
        top_wallets["wallet_id"], top_wallets["in_degree"], color="green", alpha=0.7
    )
    plt.title(f"Top {top_n} Active Wallets")
    plt.xlabel("Wallet ID")
    plt.ylabel("Transaction Count (Received)")
    plt.xticks(rotation=45)
    plt.grid(axis="y")
    plt.tight_layout()
    plt.show()


# _______________________________________________________________________________________________________________________


def plot_mean_time_diff_distribution(df):
    """
    Plot the distribution of mean time differences between transactions.

    Args:
        df (pd.DataFrame): DataFrame containing mean time difference data.
    """
    plt.figure(figsize=(10, 6))
    plt.hist(df["mean_time_diff"], bins=30, color="orange", alpha=0.7)
    plt.title("Distribution of Mean Time Differences")
    plt.xlabel("Mean Time Difference (seconds)")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.show()


# _______________________________________________________________________________________________________________________
