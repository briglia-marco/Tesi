import matplotlib.pyplot as plt
import pandas as pd
import os

def plot_metrics(dataframe):
    """
    Plot various metrics for wallet transactions.

    Args:
        dataframe (pd.DataFrame): DataFrame containing wallet transaction data.
        
    Returns:
        None
    """
    plot_top_active_wallets(dataframe)
    plot_mean_time_diff_distribution(dataframe)
    plot_time_variance_distribution(dataframe)

#_______________________________________________________________________________________________________________________

def plot_time_variance_distribution(df):
    """
    Plot the distribution of time variance.
    
    Args:
        dataframe (pd.DataFrame): DataFrame containing time variance data.
        
    Returns:
        None
    """
    plt.figure(figsize=(10, 6))
    plt.hist(df['time_variance'], bins=30, color='blue', alpha=0.7)
    plt.title('Distribution of Time Variance')
    plt.xlabel('Time Variance')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()
    
#_______________________________________________________________________________________________________________________
    
def plot_top_active_wallets(df, top_n=20):
    """
    Plot the top N active wallets based on transaction counts.
    
    Args:
        df (pd.DataFrame): DataFrame containing wallet transaction data.
        top_n (int): Number of top wallets to plot.
        
    Returns:
        None
    """
    top_wallets = df.head(top_n)
    
    plt.figure(figsize=(12, 6))
    plt.bar(top_wallets['wallet_id'], top_wallets['in_degree'], color='green', alpha=0.7)
    plt.title(f'Top {top_n} Active Wallets')
    plt.xlabel('Wallet ID')
    plt.ylabel('Transaction Count (Received)')
    plt.xticks(rotation=45)
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()
    
#_______________________________________________________________________________________________________________________
    
def plot_mean_time_diff_distribution(df):
    """
    Plot the distribution of mean time differences between transactions.
    
    Args:
        df (pd.DataFrame): DataFrame containing time difference data.
        
    Returns:
        None
    """
    plt.figure(figsize=(10, 6))
    plt.hist(df['mean_time_diff'], bins=30, color='orange', alpha=0.7)
    plt.title('Distribution of Mean Time Differences')
    plt.xlabel('Mean Time Difference (seconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()
    
#_______________________________________________________________________________________________________________________
