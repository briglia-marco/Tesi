"""
This module provides functionality for building and exporting graphs related to wallets
and their transactions.
It includes tools for creating wallet graphs and transaction graphs, exporting
them in formats suitable for analysis (Neo4j), and handling chunked transaction data.
"""

import os
import json
import pandas as pd
import networkx as nx


# _________________________________________________________________________________________________


def build_graphs_for_wallet(chunk_to_process, directory_chunks, service_node):
    """Build graphs for a specific wallet based on the provided chunks.

    Args:
        chunk_to_process (str): The specific chunk file to process.
        directory_chunks (str): Directory containing the chunked transaction data.
    """
    chunk_path = f"{directory_chunks}/{chunk_to_process}.json"
    edges_path = f"Data/graphs/edges_{chunk_to_process}.csv"
    nodes_path = f"Data/graphs/nodes_{chunk_to_process}.csv"

    if os.path.exists(chunk_path) and not (
        os.path.exists(edges_path) or os.path.exists(nodes_path)
    ):
        chunk_to_process_file = f"{chunk_to_process}.json"
        build_wallet_graph_for_chunk(
            base_directory=directory_chunks,
            service_node=service_node,
            chunk_to_process=chunk_to_process_file,
            output_dir="Data/graphs",
        )

        # wallet_id = "01264a56d1f8fb9e"  # Example wallet ID, adjust as needed
        # build_txs_graph_for_chunk(
        #     base_directory=directory_chunks,
        #     wallet_id=wallet_id,
        #     chunk_to_process=chunk_to_process,
        #     output_dir="Data/graphs"
        # )


# _________________________________________________________________________________________________


def build_wallet_graph_for_chunk(
    base_directory, service_node, chunk_to_process, output_dir="Data/graphs"
):
    """
    Build a wallet graph for a specific chunk of transactions.

    Args:
        base_directory (str): Base directory containing the chunked transaction data.
        service_node (str): The service node to analyze.
        chunk_to_process (str): The specific chunk file to process.
        output_dir (str): Directory to save the graph data.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    chunk_path = os.path.join(base_directory, chunk_to_process)
    if not os.path.exists(chunk_path):
        print(f"Chunk file {chunk_to_process} does not exist in {base_directory}.")
        return

    with open(chunk_path, "r", encoding="utf-8") as f:
        transactions = json.load(f)

    G = nx.MultiDiGraph()
    G.add_node(service_node, type="service")

    for transaction in transactions:
        timestamp = transaction["time"]
        transaction_id = transaction["txid"]
        if transaction["type"] == "sent":
            if transaction["outputs"]:
                amount = transaction["outputs"][0]["amount"]
                receiver = transaction["outputs"][0]["wallet_id"]
            else:
                amount = 0
                receiver = "Unknown"
            sender = service_node
            G.add_node(receiver, type="wallet")
            G.add_edge(
                sender,
                receiver,
                amount=amount,
                timestamp=timestamp,
                txid=transaction_id,
                direction="sent",
            )
        elif transaction["type"] == "received":
            amount = transaction["amount"]
            receiver = service_node
            sender = transaction["wallet_id"]
            G.add_node(sender, type="wallet")
            G.add_edge(
                sender,
                receiver,
                amount=amount,
                timestamp=timestamp,
                txid=transaction_id,
                direction="received",
            )

    print("Exporting wallet graph for chunk:", chunk_to_process)
    export_wallet_graph_for_neo4j(G, output_dir, chunk_to_process)


# _________________________________________________________________________________________________


def export_wallet_graph_for_neo4j(G, output_dir, chunk_to_process):
    """
    Export the graph to a format suitable for Neo4j.

    Args:
        G (networkx.Graph): The graph to export.
        output_dir (str): Directory to save the exported graph data.
        chunk_to_process (str): The specific chunk file being processed.
    """
    nodes_data = []
    edges_data = []
    for node, data in G.nodes(data=True):
        nodes_data.append({"id": node, "type": data.get("type", "unknown")})

    for source, target, data in G.edges(data=True):
        edges_data.append(
            {
                "source": source,
                "target": target,
                "amount": data.get("amount", 0),
                "timestamp": data.get("timestamp", ""),
                "txid": data.get("txid", ""),
                "direction": data.get("direction", ""),
            }
        )

    chunk_base_name = os.path.splitext(os.path.basename(chunk_to_process))[0]

    nodes_df = pd.DataFrame(nodes_data)
    edges_df = pd.DataFrame(edges_data)
    nodes_file = os.path.join(output_dir, f"nodes_{chunk_base_name}.csv")
    edges_file = os.path.join(output_dir, f"edges_{chunk_base_name}.csv")
    nodes_df.to_csv(nodes_file, index=False)
    edges_df.to_csv(edges_file, index=False)
    print(f"Graph saved with {len(G.nodes)} nodes and {len(G.edges)} edges.")


# _________________________________________________________________________________________________


def build_txs_graph_for_chunk(
    base_directory, wallet_id, chunk_to_process, output_dir="Data/graphs"
):
    """
    Build a transaction graph for a specific chunk of transactions
    related to a given wallet ID.

    Args:
        base_directory (str): Base directory containing the chunked transaction data.
        wallet_id (str): The wallet ID to analyze.
        chunk_to_process (str): The specific chunk file to process.
        output_dir (str): Directory to save the graph data.
    """

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    chunk_path = os.path.join(base_directory, chunk_to_process)
    if not os.path.exists(chunk_path):
        print(f"Chunk file {chunk_to_process} does not exist in {base_directory}.")
        return

    G = nx.MultiDiGraph()

    with open(chunk_path, "r", encoding="utf-8") as f:
        transactions = json.load(f)

    list_of_transactions = []
    for transaction in transactions:
        if (
            transaction["type"] == "sent"
            and transaction["outputs"][0]["wallet_id"] == wallet_id
        ):
            transaction = {
                "txid": transaction["txid"],
                "time": transaction["time"],
                "amount": transaction["outputs"][0]["amount"],
                "type": "sent",
                "wallet_id": wallet_id,
            }
            list_of_transactions.append(transaction)

        elif (
            transaction["type"] == "received" and transaction["wallet_id"] == wallet_id
        ):
            transaction = {
                "txid": transaction["txid"],
                "time": transaction["time"],
                "amount": transaction["amount"],
                "type": "received",
                "wallet_id": wallet_id,
            }
            list_of_transactions.append(transaction)

    if not list_of_transactions:
        print(
            f"No transactions found for wallet {wallet_id} in chunk {chunk_to_process}."
        )
        return

    list_of_transactions.sort(key=lambda x: x["time"])

    G.add_node(
        list_of_transactions[0]["txid"],
        type="transaction",
        timestamp=list_of_transactions[0]["time"],
        amount=list_of_transactions[0]["amount"],
    )

    for transaction in list_of_transactions[1:]:
        prev_txid = list_of_transactions[list_of_transactions.index(transaction) - 1][
            "txid"
        ]
        G.add_node(
            transaction["txid"],
            type="transaction",
            timestamp=transaction["time"],
            amount=transaction["amount"],
        )
        G.add_edge(
            prev_txid,
            transaction["txid"],
            timestamp=transaction["time"],
            amount=transaction["amount"],
            type=transaction["type"],
        )

    export_txs_graph_for_neo4j(G, output_dir, wallet_id, chunk_to_process)


# _________________________________________________________________________________________________


def export_txs_graph_for_neo4j(G, output_dir, wallet_id, chunk_to_process):
    """
    Export the transaction graph to a format suitable for Neo4j.

    Args:
        G (networkx.Graph): The graph to export.
        output_dir (str): Directory to save the exported graph data.
        wallet_id (str): The wallet ID being processed.
        chunk_to_process (str): The specific chunk file being processed.
    """
    nodes_data = []
    edges_data = []

    for node, data in G.nodes(data=True):
        nodes_data.append(
            {
                "id": node,
                "type": data.get("type", "unknown"),
            }
        )

    for source, target, data in G.edges(data=True):
        edges_data.append(
            {
                "source": source,
                "target": target,
                "type": data.get("type", "unknown"),
                "timestamp": data.get("timestamp", ""),
                "amount": data.get("amount", 0),
            }
        )

    chunk_base_name = os.path.splitext(os.path.basename(chunk_to_process))[0]

    nodes_df = pd.DataFrame(nodes_data)
    edges_df = pd.DataFrame(edges_data)
    nodes_file = os.path.join(output_dir, f"{wallet_id}_nodes_{chunk_base_name}.csv")
    edges_file = os.path.join(output_dir, f"{wallet_id}_edges_{chunk_base_name}.csv")
    nodes_df.to_csv(nodes_file, index=False)
    edges_df.to_csv(edges_file, index=False)

    print(f"Transaction graph for wallet {wallet_id} saved")


# _________________________________________________________________________________________________
