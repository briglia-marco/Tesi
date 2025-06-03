import os
import json
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

#_______________________________________________________________________________________________________________________

def build_wallet_graph_for_chunk(base_directory, service_node, chunk_to_process, output_dir="Data/graphs"):
    """
    Build a wallet graph for a specific chunk of transactions.
    
    Args:
        base_directory (str): Base directory containing the chunked transaction data.
        service_node (str): The service node to analyze.
        chunk_to_process (str): The specific chunk file to process.
        output_dir (str): Directory to save the graph data.

    Returns:
        None
    """

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    chunk_path = os.path.join(base_directory, chunk_to_process)
    if not os.path.exists(chunk_path):
        print(f"Chunk file {chunk_to_process} does not exist in {base_directory}.")
        return
    
    with open(chunk_path, "r") as f:
        transactions = json.load(f)
        
    G = nx.MultiDiGraph()
    G.add_node(service_node, type='service')
    
    for transaction in transactions:
        timestamp = transaction["time"]
        transaction_id = transaction["txid"]
        if transaction["type"] == "sent":
            amount = transaction["outputs"][0]["amount"]
            receiver = transaction["outputs"][0]["wallet_id"]
            sender = service_node
            G.add_node(receiver, type='wallet')
            G.add_edge(sender, receiver, amount=amount, timestamp=timestamp, txid=transaction_id, direction="sent")
        elif transaction["type"] == "received":
            amount = transaction["amount"]
            receiver = service_node
            sender = transaction["wallet_id"]
            G.add_node(sender, type='wallet')
            G.add_edge(sender, receiver, amount=amount, timestamp=timestamp, txid=transaction_id, direction="received")

    export_graph_for_neo4j(G, output_dir)
    
#_______________________________________________________________________________________________________________________
def export_graph_for_neo4j(G, output_dir):
    """
    Export the graph to a format suitable for Neo4j.
    
    Args:
        G (networkx.Graph): The graph to export.
        output_dir (str): Directory to save the exported graph data.

    Returns:
        None
    """
    nodes_data = []
    edges_data = []
    for node, data in G.nodes(data=True):
        nodes_data.append({
            "id": node,
            "type": data.get("type", "unknown")
        })
        
    for source, target, data in G.edges(data=True):
        edges_data.append({
            "source": source,
            "target": target,
            "amount": data.get("amount", 0),
            "timestamp": data.get("timestamp", ""),
            "txid": data.get("txid", ""),
            "direction": data.get("direction", "")
        })
    
    nodes_df = pd.DataFrame(nodes_data)
    edges_df = pd.DataFrame(edges_data)
    nodes_file = os.path.join(output_dir, "nodes.csv")
    edges_file = os.path.join(output_dir, "edges.csv")
    nodes_df.to_csv(nodes_file, index=False)
    edges_df.to_csv(edges_file, index=False)
    print(f"Graph exported to {output_dir} with {len(G.nodes)} nodes and {len(G.edges)} edges.")
            

        
        
            
            
        
        
    

