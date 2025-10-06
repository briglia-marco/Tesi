from Scripts.utils.graph_utils import build_txs_graph_for_chunk

# 000a9c635b54c194
# 000e4e1383db9e68
# 000a9ca4906ec8fb
wallet_id = "000a9ca4906ec8fb"
period = "2012-07-18_to_2012-10-18"
build_txs_graph_for_chunk(
    base_directory="Data/chunks/SatoshiDice.com-original/3_months",
    wallet_id=wallet_id,
    chunk_to_process=f"{period}.json",
    output_dir="Data/graphs",
)


# wallet_id = "01264a56d1f8fb9e"  # Example wallet ID, adjust as needed
# build_txs_graph_for_chunk(
#     base_directory=directory_chunks,
#     wallet_id=wallet_id,
#     chunk_to_process=chunk_to_process,
#     output_dir="Data/graphs"
# )
