import networkx as nx

class AssetGraph:

    def __init__(self):
        graph = nx.DiGraph()
        assets = {}

        nx.is_directed_acyclic_graph(graph)

        downstream = [n for n in nx.traversal.bfs_tree(g, 2) if n != 2]

        graph.remove_nodes_from(iterable_nodes_e.g._bfs_tree)
        
        upstream = [n for n in nx.traversal.bfs_tree(g, 2, reverse=True) if n != 2]

        graph.add_edge(source_node, dest_node) #also adds node
        graph.add_edges_from(edges) #also adds nodes

        graph.nodes[asset_key] 

    def add_asset(self, asset):
        graph.add_node(asset.key)
        assets[asset.key] = asset
       
