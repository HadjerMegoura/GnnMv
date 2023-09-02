import networkx as nx

def create_subgraph(resulted_workload_graph,new_workload_graph,d_hops):

    subgraph = new_workload_graph.copy()

    for node in new_workload_graph.nodes():

        if node in resulted_workload_graph.nodes():

            node_d_neighbors = nx.ego_graph(resulted_workload_graph, node, radius=d_hops,undirected=True)

            subgraph = nx.compose(subgraph,node_d_neighbors)

    return subgraph

