from matplotlib import pyplot as plt
import networkx as nx

# Create a graph
G = nx.DiGraph()
G.add_edges_from([(1, 2), (3, 1), (1, 4), (2, 3), (3, 4), (4, 5), (4, 6)])

# Get the ego graph centered at node 1 with radius 1
ego_graph = nx.ego_graph(G, 1, radius=1,undirected=True)

# Print the nodes and edges in the ego graph
print("Nodes in ego graph:", ego_graph.nodes())
print("Edges in ego graph:", ego_graph.edges())

# Draw the graph
#nx.draw(G, with_labels=True, node_color='lightblue', font_weight='bold')

# Show the plot
#plt.show()
