import networkx as nx

def construct_bipartite_graph(dict_benefit_view_to_query):

    list_of_queries = dict_benefit_view_to_query.keys()

    #print(list_of_queries)

    list_of_views = []

    for value in dict_benefit_view_to_query.values():

         list_of_views.extend(value.keys())

    #print(list_of_views)

    #create the grph
    bipartite_graph = nx.Graph()

    #add nodes th the graph
    bipartite_graph.add_nodes_from(list_of_queries,bipartite = 0)

    bipartite_graph.add_nodes_from(list_of_views,bipartite = 1)

    #add edges to the graph
    for query,list_of_views in dict_benefit_view_to_query.items():

       # print(query)

        for view in list_of_views:
            
            #print(view)

            benefit = list_of_views[view]

            #print(benefit)

            bipartite_graph.add_edge(query,view,weight= benefit)


    return bipartite_graph