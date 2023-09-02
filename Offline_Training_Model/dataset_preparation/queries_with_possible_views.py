import networkx as nx


def get_ancestors(graph,node):

    ancestors = set(graph.predecessors(node))

    for predecessor in ancestors.copy():

        ancestors |= get_ancestors(graph, predecessor)

    return ancestors




def get_queries_with_possible_views(workload_graph,list_of_views,list_of_queies):

    dict_queries_with_possible_views = {}

    for node in workload_graph.nodes():

        if node in list_of_queies:

            list_possible_views = []

            node_descendents = list(get_ancestors(workload_graph,node))

            #print(node,node_descendents)

            for view in list_of_views:

                if view in node_descendents:


                    list_possible_views.append(view)

            dict_queries_with_possible_views[node] = list_possible_views


    return dict_queries_with_possible_views



