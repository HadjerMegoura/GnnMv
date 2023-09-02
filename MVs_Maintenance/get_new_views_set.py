import networkx as nx

def get_new_views_set(new_queries,excisting_views,new_views_set,new_workload_graph):

    views_set = {}

    views_keys = []

    for query in new_queries:

        views_list = []

        query_anscestors = list(nx.ancestors(new_workload_graph,query))

        for ansc in query_anscestors:

            if ansc in excisting_views or ansc in new_views_set:

                views_list.append(ansc)

                views_keys.append(ansc)

        views_set[query] = views_list

                

    return views_set,views_keys