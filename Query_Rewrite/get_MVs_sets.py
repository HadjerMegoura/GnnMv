from matplotlib import pyplot as plt
from Offline_Training_Model.dataset_preparation.join_graph import Create_Graph
import networkx as nx

from Offline_Training_Model.dataset_preparation.computing_cost import ComputeCost
from Offline_Training_Model.dataset_preparation.dataset_schema import Dataset_Schema, Dataset_Schema2


def get_MVs_sets_for_join_orders(query,join_orders,list_of_MVs,query_tables_predicates,workload_graph,All_join_predicates,
                                 All_selection_predicates,connexion):

    MVs_sets = {}

    exicted_sets = []

    for join in join_orders:

        #print(join)

        mv_set = []

        #create the graph for the new query
        query_graph = Create_Graph(query,query_tables_predicates,join)

          #get new nodes scripts
        MVPP_With_Selection_0_With_Cost, List_Nodes_With_SQL_Script = ComputeCost(
           query_graph,
           All_join_predicates,
           All_selection_predicates,
           Dataset_Schema,
           Dataset_Schema2,
           connexion)
   
        #visualize_graph(query_graph)

        #append it to the workload graph
        new_workload_graph = nx.compose(workload_graph,query_graph)

        #get MVs set
        anscetors = list(nx.ancestors(new_workload_graph, query))

       

        #print(list_of_MVs)

        for anscetor in anscetors:

            if anscetor in list_of_MVs:

                mv_set.append(anscetor)

        if mv_set ==  [] or mv_set in exicted_sets : pass       

        else :
            
            MVs_sets[join] = {'mv_set' : mv_set, 'new_workload_graph':new_workload_graph,
                              'new_nodes_with_sql_script':List_Nodes_With_SQL_Script}

            exicted_sets.append(mv_set)

        #keep just diffrent sets

    return MVs_sets




def visualize_graph(new_graph):
    pos = nx.spring_layout(new_graph)
    nx.draw_networkx_nodes(new_graph, pos, node_size=500)
    nx.draw_networkx_edges(new_graph, pos)
    nx.draw_networkx_labels(new_graph, pos, font_size=20, font_family='sans-serif')
    plt.axis('off')
    plt.show()
