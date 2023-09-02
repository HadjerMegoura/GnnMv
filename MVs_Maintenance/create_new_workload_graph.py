from collections import OrderedDict
import time
from Offline_Training_Model.dataset_preparation.join_graph import Create_Graph
from Offline_Training_Model.dataset_preparation.merge_plan import MergePlan
from Offline_Training_Model.dataset_preparation.query_estimated_cost import GetQueriesEstimatedCost
from Offline_Training_Model.dataset_preparation.workload import ParseWorkload
import networkx as nx
from Offline_Training_Model.dataset_preparation.computing_cost import ComputeCost

from Offline_Training_Model.dataset_preparation.dataset_schema import Dataset_Schema, Dataset_Schema2
from Offline_Training_Model.dataset_preparation.get_frequency import getFrequency
from Offline_Training_Model.dataset_preparation.selecting_mv_condidates import Get_Views_Info_From_MVPP


def create_new_workload_graph(Path_Workload,connexion,label):
   #creates the graph
   #stores any essentiel info in a file
    # # # PARSING==================================================================
    List_All_queries, List_All_queries_with_their_Tables, List_All_queries_with_their_Tables_and_their_Predicates, \
        All_Join_Predicates, All_selection_Predicates, Workload_Size, \
        List_All_queries_with_their_parsed_format ,All_queries_with_id_query ,Queries_with_id_and_filename= ParseWorkload(Path_Workload,label)


    # DBMS COST ESTIMATION================================================================
    Rewritten_List_All_queries_with_their_EstimatedCost, Rewritten_Total_Queries_Estimated_Cost = GetQueriesEstimatedCost(
        List_All_queries,
        connexion)
    
    with open("D:\\PFE_Final\\code\\stored_data\\MVs_mainteneance\\new_queries_with_estimated_cost.txt","w") as file:
         
         file.write(str(Rewritten_List_All_queries_with_their_EstimatedCost))


    Dic_Query_With_Query_Join_tree_graph = {}
    Dic_Query_With_Query_Join_tree_graph_Light = {}
    Dic_Query_by_Oreder_In_The_Workload = {}

    startime_generate_query_trees = time.time()
    i = 0

    ListQueries = list(List_All_queries_with_their_Tables.keys())
    for query in List_All_queries_with_their_Tables:
        Dico_query_tables_and_predicates = dict(List_All_queries_with_their_Tables_and_their_Predicates[query])
      #  Dico_query_tables_and_selectAttributes = dict(List_All_queries_with_their_Select_Attributes[query])
        query_join_order = List_All_queries_with_their_Tables[query]

        Query_Join_tree_graph = Create_Graph(query,
                                                        Dico_query_tables_and_predicates,
                                                        query_join_order)
        #Path_to_ = '/Users/ilyes/Downloads/gml/' + str(i) + '.gml'
        #nx.write_gml(Query_Join_tree_graph, Path_to_MVPP)

        Dic_Query_With_Query_Join_tree_graph[query] = Query_Join_tree_graph
        Dic_Query_by_Oreder_In_The_Workload[query] = i
        i += 1
    endtime_generate_query_trees = time.time() - startime_generate_query_trees

    # GRAPH VISUALISATION
    # nx.draw(Query_Join_tree_graph,with_labels=True)
    # plt.draw()
    # plt.show()

    # THE MERGING PHASE
    Dic_Id_With_MVPP_graph = {}
    # the folowwing loop performs the rotation of query graph for merging
    t1 = OrderedDict(sorted(Dic_Query_by_Oreder_In_The_Workload.items(), key=lambda x: x[1]))
    lst = list(Dic_Query_With_Query_Join_tree_graph.keys())
    i = 0

    Queries_Order_For_Merging = list(t1.keys())
    workload_graph = MergePlan(Queries_Order_For_Merging, Dic_Query_With_Query_Join_tree_graph)

    #print(workload_graph)

    Path_to_workload_graph = 'D:\\PFE_Final\\code\\graphs\\MVs_mainteneance\\new_workload_graph' + str(i) + '.gml'
    nx.write_gml(workload_graph, Path_to_workload_graph)

    MVPP_With_Selection_0_With_Cost, List_Nodes_With_SQL_Script = ComputeCost(
        workload_graph,
        All_Join_Predicates,
        All_selection_Predicates,

        Dataset_Schema,
        Dataset_Schema2,
        connexion)
    
    frequency = getFrequency(workload_graph, Dic_Query_With_Query_Join_tree_graph)
    
    views_with_cost = Get_Views_Info_From_MVPP(MVPP_With_Selection_0_With_Cost,
                                               frequency,
                                               ListQueries)

    return workload_graph, views_with_cost,List_Nodes_With_SQL_Script