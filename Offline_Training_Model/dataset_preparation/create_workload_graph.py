from collections import OrderedDict
import random
import time
import networkx as nx
from Offline_Training_Model.dataset_preparation.computing_cost import ComputeCost
from Offline_Training_Model.dataset_preparation.dataset_schema import Dataset_Schema, Dataset_Schema2
from Offline_Training_Model.dataset_preparation.get_frequency import getFrequency
from Offline_Training_Model.dataset_preparation.join_graph import Create_Graph
from Offline_Training_Model.dataset_preparation.merge_plan import MergePlan
from Offline_Training_Model.dataset_preparation.query_estimated_cost import GetQueriesEstimatedCost
from Offline_Training_Model.dataset_preparation.selecting_mv_condidates import Get_Views_Info_From_MVPP
from Offline_Training_Model.dataset_preparation.workload import ParseWorkload


def create_workload_graph(Path_Workload,connexion):
   #creates the graph
   #stores any essentiel info in a file
    # # # PARSING==================================================================
    List_All_queries, List_All_queries_with_their_Tables, List_All_queries_with_their_Tables_and_their_Predicates, \
        All_Join_Predicates, All_selection_Predicates, Workload_Size, \
        List_All_queries_with_their_parsed_format ,All_queries_with_id_query ,Queries_with_id_and_filename= ParseWorkload(Path_Workload)


    # DBMS COST ESTIMATION================================================================
    Rewritten_List_All_queries_with_their_EstimatedCost, Rewritten_Total_Queries_Estimated_Cost = GetQueriesEstimatedCost(
        List_All_queries,
        connexion)
    
    with open("D:\\PFE_Final\\code\\stored_data\\queries_with_estimated_cost.txt","w") as file:
         
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

    Path_to_workload_graph = 'D:\\PFE_Final\\code\\graphs\\workload_graph' + str(i) + '.gml'
    nx.write_gml(workload_graph, Path_to_workload_graph)

    # workload_graph GRAPH VISUALISATION
    # nx.draw(workload_graph,with_labels=True)
    # plt.draw()
    # plt.show()

    MVPP_With_Selection_0_With_Cost, List_Nodes_With_SQL_Script = ComputeCost(
        workload_graph,
        All_Join_Predicates,
        All_selection_Predicates,

        Dataset_Schema,
        Dataset_Schema2,
        connexion)
    

    with open("D:\\PFE_Final\\code\\stored_data\\nodes_with_sql_script.txt","w") as file:
         
         file.write(str(List_Nodes_With_SQL_Script))

    #Calculating frequency
    frequency = getFrequency(workload_graph, Dic_Query_With_Query_Join_tree_graph)

    """
    with open("D:\\PFE_Final\\code\\stored_data\\queries_with_corresponding_views.txt","w") as file:
         
         file.write(str(queries_with_corresponding_views))
"""
    #Getting the views each with their cost
    views_with_cost = Get_Views_Info_From_MVPP(MVPP_With_Selection_0_With_Cost,
                                               frequency,
                                               ListQueries)
    
    

 
    for v in views_with_cost:
            views_with_cost[v].append({"Frequency":frequency[v] } )



    #Ordering the views on frequency
    views_with_cost = OrderedDict(sorted(views_with_cost.items(), key=lambda x: x[1][3]["Frequency"] , reverse=True))

      #Choose views to materialize
     #Choose views to materialize

    #print("views len",len(list(views_with_cost.keys())))
  

 
    #Getting the sql script of the views
    for v in views_with_cost:

        views_with_cost[v].append(List_Nodes_With_SQL_Script[v])

    views_samples = {}
    #the first technique (simple random)
    #views_samples_keys = random.sample(views_with_cost.keys(),10)

    #print("=================================================")

    #print(views_samples_keys)



    #the seconde technique (uniformaly choose)
    positive_samples = list(views_with_cost.keys())[:20]

    negative_samples = list(views_with_cost.keys())[100:110]

    views_samples_keys = list(set(positive_samples) | set(negative_samples))


    for key,value in views_with_cost.items():
         
         if key in views_samples_keys:
              
              views_samples[key] = value
  
    #store those views in a file 
    with open("D:\\PFE_Final\\code\\stored_data\\views_samples.txt","w") as file:
         
         file.write(str(views_samples))
