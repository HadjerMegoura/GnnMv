from  Offline_Training_Model.GnnModel_Training.get_nodes_neighbors import get_node_with_neighbors
from Query_Rewrite.update_features_list import update_features_list
from Offline_Training_Model.dataset_preparation.nodes_features_encoding import nodes_features_encoding
from Query_Rewrite.views_benefit_estimation import views_benefit_estimation
from Query_Rewrite.views_conflict import views_conflict


def get_sets_benefits(query,join_mv_sets,model,exicted_features_dict,connexion,list_of_nodes_with_sql_script):

    weighted_sets = {}

    for join,mv_set_info in join_mv_sets.items():
        
           
           
           final_set_without_conflicts = []
           
           mv_set = mv_set_info['mv_set']

           #print(mv_set)

           new_workload_graph = mv_set_info['new_workload_graph']

           #print(new_workload_graph)

           #get new nodes script
           new_nodes_scripts = mv_set_info['new_nodes_with_sql_script']

           #print(new_nodes_scripts)  

           updated_feats_dict = update_features_list(exicted_features_dict,new_nodes_scripts,new_workload_graph,connexion)

           nodes_with_neighbors = get_node_with_neighbors(new_workload_graph,3,updated_feats_dict)

           mv_set_weighted = views_benefit_estimation(mv_set,model,query,nodes_with_neighbors,updated_feats_dict)
           

           total_set_benefit = 0
           
           for view in mv_set:
               
            if view  not in final_set_without_conflicts: 

                for v in final_set_without_conflicts:

                    if views_conflict(list_of_nodes_with_sql_script[view],list_of_nodes_with_sql_script[v]) : 

                        break
                 
                                 
                view_benefit = mv_set_weighted[view]

                total_set_benefit = total_set_benefit + view_benefit

                final_set_without_conflicts.append(view)
           
           weighted_sets[join] = {'mv_set':mv_set,'benefit':total_set_benefit}   

    return weighted_sets