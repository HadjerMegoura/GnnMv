import itertools
import json
import random
import numpy as np

from sklearn.model_selection import train_test_split
import torch
import Database
import networkx as nx
from Offline_Training_Model.GnnModel_Training.evaluate_model import compute_accuracy
from MVs_Maintenance.conflict_matrix import create_conflict_matrix
from MVs_Maintenance.construct_bipartite_graph import construct_bipartite_graph
from MVs_Maintenance.create_subgraph import create_subgraph
from MVs_Maintenance.get_new_views_set import get_new_views_set
from MVs_Maintenance.get_view_materialization_cost import get_views_materialization_cost
from MVs_Maintenance.get_views_spaces import get_views_with_spaces
from MVs_Maintenance.select_new_MVs import select_MVs_set
from MVs_Maintenance.views_benefit_estimation import views_benefit_estimation
from MVs_Maintenance.views_with_sql_script import get_all_views_with_sql_script
from Offline_Training_Model.GnnModel_Training.train_model import train_model
from Offline_Training_Model.GnnModel_Training.MV_Aggregator import MV_aggregator
from  Offline_Training_Model.dataset_preparation.create_workload_graph import create_workload_graph
from Offline_Training_Model.dataset_preparation.queries_with_possible_views import get_queries_with_possible_views
from Offline_Training_Model.dataset_preparation.nodes_features_encoding import featues_reshaping, nodes_features_encoding
from Offline_Training_Model.dataset_preparation.create_dataset import create_dataset
from Offline_Training_Model.dataset_preparation.query_with_corresponding_view import query_with_correponding_view
from Offline_Training_Model.GnnModel_Training.get_nodes_neighbors import get_node_with_neighbors
from Query_Rewrite.get_MVs_sets import get_MVs_sets_for_join_orders
from Offline_Training_Model.dataset_preparation.join_graph import Create_Graph
from Offline_Training_Model.dataset_preparation.parse_query import parse_query
from Offline_Training_Model.dataset_preparation.computing_cost import ComputeCost
from Offline_Training_Model.dataset_preparation.dataset_schema import Dataset_Schema, Dataset_Schema2
from Query_Rewrite.set_benefit_computing import get_sets_benefits
from Query_Rewrite.update_features_list import update_features_list
from Query_Rewrite.views_conflict import views_conflict
from MVs_Maintenance.create_new_workload_graph import create_new_workload_graph

#========================dataset preparation================================================================================
#take the workload as parameter then extract the dataset from it 
#create the workload graph and extract views samples



connexion = Database.connect()

Path_Workload = "D:\\PFE\\benchmarks\\RTOS_EXPLICIT_JOIN\\RTOS_EXPLICIT_JOIN"

#create_workload_graph(Path_Workload,connexion)


workload_graph = nx.read_gml("D:\\PFE_Final\\code\\graphs\\workload_graph0.gml")


with open("D:\\PFE_Final\\code\\stored_data\\views_samples.txt","r") as file :

    views_samples = eval(file.read())


with open("D:\\PFE_Final\\code\\stored_data\\queries_with_corresponding_views.txt","r") as file :

    queries_with_corresponding_views = eval(file.read())    


with open("D:\\PFE_Final\\code\\stored_data\\queries_with_estimated_cost.txt","r") as file :

    queries_with_estimated_cost = eval(file.read()) 



with open("D:\\PFE_Final\\code\\stored_data\\nodes_with_sql_script.txt","r") as file :

    nodes_with_sql_script = eval(file.read())    

#get the list_of_features_encoded of queries
list_of_queries = []

for node in workload_graph.nodes():

    if str(node)[0] == 'Q':

        list_of_queries.append(node)

       

"""
for query,view in queries_with_corresponding_views.items():

    print(view)

print("==================================")
"""
#queries_with_possible_views = get_queries_with_possible_views(workload_graph,views_samples.keys(),list_of_queries)

#print(queries_with_possible_views)


#========================train and save models==============================================================================
#========================use trained model in query rewrite and MVs maintenance=============================================
"""    




dict_of_features_encoded,list_of_features_encoded = nodes_features_encoding(nodes_with_sql_script,list_of_queries,workload_graph,connexion)

#Dataset =  create_dataset(queries_with_corresponding_views,views_samples,queries_with_estimated_cost,connexion,dict_of_features_encoded)

queries_with_possibles_views = get_queries_with_possible_views(workload_graph,views_samples.keys(),list_of_queries)

query_with_corres_view = query_with_correponding_view(queries_with_possibles_views,views_samples)






Dataset =  create_dataset(query_with_corres_view,views_samples,queries_with_estimated_cost,connexion,dict_of_features_encoded)


#save dataset in a file
 

    
with open("D:\\PFE_Final\\code\\Gnn_Mv\\dataset.txt","w") as file :

    file.write(str(Dataset))
    


"""

#read the dataset from file

with open("D:\\PFE_Final\\code\\Gnn_Mv\\dataset.txt","r") as file :

    dataset = eval(file.read())







dict_of_features_encoded,list_of_features_encoded = nodes_features_encoding(nodes_with_sql_script,list_of_queries,workload_graph,connexion)


nodes_with_neighbors = get_node_with_neighbors(workload_graph,3,dict_of_features_encoded)



# Set the desired proportions for train, test, and validation
train_ratio = 0.3  # 70% for training
test_ratio = 0.6  # 15% for testing
validation_ratio = 0.1  # 15% for validation

# Get the keys of the dataset
data_keys = list(dataset.keys())

# Shuffle the keys to ensure randomness
random.shuffle(data_keys)

# Calculate the split points
train_split = int(train_ratio * len(data_keys))
test_split = int((train_ratio + test_ratio) * len(data_keys))

# Split the keys into train, test, and validation sets
train_keys = data_keys[:train_split]
test_keys = data_keys[train_split:test_split]
validation_keys = data_keys[test_split:]

# Create the actual train, test, and validation dictionaries
train_set = {key: dataset[key] for key in train_keys}
test_set = {key: dataset[key] for key in test_keys}
validation_set = {key: dataset[key] for key in validation_keys}

# Now you have three separate datasets: train_set, test_set, and validation_set






input_dim = 256
hidden_dim = 256
output_dim = 1

trained_model = train_model(train_set,input_dim,hidden_dim,output_dim,dict_of_features_encoded,nodes_with_neighbors)


#save the trained model 
# Specify the file path where you want to save the model
model_path = "D:\\PFE_Final\\code\\Gnn_Mv\\gnn_model.pt"

# Save the model
torch.save(trained_model, model_path)





#reload Model for use=====================================================================

benefitEstimationmodel = torch.load("D:\\PFE_Final\\code\\Gnn_Mv\\gnn_model.pt")

benefitEstimationmodel.eval()

###model Evaluation
accaracy = compute_accuracy(benefitEstimationmodel,test_set,dict_of_features_encoded,nodes_with_neighbors)

print("accuracy",accaracy)

"""
with open("D:\\PFE\\benchmarks\\RTOS_EXPLICIT_JOIN\\RTOS_EXPLICIT_JOIN\\2a.sql","r") as file:

   new_query_script = file.read()



All_Join_Predicates, Selection_Predicates_By_Table , All_selection_Predicates, list_tables , Predicates_List = parse_query(new_query_script)



#enumerate join orders

join_orders =  list(itertools.permutations(list_tables))


join_orders_with_possible_MVs_sets = get_MVs_sets_for_join_orders('N',join_orders,views_samples.keys(),Predicates_List,
                                        workload_graph,All_Join_Predicates,All_selection_Predicates,connexion)

for key,value in join_orders_with_possible_MVs_sets.items():

    print(key,value)

print("=======================================")



possible_MVs_sets_with_benefit = get_sets_benefits('N',join_orders_with_possible_MVs_sets,benefitEstimationmodel,dict_of_features_encoded,connexion,nodes_with_sql_script)

for key,value in possible_MVs_sets_with_benefit.items():

    print(key,value)

#choose the best set
selected_MV_set =  max(possible_MVs_sets_with_benefit.items(), key=lambda item: item[1]['benefit'])

print('selected set',selected_MV_set)






"""


#=====================================MVs maintenance=====================================================================
new_arrived_workload_path = "D:\PFE_Final\\code\\benchmarks\\MVs_maintenance"
"""
#create the new workload for new arrived queries , and get new views condidates

new_workload_graph , new_views_condidates, new_nodes_with_sql_script = create_new_workload_graph(new_arrived_workload_path,connexion,'N')





#append the new graph to the fiest one

resulted_graph = nx.compose(workload_graph,new_workload_graph)

#get new queries list
new_queries = []

for node in resulted_graph.nodes():

    if str(node)[0] == 'N' or str(node)[0] == 'Q' :
        
        new_queries.append(node)

#get new set of MVs (excisting and new)

queries_with_views,new_views_keys = get_new_views_set(new_queries,views_samples.keys(),new_views_condidates.keys(),resulted_graph)



views_with_materialization_cost =   get_views_materialization_cost(new_views_keys,views_samples,new_views_condidates)




#create a subqueries to update embeddings
subgraph = create_subgraph(resulted_graph,new_workload_graph,3)




#encode new nodes features and update excisting embeddings
subgraph_updated_embeddings = update_features_list(dict_of_features_encoded,new_nodes_with_sql_script,subgraph,connexion)




new_nodes_with_neighbors = get_node_with_neighbors(subgraph,3,subgraph_updated_embeddings)

views_benefits = views_benefit_estimation(queries_with_views,new_nodes_with_neighbors,subgraph_updated_embeddings,benefitEstimationmodel)



#construct the bipartite graph
#bipartite_graph = construct_bipartite_graph(views_benefits)


#get the list of both new and old views with their 

all_views_with_sql_script = get_all_views_with_sql_script(new_nodes_with_sql_script,views_samples)



#create conflict matrix

conflict_matrix = create_conflict_matrix(queries_with_views,all_views_with_sql_script,new_views_keys)




#get views spaces

views_with_spaces = get_views_with_spaces(all_views_with_sql_script)


#select the new MVs set to materialize

space_budget = 10

MVs_set = select_MVs_set(conflict_matrix,list(all_views_with_sql_script.keys()),views_benefits,space_budget,views_with_spaces)

print(MVs_set)
"""