import numpy as np
import torch
from  Offline_Training_Model.dataset_preparation.features_encoding import node_encoding
from Offline_Training_Model.dataset_preparation.nodes_features_encoding import featues_reshaping

#change feats dict to embeddings
def update_features_list(exciting_feats_dict,new_nodes_scripts,new_workload_graph,connexion):

    updated_dict_features = {}

    for node in new_workload_graph.nodes():

        if node in exciting_feats_dict.keys():

            updated_dict_features[node] = exciting_feats_dict[node]

        elif node in new_nodes_scripts.keys():

            updated_dict_features[node] = node_encoding(new_nodes_scripts[node],connexion)

        else :

            updated_dict_features[node] = torch.zeros(5)

            #print("zero",node)

    return featues_reshaping(updated_dict_features)