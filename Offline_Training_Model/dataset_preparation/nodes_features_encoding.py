import torch
from Offline_Training_Model.dataset_preparation.features_encoding import node_encoding


def nodes_features_encoding(list_nodes_with_sql_script,list_of_queries_with_scripts,workload_graph,connection):

    dict_nodes_features = {}
    list_nodes_features = []

    for node in workload_graph.nodes():

        if node in list_nodes_with_sql_script:

           features =  node_encoding(list_nodes_with_sql_script[node],connection)

           dict_nodes_features[node] = features

           list_nodes_features.append(features)

        elif node in list_of_queries_with_scripts:

           features =  torch.zeros(5)

           dict_nodes_features[node] = features

           list_nodes_features.append(features)

        else :

           features =  torch.zeros(5)

           dict_nodes_features[node] = features

           list_nodes_features.append(features)


    return featues_reshaping(dict_nodes_features),list_nodes_features


def featues_reshaping(dict_feats):

   reshaped_dict = {}

   max_item = max(dict_feats.items(), key=lambda item: item[1].shape)

   max_dim = max_item[1].shape

   for key ,value in dict_feats.items():

      resized_tensor = torch.nn.functional.interpolate(value.unsqueeze(0).unsqueeze(0), size=max_dim, mode="nearest").squeeze(0).squeeze(0)

      #print(resized_tensor.shape)

      reshaped_dict[key] = resized_tensor

   return reshaped_dict