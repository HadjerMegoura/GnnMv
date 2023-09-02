import torch
from Offline_Training_Model.GnnModel_Training.GnnModel import GNN_Model
from Offline_Training_Model.GnnModel_Training.GnnModel import smooth_l1_loss


def train_model(dataset,input_dim,hidden_dim,output_dim,dict_feats,nodes_with_neighbors):

    gnn_model = GNN_Model(input_dim,hidden_dim,output_dim)

    optimizer = torch.optim.Adam(gnn_model.parameters(), lr=0.001)

    num_epochs = 500

    k = 3

    for i in range(num_epochs):
       
       epoch_total_loss = 0

       avg_loss = 0

       for query,mv in dataset.items():
           
           target_benefit = mv['benefit']

           print('target',target_benefit)

           predicted_benefit = gnn_model(query, mv['view'] , k , nodes_with_neighbors,dict_feats)

           print('predicted',predicted_benefit)

           optimizer.zero_grad()

           loss = smooth_l1_loss(predicted_benefit,target_benefit)

           epoch_total_loss = epoch_total_loss + loss

           loss.backward(retain_graph=True)

           optimizer.step()

       avg_loss = epoch_total_loss / num_epochs

       print(i , avg_loss)

       print("===========================================================")

    return gnn_model