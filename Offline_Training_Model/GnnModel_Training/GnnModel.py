import torch
import torch.nn as nn

from Offline_Training_Model.GnnModel_Training.MV_Aggregator import MV_aggregator


class GNN_Model(nn.Module):

    def __init__(self, input_dim, hidden_dim,output_dim):
        super(GNN_Model, self).__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim
        self.W1 = nn.Linear(2 * input_dim, hidden_dim)
        self.W2 = nn.Linear(hidden_dim, 1)
        self.relu = nn.ReLU()


    def forward(self, query, mv, k, nodes_with_their_neighbors,feats_dict):

        input_d = len((feats_dict['Q0']))

        for i in range(k):
            
            
            #initialize matrices
            self.Wf = nn.Linear(input_d,input_d)
            self.Wi = nn.Linear(input_d,input_d)
            self.Wo = nn.Linear(input_d,input_d)
            self.Wmv = nn.Linear(input_d * 2, self.input_dim)

           

            query_neighbors_list = nodes_with_their_neighbors[query][i][i]

            query_emb = MV_aggregator(query,query_neighbors_list,feats_dict,self)

            mv_emb = MV_aggregator(mv,query_neighbors_list,feats_dict,self)

            #input_d= len(mv_emb)
      
        concat_emb = torch.cat((query_emb, mv_emb), dim=0)

        hidden = self.relu(self.W1(concat_emb))

        benefit = self.W2(hidden)

        return benefit



def smooth_l1_loss(prediction, target):

    diff = torch.abs(prediction - target)
    loss = torch.where(diff < 1, 0.5 * diff**2, diff - 0.5)
    return loss.mean()