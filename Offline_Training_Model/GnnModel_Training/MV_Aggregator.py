import torch


def MV_aggregator(node,neighbors_list,dict_of_feats,self):

    #print(neighbors_list)

    node_feats = dict_of_feats[node]

    list_of_aggregated_neighbors_feats = []

    if neighbors_list == [] :
           
           #print("list viide")

           node_embeddings = torch.cat((node_feats,torch.zeros_like(node_feats)),dim=0)

           node_embeddings = self.relu(self.Wmv(node_embeddings))

    else :

        for neighbor in neighbors_list:

           for neighb,role in neighbor.items(): 

               neighbor_feats = dict_of_feats[neighb]

               if role == 'father' :

                   neighb_feats = self.relu(self.Wf(neighbor_feats))

                   list_of_aggregated_neighbors_feats.append(neighb_feats)

               elif role == 'index' :

                  neighb_feats = self.relu(self.Wi(neighbor_feats))

                  list_of_aggregated_neighbors_feats.append(neighb_feats)

               elif role == 'otherwise' :

                  neighb_feats = self.relu(self.Wo(neighbor_feats))

                  list_of_aggregated_neighbors_feats.append(neighb_feats)

    #MEAN of  the features of neighbors

        mean_feats = torch.mean(torch.stack(list_of_aggregated_neighbors_feats,dim=0),dim = 0 )


        concat_feats = torch.cat((node_feats,mean_feats),dim = 0)

        node_embeddings = self.relu(self.Wmv(concat_feats))

    return node_embeddings    
                




