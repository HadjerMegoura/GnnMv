import networkx as nx


def get_node_with_neighbors(graph,max_depth,dict_nodes_feats):
        
        nodes_with_their_neighbors = {}

        for node in graph.nodes():
                
                nodes_with_their_neighbors[node] = []
        
        i = 0

        while i < max_depth:
                
                for node in graph.nodes():
                        
                        #print(node)

                        node_neighbors_subgrph = nx.ego_graph(graph, node, radius=i,undirected=True)


                        neighbors_set = set(node_neighbors_subgrph.nodes()) - {node}


                        list_of_predecessors = list(graph.predecessors(node))

                        list_of_seccessors = list(graph.successors(node))

                        

                        i_neighbors_list = []
                        

                        for neighbor in neighbors_set:
                            
                            neighbor_feats = dict_nodes_feats[neighbor]

                            #test if the neighbor is a father (in the list of predeccessors)
                            if neighbor in list_of_predecessors:
                                   
                                   i_neighbors_list.append({neighbor:'father'})

                                   #print("father")

                                   #nodes_with_their_neighbors[node].append({i,{neighbor:'father'}})

                            #test if node is accessed by index
                           

                            elif  neighbor in list_of_seccessors:  
                                   
                                   i_neighbors_list.append({neighbor:'index'})
                                   
                                   #nodes_with_their_neighbors[node].append({i,{neighbor:'index'}})

                            else :
                                   
                                   i_neighbors_list.append({neighbor:'otherwise'})
                                   
                                   #nodes_with_their_neighbors[node].append({i,{neighbor:'otherwise'}})

                                         
                        nodes_with_their_neighbors[node].append({i:i_neighbors_list})
                        

                i = i + 1

        return nodes_with_their_neighbors 


        
