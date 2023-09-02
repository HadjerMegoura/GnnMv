def views_benefit_estimation(mv_set,model,query,nodes_with_neighbors,updated_feats_dict):

    mv_set_weighted = {}

    for view in mv_set:

        benefit =  model(query,view,3,nodes_with_neighbors,updated_feats_dict)

        mv_set_weighted[view] = benefit


    sorted_dict = dict(sorted(mv_set_weighted.items(), key=lambda item: item[1], reverse=True))   

    return mv_set_weighted