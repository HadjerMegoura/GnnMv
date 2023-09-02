def views_benefit_estimation(queries_with_views,nodes_with_neighbors,new_nodes_features_dict,model):

    views_with_benefit = {}

    for query,views_list in queries_with_views.items():

        views_weighted = {}

        for view in views_list:

            view_benefit = model(query,view,3,nodes_with_neighbors,new_nodes_features_dict)

            views_weighted[view] = view_benefit

        views_with_benefit[query] = views_weighted



    



    return views_with_benefit