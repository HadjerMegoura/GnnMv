def get_views_materialization_cost(views_set,excisting_views_list,new_condidates_views_list):

    views_with_materialization_cost = {}

    for view_label in views_set:

        if view_label in excisting_views_list.keys():

            #materialization cost = update cost

            mat_cost = excisting_views_list[view_label][0]['view_creation_cost']

            views_with_materialization_cost[view_label] = mat_cost

        else :

           mat_cost = new_condidates_views_list[view_label][0]['view_creation_cost']

           views_with_materialization_cost[view_label] = mat_cost




    return views_with_materialization_cost