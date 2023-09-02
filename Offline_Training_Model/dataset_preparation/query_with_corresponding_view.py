def query_with_correponding_view(queries_with_possible_views,views_with_cost):

    queries_with_corresponding_view = {}

    for query , views in queries_with_possible_views.items():

        if views == [] or len(views) == 1 :
            
            queries_with_corresponding_view[query] = views

        else :

            views_with_frequencies = {}

            for view in views:

                views_with_frequencies[view] = views_with_cost[view][3]['Frequency']


            chosen_view = max(views_with_frequencies, key=views_with_frequencies.get)

          


            queries_with_corresponding_view[query] = [chosen_view]
           


    return queries_with_corresponding_view