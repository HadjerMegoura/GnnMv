def get_all_views_with_sql_script(new_nodes_with_sql_script,views_samples):

    all_views_with_sql_script = {}

    keys = list((set(new_nodes_with_sql_script.keys())).union(set(views_samples.keys())))

    

    for key in keys:

        if key in new_nodes_with_sql_script.keys():

            all_views_with_sql_script[key] = new_nodes_with_sql_script[key]

        else :

            all_views_with_sql_script[key] = views_samples[key][4]

    return all_views_with_sql_script