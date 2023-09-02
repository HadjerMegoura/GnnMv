def get_views_with_spaces(list_of_views):

    dict_views_with_space = {}

    for view,view_info in list_of_views.items():

        view_occuped_space = 1

        dict_views_with_space[view] = view_occuped_space

    return dict_views_with_space