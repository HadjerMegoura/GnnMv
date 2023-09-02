from http.client import CONFLICT
import torch

from Query_Rewrite.views_conflict import views_conflict


def create_conflict_matrix(queries_with_views,views_with_sql_script,list_of_views_condidates):

    u = len(list(queries_with_views.keys()))
    v = len(list_of_views_condidates)
    conflict_mat = torch.zeros(v,v,u)

    for query,list_of_query_views in queries_with_views.items():

        query_index = list(queries_with_views.keys()).index(query)

        #print(query,query_index)

     

        for i in range(1,len(list_of_query_views)):

            first_view = list_of_query_views[0]

            first_view_index = list_of_views_condidates.index(first_view)


            v = list_of_query_views[i]

            #print(v)

            #print("===================================")

            v_index = list_of_views_condidates.index(v)


            if views_conflict(views_with_sql_script[first_view],views_with_sql_script[v]) :


                    conflict_mat[first_view_index,v_index,query_index] == 1

                    conflict_mat[v_index,first_view_index,query_index] == 1

            else :
                 
                    conflict_mat[first_view_index,v_index,query_index] == 0

                    conflict_mat[v_index,first_view_index,query_index] == 0
                 


                 

    return conflict_mat