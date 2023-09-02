from Offline_Training_Model.dataset_preparation.parse_query import parse_query


def views_conflict(view1_script,view2_script):

    conflict = False

    #get view1 subqueries
    All_Join_Predicates1, Selection_Predicates_By_Table1 , All_selection_Predicates1, list_tables1 , Predicates_List1 = parse_query(view1_script)
   
    #get view2 subqueries
    All_Join_Predicates2, Selection_Predicates_By_Table2 , All_selection_Predicates2, list_tables2 , Predicates_List2 = parse_query(view2_script)
    
    for pred in Predicates_List2:

        if pred in Predicates_List1:

            conflict = True

            break
    
    return conflict