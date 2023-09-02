def select_MVs_set(conflict_matrix,list_all_views,dict_views_benefit,space_budget,dict_view_space):
   
   #initialize selected view set
   selected_views_set = []

   #initialize view's benefits considiring other selected MVs
   views_benefits = {}

   for query,views_benefits_list in dict_views_benefit.items():
       
        for view,benefit in views_benefits_list.items():
           
           if view in views_benefits.keys():
           
                views_benefits[view] = views_benefits_list[view] + benefit

           else :
              
                views_benefits[view] =  benefit

    
    #sort views by benefit
   views_sorted_by_benefit = {}

   # Sort the dictionary by its values in ascending order
   sorted_dict_items = sorted(views_benefits.items(), key=lambda item: item[1],reverse=True)

# If you want to convert the sorted list back to a dictionary:
   views_sorted_by_benefit = {k: v for k, v in sorted_dict_items}

   
    
    #select and add views
   space_count = 0

   for view in views_sorted_by_benefit:
        

        if space_count < space_budget :
       
            selected_views_set.append(view)

            space_count = space_count + dict_view_space[view]

             
            print(view)

            print(views_sorted_by_benefit.keys())

            print(space_count)

            print("============================================")

       #update views benefits
       
            for query,views_benefits in dict_views_benefit.items():
          
                query_index = list(dict_views_benefit.keys()).index(query)


                for view in views_benefits:
             
                    view_index = list_all_views.index(view)
          
                    for selected_view in selected_views_set:
                  
                        selected_view_index = list_all_views.index(selected_view)
             
                        if conflict_matrix[view_index,selected_view_index,query_index] == 1 :
                     
                             views_sorted_by_benefit[view] = 0
       
        else :

            break
        
   return selected_views_set