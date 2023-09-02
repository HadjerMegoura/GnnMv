def compute_benefit(query_cost,view_creation_cost,view_scan_cost):

    benefit = query_cost - view_creation_cost + view_scan_cost

    return benefit




def get_view_row_size(view_script,connection):
    
    # Create a cursor object
   cursor = connection.cursor()

# Execute the query
   cursor.execute(view_script)
# Fetch a single row from the result set
   row = cursor.fetchone()

   if row:
    # Calculate the storage size for each column in the row
      column_sizes = [
        len(str(value).encode())
        for value in row
    ]

    # Calculate the total storage size of the row
      row_size = sum(column_sizes)

    
   
   else:
     row_size = 0

     

# Close the cursor and connection
   cursor.close()


   return row_size




def create_dataset(query_with_corresponding_view,views_with_cost,queries_with_cost,connection,dict_features_encoded):

    dataset = {}

    consulted_views = {}

    for query,view in query_with_corresponding_view.items():

      if view == [] : pass

      else :
        
           view = view[0]
        
           print(query)

           query_cost = queries_with_cost[query]

           print(query_cost)

         
           print(view)

        #print(views_with_cost)

           view_creation_cost = views_with_cost[view][0]['view_creation_cost']
       

           #print(view_creation_cost)
 
           view_script = views_with_cost[view][4]

           #print(view_script)

           
           if view in consulted_views.keys():
              
              view_scan_cost = consulted_views[view]

           else :

             view_scan_cost = (views_with_cost[view][2]['view_size_in_rows'] * 
                          get_view_row_size(view_script,connection)) / views_with_cost[view][1]['view_size_in_pages']

           print(view_scan_cost)

           benefit = compute_benefit(query_cost,view_creation_cost,view_scan_cost)

           print('benefit',benefit)

           print("=================================")

           consulted_views[view] = view_scan_cost



       

           dataset[query] = {'query features' : str(dict_features_encoded[query]).replace("tensor","") , 'view':view ,'view features' :
                           str(dict_features_encoded[view]).replace("tensor","") ,'benefit' : benefit} 


      




    return dataset
        


