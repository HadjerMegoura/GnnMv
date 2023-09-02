import os
import re
import numpy as np
import psycopg2
import json

import torch

workload_path = 'C:\\Users\\hp\\Desktop\\PFE\\benchmarks\\join-order-benchmark'

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="imdbload",
    user="postgres",
    password="hadjer"
)



def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False




def Get_Workload_Nodes_List(workload_path,conn):
   #conn = psycopg2.connect(database="imdbload", user="postgres", password="hadjer", host="localhost", port="5432")
   cur = conn.cursor()
   j=0
   #workload_path = 'C:\\Users\\hp\\Desktop\\PFE\\Code\\join-order-benchmark'
   workload_nodes_list=[]
   for filename in os.listdir(workload_path):
     while j<4:
       if os.path.isfile(os.path.join(workload_path, filename)):
          with open(os.path.join(workload_path, filename), 'r') as f:
             file_contents = f.read()
             query=file_contents
             query_tree = get_query_tree(query,conn)
             query_tree_converted = convert_to_tree(query_tree)
             query_nodes_list = []
             serialized_nodes(query_tree_converted,query_nodes_list)
             for node in query_nodes_list:
               workload_nodes_list.append(node)
             j+=1
            # print(j)
   cur.close()
   
   return workload_nodes_list



#get all exicting operation types in the workload
def Get_Operations_Dict(parsed_query_trees_dict):

   i=0
   operations_dict={}

   for item in parsed_query_trees_dict:
      
      operation = item['Node Type']

      if operation not in operations_dict.keys(): 

         operations_dict[operation] = i
         i=i+1
         
      else : pass
    
   return operations_dict




#get a dict of all database columns
def all_culomns(connection):
  columns_dict={}
  
    # Create a cursor to execute SQL queries
  cursor = connection.cursor()

    
  cursor.execute("""
         select DISTINCT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
    """)


  rows = cursor.fetchall()

    
  i=0
  for row in rows:
        col=str(row)[2:-3]
        columns_dict[col] = i
        i=i+1
        
  cursor.close()

  return columns_dict


   
  




#get the query tree from postgres optimizer
def get_query_tree(sql_query,connection):

    cur = connection.cursor()


    cur.execute(" EXPLAIN (FORMAT JSON) " + sql_query)


    query_tree = cur.fetchone()[0][0]['Plan']

    return query_tree



#convert the tree to a tree structure for ease of access
def convert_to_tree(query_plan):
    node = {}

    for key, value in query_plan.items():
        if key == 'Plans':
            node[key] = [convert_to_tree(plan) for plan in value]
        else:
            node[key] = value

    return node





#get nodes of a query erialized
def serialized_nodes(node, serialized_list):
    if "Plans" in node:
        plans = node.pop("Plans")
        for plan in plans:
            serialized_nodes(plan, serialized_list)
    serialized_list.append(dict(node))






all_workload_nodes_list = Get_Workload_Nodes_List(workload_path,conn)

def operator_encoding(query_tree):
    
    operations_dict = Get_Operations_Dict(all_workload_nodes_list)

    operator_vector_size=len(operations_dict)
    
    operator_vector=torch.zeros(operator_vector_size)

    index_info_vector = torch.zeros(1)
    

    operator_type=query_tree['Node Type']

    for key,value in operations_dict.items():
       
       if operator_type==key: operator_vector[value] = 1
   
   
    edge_info_vector=torch.zeros(2)

    if 'Parallel Aware' in query_tree.keys():
       
       if query_tree['Parallel Aware'] ==" True" : edge_info_vector[1]=1

       else :  edge_info_vector[1]=0

    else : pass
    
    if 'Parent Relationship' in query_tree.keys(): edge_info_vector[0]=1

    else : edge_info_vector[0]=0
   
    
   #get index info
    if 'Index Name' in query_tree.keys():
        
        index_info_vector[0] = 1
       
    else : index_info_vector[0] = 0
    
    
    operator_encoding=torch.cat((index_info_vector,edge_info_vector,operator_vector))
    
    return operator_encoding




#meta data encoding
def meta_encoding(query_tree):
    
    cost_info_vector=torch.zeros(4)

   
   
   #get cost info
    cost_info_dict={'Startup Cost':0,'Total Cost':1,'Plan Rows':2,'Plan Width':3}
    
    for item in cost_info_dict:
       
       cost_info_vector[cost_info_dict[item]] = float(query_tree[item])
    



    return cost_info_vector



#predicate encoding
def encode_atomic_predicate(atomic_predicate,connection):

   columns_dict=all_culomns(connection)
   operators_dict= {
    '=': 0,
    '!=': 1,
    '>': 2,
    '>=': 3,
    '<': 4,
    '<=': 5,
    'LIKE': 6,
    'IN': 7,
    'NOT IN': 8,
    'NOT BETWEEN': 9,
    'BETWEEN': 10,
    'IS NULL': 11,
    'IS NOT NULL': 12,
    'NOT': 13,
    'EXISTS': 14
}
   
   operands_list=[]
   operator=''
   operand=''
   operator_vector=torch.zeros(len(operators_dict)).reshape(len(operators_dict),1)

   operand_vector=torch.zeros(1).reshape(1,1)

   column_vector=torch.zeros(len(columns_dict)).reshape(len(columns_dict),1)

   #get the operator vect encoding
   for key,value in operators_dict.items():

      if key in atomic_predicate:

         operator=key
         operator_vector[value]=1
         
         break

   operator_vector=operator_vector.reshape(len(operator_vector),1)

   #get operators
   left_operand=atomic_predicate.split(operator)[0].rstrip()

   operands_list.append(left_operand)

   index= atomic_predicate.index(operator)+len(operator)

   right_operand=atomic_predicate[index:].lstrip()
   
   operands_list.append(right_operand)
   
   
   
   for item in operands_list:
      is_column=False

      for key,value in columns_dict.items():

         if str(key) == item:
            column = str(key)
            column_vector[value]=1
            is_column=True
            break
      if is_column == False :
          
          operand = item
         
          if is_float(operand) : operand_vector[0] = operand

   atomic_predicate_encoded=torch.cat((operator_vector,column_vector,operand_vector))
   
   return atomic_predicate_encoded




#get node predicates
def get_predicate(node):
   prop_list = ["Filter","Index Cond","Index Filter","Recheck Cond","Heap Cond","Join Filter","Hash Cond","Hash Filter"]
   for key,value in node.items():
      if key in prop_list:
         return value



def node_encoding(query,connection):
    
    query_encoded = None

    #get the query plan tree
    query_tree = get_query_tree(query,connection)

    #convert it into a tree
    query_tree_converted = convert_to_tree(query_tree)

    #get serialized nodes
    nodes_list = []

    serialized_nodes(query_tree_converted,nodes_list)
    
    for node in nodes_list : 

        meta = meta_encoding(node)

        operator = operator_encoding(node)
       
        node_encoded = torch.cat((operator,meta))
      
       
        if query_encoded is None :
           
           query_encoded = node_encoded

        else : 
           
           query_encoded = torch.cat((query_encoded,node_encoded))
        

    return query_encoded
    




"""
def encode_graph_nodes(workload_graph,dist_of_queries_encoded,list_nodes_with_sql_script,connection):
   
   nodes_queries_list = []
   nodes_views_list = []
   nodes_selection_list = []
   nodes_tables_list = []
   sel_join_nodes = []


   for node in workload_graph.nodes():
      
      if str(node).startswith('Q') :
         
         nodes_queries_list.append(node)
         node_encoded = dist_of_queries_encoded[str(node)]
         workload_graph.nodes[node]['encoding'] = node_encoded
        
      elif is_float(str(node)) :
         nodes_tables_list.append(node) 
         node_encoded = 0
         workload_graph.nodes[node]['encoding'] = node_encoded

      elif re.match(r'^[s\d-]+$', str(node)) and node not in nodes_tables_list:
         nodes_selection_list.append(node)
         node_encoded = 0
         workload_graph.nodes[node]['encoding'] = node_encoded
      
      
        
      elif  str(node) in list_nodes_with_sql_script.keys():
             nodes_views_list.append(node)
             node_encoded = node_encoding(list_nodes_with_sql_script[str(node)],connection)
             workload_graph.nodes[node]['encoding'] = node_encoded
             print(len(node_encoded))
             
           
      else :
            sel_join_nodes.append(node)
            node_encoded = 0
            workload_graph.nodes[node]['encoding'] = node_encoded
        
   return nodes_views_list
   """