from moz_sql_parser import parse
from Offline_Training_Model.dataset_preparation.dataset_schema import Dataset_Schema
from Offline_Training_Model.dataset_preparation.extract_predicats import ExtractPredicates
from Offline_Training_Model.dataset_preparation.group_predicates import GroupPredicates




def parse_query(query_script):

        
  
        All_Join_Predicates = []
        All_selection_Predicates = {}
   
        index_predicate = 1


        Parsed_Query = parse(query_script)
        
        # print(Parsed_Query )
        # =======================================================
        # data = json.loads(Parsed_Query)
        tables = []
        predicates = {}
        predicates["and"] = []

        def traverse_json(data):
            if isinstance(data, dict):
                for key, value in data.items():

                    if (key.lower() == "from"):
                        tables.append(value[0])
                    if ((key.lower() == "inner join" ) or (key.lower() == "join" ) ):
                        tables.append(value)

                    if (key.lower() == "and"):
                        if (len(predicates['and']) == 0):
                            predicates['and'] = value
                        else:
                            for value in value:
                                predicates['and'].append(value)

                    traverse_json(value)
            elif isinstance(data, list):
                for item in data:
                    traverse_json(item)

        traverse_json(Parsed_Query)

        # print("tables EXPLICIT:",tables)
        # print("predicates EXPLICIT:",predicates)

        # =======================================================
        # Getting the list of tables accesed by the query
        # Tables_list = Parsed_Query['from']

        # Extracting the list of all predicats from the query
        Predicates = []
        # Predicates_Dictionary = Parsed_Query['where']
        Predicates_List = ExtractPredicates(predicates, Predicates)

        All_Join_Predicates, Selection_Predicates_By_Table, All_selection_Predicates, index_predicate = \
            GroupPredicates(Predicates_List, tables, All_Join_Predicates, All_selection_Predicates,
                            index_predicate)
        
        list_tables = []

        dataset_shema = Dataset_Schema()

        for table in tables:

            list_tables.append(dataset_shema[table["name"]])
            


        return All_Join_Predicates, Selection_Predicates_By_Table , All_selection_Predicates, list_tables , Predicates_List




