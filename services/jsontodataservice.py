import json
from flatten_json import flatten
from jsonpath import jsonpath as jp


def parse_json(json_data):
    """
    This function will parse the given json body in the API call
    It will return a blueprint of the data which will aid in creating and loading tables
    :param json_data: json data passed in the API call
    :return: a dictionary consisting of the following:-
             1. data type of the columns against each table
             2. list of table names
             3. columns available in each table
             4. values of each column in tha API call for each table
             5. relationship of tables with each other

    """
    # with open(json_file, encoding='utf-8', errors='ignore') as json_data:
    #     data = json.load(json_data, strict=False)
    data = json.loads(json_data)
    flattened_data = flatten(data, separator='.')
    data_type_data = {}
    tables_list = set()
    table_column ={}
    table_values = {}
    parent_child_tables = {}
    for key, value in flattened_data.items():
        if '.' in key:
            # Finding the list of different tables
            if not key.split('.')[-2].isnumeric():
                tables = key.split('.')[-2]
                is_list = False
            else:
                tables = key.split('.')[-3]
                is_list = True
            tables_list.add(tables)
            # Enlisting columns for each table
            if not table_column.get(tables):
                table_column[tables] = set()
            table_column[tables].add(key.split('.')[-1])
            # Enlisting the values for each column and their data types against each table
            if not table_values.get(tables):
                if not is_list:
                    table_values[tables] = {}
                    data_type_data[tables] = {}
                else:
                    table_values[tables] = [{}]*len(jp(data,'$.{key}'.format(key='.'.join(key.split('.')[0:-3]))))
                    data_type_data[tables] =  [{}]*len(jp(data,'$.{key}'.format(key='.'.join(key.split('.')[0:-3]))))
            if not is_list:
                table_values[tables].update({key.split('.')[-1]: value})
                data_type_data[tables].update({key.split('.')[-1]: type(value)})
            else:
                table_values[tables][int(key.split('.')[-2])].update({key.split('.')[-1]: value})
                data_type_data[tables][int(key.split('.')[-2])].update({key.split('.')[-1]: type(value)})
            # Relation tables have with each other. Not if a table has set() against it, it means its a root table
            if not parent_child_tables.get(tables):
                parent_child_tables[tables] = set()
            try:
                if not is_list:
                    parent_child_tables[tables].add(key.split('.')[-3])
                else:
                    parent_child_tables[tables].add(key.split('.')[-4])
            except:
                pass
    return {
            'data_type_data': data_type_data,
            'tables_list': tables_list,
            'table_column': table_column,
            'table_values': table_values,
            'parent_child_tables': parent_child_tables
            }


def create_load_automated_tables(DB, json_data):
    """
    This function will be used to create automated tables and insert values into them.
    This will ensure, new tables will be created if the table does not exist with newer payloads
    :param DB: An object of the DB tools class
    :param json_data: json data passed in the API call
    """
    # Parse the json and get a blueprint
    meta_parse_json = parse_json(json_data)
    # Iterate over the table list from the meta data received and perform creation and insertion funcions
    for table in meta_parse_json['tables_list']:
        table_name = table
        table_columns = ''
        column_names = ''
        # Build create table queries for tables that have dictionary values in the provided json
        if isinstance(meta_parse_json['data_type_data'][table_name], dict):
            for key, value in meta_parse_json['data_type_data'][table_name].items():
                new_column = '"' + key + '"' + ' ' + return_datatype_for_postgres(value) + ',  '
                table_columns += new_column
        # Build create table queries for tables that have list values in the provided json
        elif isinstance(meta_parse_json['data_type_data'][table_name], list):
            for key, value in meta_parse_json['data_type_data'][table_name][0].items():
                new_column = '"' + key + '"' + ' ' + return_datatype_for_postgres(value) + ',  '
                table_columns += new_column
        # create the given table
        create_sql(DB, table_name, table_columns)
        params = {}
        table_columns = ''
        # Build the insert queries for the tables which have dictionary values
        if isinstance(meta_parse_json['table_values'][table_name], dict):
            for key, value in meta_parse_json['table_values'][table_name].items():
                table_columns += '"' + key + '",'
                column_names += '%(' + key + ')s,'
                params[key] = value if value else None
            insert_sql(DB, table_name, table_columns, column_names, params)
        # Build the insert queries for the tables which have an array of values
        elif isinstance(meta_parse_json['table_values'][table_name], list):
            for item in meta_parse_json['table_values'][table_name]:
                for key, value in item.items():
                    table_columns += '"' + key + '",'
                    column_names += '%(' + key + ')s,'
                    params[key] = value if value else None
                insert_sql(DB, table_name, table_columns, column_names, params)


def return_datatype_for_postgres(datatype):
    """
    convert a python data type to one that postgres can understand in the create queries
    :param datatype: python data type
    :return: postgres data type
    """
    data_type = datatype.__name__
    if datatype == 'datetime':
        data_type = 'timestamp'
    elif data_type == 'int':
        data_type = 'bigint'
    elif data_type == 'dict':
        data_type = 'json'
    else:
        data_type = 'str'
    return data_type


def create_sql(DB, table_name, table_columns):
    """
    This will help build the create table sql and execute it.
    New Tables will be created if table does not exist
    :param DB: An object of the DB tools class
    :param table_name: name of the table to be created
    :param table_columns: string of table column names along with their data type
    """
    create_sql = '''
                         CREATE TABLE IF NOT EXISTS
                         "{table_name}"
                         ({table_columns}
                         );
                    '''.format(table_name=table_name,
                               table_columns=table_columns,
                               )
    DB.execute_sql(create_sql)


def insert_sql(DB, table_name, table_columns, column_names, params):
    """

    :param DB: An object of the DB tools class
    :param table_name: name of the table to which values will be inserted
    :param table_columns: name of the columns in the tables
    :param column_names: its a parameterized format of the column names which will help in executing the sql
    :param params: a dictionary consisting of values to be inserted against its column names
    """
    insert_sql = '''
                    INSERT INTO 
                        "{table_name}"
                    ({table_columns})
                    VALUES
                    ({column_names})
                 '''.format(table_name=table_name,
                            table_columns=table_columns,
                            column_names=column_names)
    DB.execute_sql(insert_sql, params)
