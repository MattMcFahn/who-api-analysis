"""
 Helper functions to connect to and write local SQLite files. Utilised 
 currently for data storage as a "staged" phase, but may evolve into a way to 
 break up the data after cleaning, and modelling.
 
 -----------------------------------
 Created on Wed Feb 24 15:52:43 2021
 @author: matthew.mcfahn
"""

import pandas as pd
import json
import sqlite3
from sqlite3 import Error
from getpass import getuser
import os

# Set up the out directory, based on the user, and whether the OS is Mac or Windows
if os.name == 'posix':
    outdir = f'/Users/{getuser()}/Documents/World Health Organisation project'
else:
    outdir = fr'C:\Users\{getuser()}\Documents\World Health Organisation project'
sqlite_name = 'WHO_Data'

db_file = f'{outdir}/{sqlite_name}.sqlite3'
create_db_script = f'/Users/matthew.mcfahn/Documents/Github/who-api-analysis/create_modelled_db.sql'

### - Generic helpers
def create_connection(db_file = db_file):
    """Create a connection to the SQLite database specified by db_file"""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        raise Exception(f'SQLite connection failed with error code {e}')

def __frame_to_sqlite(frame, table_name, db_file, if_exists = 'fail'):
    """
    Writes a pandas dataframe to a table in the existing db_file
    Parameters
    ----------
    frame : pd.DataFrame()
        The frame to be written to the table
    table_name : str
        The name of the table in the database
    db_file : str
        The filepath to the database file
    Returns
    -------
    None
    """
    conn = create_connection(db_file)
    
    frame.to_sql(name = table_name, con = conn, index = False, if_exists = if_exists)
    
    conn.close()
    return None

def __load_db_to_pandas(db_file, table_names):
    """
    Pulls the table(s) specified from the database, returning as pd.DataFrame
    if one table, and wrapped in a dictionary if > 1 table
    
    Parameters
    ----------
    db_file : str
        Path to the SQlite3 database
    table_names : str / list
        The tables to be retrieved
    Returns
    -------
    dataframes : dict / pd.DataFrame()
        If type(table_names) == str, then returns a pd.DataFrame() of that table
        If type(table_names) == list, then returns a dict of 
        {table : pd.DataFrame for table in table_names}
    """
    conn = create_connection(db_file)
    # Exception catching
    if not (type(table_names) == list or type(table_names) == str):
        raise Exception("Incorrect 'table_names' param passed. Review")
    
    # Deal with string case
    if type(table_names) == str:
        print(f"""[SQLite] Loading table: {table_names}... """)
        dataframe = pd.read_sql(sql = f"""SELECT * FROM {table_names}""", con = conn)
        print(f"""[SQLite] Loading table: {table_names}... DONE""")
        conn.close()
        return dataframe
    else:
        pass
    
    # Main logic, list
    dataframes = {}
    for table in table_names:
        print(f"""[SQLite] Loading table: {table}...""")
        dataframes[table] = pd.read_sql(sql = f"""SELECT * FROM {table}""", con = conn)
        print(f"""[SQLite] Loading table: {table}... DONE""")
    return dataframes

def __run_sql_on_db(db_file, query):
    """
    Generate a pandas dataframe from the SQL query on the db_file
    
    Parameters
    ----------
    db_file : str
        Path to the SQlite3 database
    query : str
        SQL query
    Returns
    -------
    df : pd.DataFrame()
        Dataframe of return result from the query
    """
    conn = create_connection(db_file)
    
    dataframe = pd.read_sql(sql = query, con = conn)
    conn.close()
    return dataframe

def __get_table_schema(db_file):
    """
    Get the table schema for the given db_file

    Parameters
    ----------
    db_file : str
        Filepath to the specified database

    Returns
    -------
    table_schema : list
        A list of tables to be found in the database
    """
    if not os.path.exists(db_file):
        raise Exception(f"The {db_file} object wasn't found on your SSD. Please review.")
    
    conn = create_connection(db_file)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_schema = [x[0] for x in cur.fetchall()]
    cur.close()
    conn.close()
    return table_schema

### - Bespoke functions
def __responses_to_sqlite(responses, db_file = db_file):
    """
    Bespoke function: outputs all responses for the indicator_data table.
    
    Takes the contents of the responses dict, converts to JSON in sequence, and
    outputs to a sqlite file.
        
    Parameters
    ----------
    responses : dict
        The http responses from the WHO API
    db_file : str
        Filepath to the database to output to
    Returns
    -------
    None
    """
    # Create connection
    conn = create_connection(db_file)
    
    # Define the structure of the 'indicator_data' table, with datatypes
    create_table_sql = """CREATE TABLE IF NOT EXISTS indicator_data (
                                    ID integer PRIMARY KEY,
                                    IndicatorCode varchar(100) NOT NULL,
                                    SpatialDimType varchar(100),
                                    SpatialDim varchar(20),
                                    TimeDimType varchar(20),
                                    TimeDim integer,
                                    Dim1Type text,
                                    Dim1 text,
                                    Dim2Type text,
                                    Dim2 text,
                                    Dim3Type text,
                                    Dim3 text,
                                    DataSourceDimType text,
                                    DataSourceDim text,
                                    Value integer,
                                    NumericValue decimal,
                                    Low decimal,
                                    High decimal,
                                    Comments text,
                                    Date datetime,
                                    TimeDimensionValue int,
                                    TimeDimensionBegin datetime,
                                    TimeDimensionEnd datetime
                   );"""
    # Create 'indicator_data' table
    try:
        cur = conn.cursor()
        cur.execute(create_table_sql)
    except Error as e:
        raise Exception(f'Creating SQLite table failed with error code {e}')
    
    # Finally, update table with results from responses
    for key, response in responses.items():
        print(f'Inserting data for: {key}')
        frame = pd.DataFrame(json.loads(response)['value'])
        frame.to_sql(name = 'data', con = conn, if_exists = 'append', index = False)
    
    # Close connection
    conn.close()
    return None

def __dimensions_to_sqlite(dimensions, db_file = db_file, val_is_frame = False):
    """Very simple data dump of the dimensions data pulled by __get_main_measures"""
    print(f'Outputting to SQLite database: {db_file}')
    # Create connection
    conn = create_connection(db_file)
    
    for key, contents in dimensions.items():
        print(f'Outputting: {key}')
        if not val_is_frame:
            frame = contents['content']
        else:
            frame = contents
        # Just use inbuilt pandas function to write to SQLite via conn
        frame.to_sql(name = key, con = conn, index = False)
    
    conn.close()
    print(f'Outputting to SQLite database: {db_file} <<< DONE')
    return None

def __output_modelled_data(final_frames, out_db_file):
    """
    Custom routine for outputting the modelled data, specifying datatypes and
    cardinality of relationships etc.
    
    Parameters
    ----------
    final_frames : dict (str : pd.DataFrame())
        A dictionary of table names and data for tables
    out_db_file : str
        Filepath to the SQLite file to be output
    Returns
    -------
    None
    """
    # Check we have expected data
    tables = list(final_frames.keys())
    if not tables == ['comments','results_to_parameters', 'parameters', 'indicator_info', 'categories', 'datasources', 'datasource_bridge_table', 'areas', 'values_table']:
        raise Exception('Expected a different set of tables. Please review.')
    
    # Create connection
    conn = create_connection(out_db_file)
    
    ### - Define the structure of the tables, with datatypes
    create_table_sql = ""
    for content in open(create_db_script,'r'):
        create_table_sql += content
    
    # Create tables
    try:
        cur = conn.cursor()
        cur.executescript(create_table_sql)
    except Error as e:
        raise Exception(f'Creating SQLites table failed with error code {e}')
    
    # Finally, update table with results from responses
    for key, frame in final_frames.items():
        print(f'Outputting:{key}\n{frame}')
        frame.to_sql(name = key, con = conn, if_exists = 'append', 
                     index = False)
    print('Data output: COMPLETE')
    # Close connection
    cur.close()
    conn.close()
    return None
