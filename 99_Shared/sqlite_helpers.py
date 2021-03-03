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
    outdir = f'/Users/{getuser()}/Documents'
else:
    outdir = fr'C:\Users\{getuser()}\Documents'
sqlite_name = 'WHO_Data'

def create_connection(db_file):
    """Create a connection to the SQLite database specified by db_file"""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        raise Exception(f'SQLite connection failed with error code {e}')

def __responses_to_sqlite(responses, outdir = outdir, sqlite_name = sqlite_name):
    """
    Probably best to do in a for-loop as this will overload RAM
    
    Takes the contents of the responses dict, converts to JSON in sequence, and
    outputs to a sqlite file.
        
    Parameters
    ----------
    responses : dict
        The http responses from the WHO API
    outdir : str
        A local location to be output to on SSD
    sqlite_name : str
        Name of the sqlite3 file to be written

    Returns
    -------
    None
    """
    # Create connection
    db_file = f'{outdir}/{sqlite_name}.sqlite3'
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
        c = conn.cursor()
        c.execute(create_table_sql)
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

def __dimensions_to_sqlite(dimensions, outdir = outdir, sqlite_name = sqlite_name):
    """Very simple data dump of the dimensions data pulled by __get_main_measures"""
    
    # Create connection
    db_file = f'{outdir}/{sqlite_name}.sqlite3'
    conn = create_connection(db_file)
    
    for key, contents in dimensions.items():
        frame = contents['content']
        # Just use inbuilt pandas function to write to SQLite via conn
        frame.to_sql(name = key, con = conn, index = False)
    
    conn.close()
    return None

def __load_db_to_pandas(db_file, table_name):
    """Pulls the table specified from the database, returning as pd.DataFrame"""
    conn = create_connection(db_file)
    
    dataframe = pd.read_sql(sql = f"""SELECT * FROM {table_name}""", con = conn)
    conn.close()
    return dataframe

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
