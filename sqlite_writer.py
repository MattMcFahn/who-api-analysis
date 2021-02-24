"""
 Helper functions to connect to and write SQLite files.
 -----------------------------------
 Created on Wed Feb 24 15:52:43 2021
 @author: matthew.mcfahn
"""

import json
import sqlite3
from sqlite3 import Error
from getpass import getuser

import data_retrieval

# dimensions = data_retrieval.__get_main_measures(data_retrieval.dimensions)
# responses = data_retrieval.__get_maindata_async(dimensions)

if os.name == 'posix':
    outdir = f'/Users/{getuser()}/Documents'
else:
    outdir = f'C:\Users\{getuser()\Documents'
sqlite_name = 'WHO_Data'

def create_connection(db_file):
    """Create a connection to the SQLite database specified by db_file"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def __responses_to_sqlite(responses, outdir = outdir, sqlite_name = sqlite_name):
    """
    Probably best to do in a for-loop as this will overload RAM
    
    Takes the contents of the responses dict, converts to JSON in sequence, and
    outputs to a sqlite file.
    
    TODO: From investigating the SQLite3 currently out, improve handling of different cases...
    
    Parameters
    ----------
    responses : dict
        The http responses from the WHO api
    outdir : str
        Location to be output to
    sqlite_name : str
        Name of the sqlite3 file to be written

    Returns
    -------
    None
    """
    # Create connection, make table for responses
    db_file = f'{outdir}/{sqlite_name}.sqlite3'
    conn = create_connection(db_file)
    
    # Define the structure of the data table, with datatypes
    create_table_sql = """CREATE TABLE IF NOT EXISTS data (
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
    # Create table
    if conn is not None:
        # create projects table
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)

    else:
        print("Error! cannot create the database connection.")    
        return None
    
    # Update table with results from responses
    for key, response in responses.items():
        print(f'Key: {key}')
        frame = pd.DataFrame(json.loads(response)['value'])
        frame.to_sql(name = 'data', con = conn, if_exists = 'append', index = False)
    
    return None

def __dimensions_to_sqlite(dimensions, outdir = outdir, sqlite_name = sqlite_name):
    """
    Very rough - Doesn't currently bother specifying the PK, Data types etc.
    in the resultant tables...
    """
    # Create connection, make table for responses
    db_file = f'{outdir}/{sqlite_name}.sqlite3'
    conn = create_connection(db_file)
    
    for key, contents in dimensions.items():
        frame = contents['content']
        frame.to_sql(name = key, con = conn, index = False)
    return None