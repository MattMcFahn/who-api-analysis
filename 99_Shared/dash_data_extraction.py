"""
 Helper retrieval functions in SQL for the app.py Dash app. It's more efficient
 to retrieve data at the last minute using SQL rather than loading all into pandas
 at the beginning of the app.
 
 -----------------------------------
 Created on Tue Mar 16 14:32:39 2021
 @author: matthew.mcfahn
"""

import pandas as pd
import sqlite3
from sqlite3 import Error
from getpass import getuser
import os

# Set up the out directory, based on the user, and whether the OS is Mac or Windows
if os.name == 'posix':
    outdir = f'/Users/{getuser()}/Documents/World Health Organisation project'
else:
    outdir = fr'C:\Users\{getuser()}\Documents\World Health Organisation project'
sqlite_name = 'visualisation_model'

db_file = f'{outdir}/{sqlite_name}.sqlite3'

# Some SQL queries for static assets
def __get_static_queries():
    helper_path = '/Users/matthew.mcfahn/Documents/GitHub/who-api-analysis/99_Shared'
    indicator_query = f'{helper_path}/dash_get_indicators.sql'
    category_query = f'{helper_path}/dash_get_categories.sql'
    area_query = f'{helper_path}/dash_get_areas.sql'
    query_paths = {'indicators':indicator_query,
                   'categories':category_query,
                   'areas':area_query}
    queries = {}
    for key, query_loc in query_paths.items():
        sql = ""
        for content in open(query_loc,'r'):
            sql += content
        queries[key] = sql
    return queries

### - Template SQL queries with variables. This is gonna be a bit tricky to work out.
# TODO:
select_values_temp = """SELECT indicator_name, area_name, measurement_year, numeric_value, 
                        """


### - Generic helpers
def create_connection(db_file = db_file):
    """Create a connection to the SQLite database specified by db_file"""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        raise Exception(f'SQLite connection failed with error code {e}')

def get_static_data_assets(db_file):
    """
    
    Parameters
    ----------
    conn : Connection object to visualisation model

    Returns
    -------
    #TODO:

    """
    conn = create_connection(db_file)
    
    queries = __get_static_queries()
    frames = {}
    
    for key, query in queries.items():
        frame = pd.read_sql(query, con = conn)
        frames[key] = frame
    
    conn.close()
    
    return frames['indicators'], frames['categories'], frames['areas']


def __get_available_areas(ind_code):
    """
    Helper: Get a list of areas available for the given indicator code
    """
    conn = create_connection(db_file)

    sql = f"""SELECT DISTINCT area_code
              FROM value_table
              WHERE indicator_code = '{ind_code}'"""
    
    areas = pd.read_sql(sql, con = conn)
    conn.close()
    return list(areas.area_code)

def __get_linegraph_data(area_code, ind_code, main_measure = True):
    """
    Helper: Get the data for a linegraph (or barchart) for the area and ind code
    """
    conn = create_connection(db_file)
    if main_measure:
        sql = f"""SELECT *
                  FROM value_table
                  WHERE 
                      indicator_code = '{ind_code}'
                  AND area_code = '{area_code}'
                  AND is_main_measure = 1
                  ORDER BY measurement_year"""
    else:
        raise Exception('WEVE NOT FIGURED THAT OUT YET :(')
    
    data = pd.read_sql(sql, con = conn).rename(columns = {'measurement_year':'year'})
    conn.close()
    return data

def __get_worldmap_data(ind_code, main_measure = True):
    """
    """
    conn = create_connection(db_file)

    if main_measure:
        sql = f"""SELECT *
                  FROM value_table
                  WHERE 
                      indicator_code = '{ind_code}'
                  AND is_main_measure = 1
                  ORDER BY measurement_year"""
    else:
        raise Exception('WEVE NOT FIGURED THAT OUT YET :(')
    
    data = pd.read_sql(sql, con = conn).rename(columns = {'measurement_year':'year'})
    conn.close()
    return data

