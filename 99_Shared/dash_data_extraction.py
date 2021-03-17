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

# Set up the db location, based on the user, and whether the OS is Mac or Windows
if os.name == 'posix':
    outdir = f'/Users/{getuser()}/Documents/World Health Organisation project'
else:
    outdir = fr'C:\Users\{getuser()}\Documents\World Health Organisation project'
sqlite_name = 'visualisation_model'

db_file = f'{outdir}/{sqlite_name}.sqlite3'

##############################################################################
#   - Helper functions for plotting
##############################################################################
def __get_years_tickvals(years):
    """
    Helper function to determine the year ticks for a graph based on how many 
    years are passed as input
    
    Parameters
    ----------
    years : list
        A list of the years for this dataset
    Returns
    -------
    year_ticks : list
        A list of places for tick marks on the years axis of the graph
    """
    min_year = int(min(years))
    max_year = int(max(years))
    delta = max_year - min_year
    if delta >= 80:
        stepsize = 10
    elif delta >= 40:
        stepsize = 5
    elif delta >= 16:
        stepsize = 2
    else:
        stepsize = 1
    year_ticks = list(range(min_year, max_year + stepsize, stepsize))
    return year_ticks
##############################################################################
#   - Helper functions for plotting
##############################################################################


##############################################################################
#   - Data retrieval
##############################################################################
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

def __get_linegraph_data(area_code, ind_code, param_value):
    """
    Helper: Get the data for a linegraph (or barchart) for the area and ind code
    """
    conn = create_connection(db_file)
    if not param_value:
        sql = f"""SELECT *
                  FROM value_table
                  WHERE 
                      indicator_code = '{ind_code}'
                  AND area_code = '{area_code}'
                  ORDER BY measurement_year"""
    else:
        sql = f"""WITH value_data AS (
                  SELECT *
                  FROM granular_table
                  WHERE indicator_code = '{ind_code}'
                  AND area_code = '{area_code}'
                  ),
    bridge_table AS (
        SELECT * 
        FROM results_to_parameters
        WHERE parameter_value = '{param_value}'
        )
    SELECT *
    FROM bridge_table
    INNER JOIN value_data
    ON bridge_table.measurement_id = value_data.measurement_id
    ORDER BY measurement_year"""
    
    data = pd.read_sql(sql, con = conn).rename(columns = {'measurement_year':'year'})
    conn.close()
    return data

def __get_worldmap_data(ind_code, param_value = None):
    """
    """
    conn = create_connection(db_file)

    if not param_value:
        sql = f"""SELECT *
                  FROM value_table
                  WHERE 
                      indicator_code = '{ind_code}'
                  ORDER BY measurement_year"""
    else:
        sql = f"""WITH value_data AS (
                  SELECT *
                  FROM granular_table
                  WHERE indicator_code = '{ind_code}'
                  ),
    bridge_table AS (
        SELECT * 
        FROM results_to_parameters
        WHERE parameter_value = '{param_value}'
        )
    SELECT *
    FROM bridge_table
    INNER JOIN value_data
    ON bridge_table.measurement_id = value_data.measurement_id
    ORDER BY measurement_year"""
    
    data = pd.read_sql(sql, con = conn).rename(columns = {'measurement_year':'year'})
    conn.close()
    return data

def __get_parameter_types_for_ind(ind_code):
    """
    """
    conn = create_connection(db_file)
    
    sql = f"""SELECT DISTINCT parameter_name
              FROM indicator_to_parameters
              WHERE indicator_code = '{ind_code}'"""
    data = pd.read_sql(sql, con = conn)
    conn.close()
    return data

def __get_param_options(param_name):
    """Helper to grab parameter options once a type chosen"""
    conn = create_connection(db_file)
    
    sql = f"""SELECT DISTINCT parameter_value
              FROM indicator_to_parameters
              WHERE parameter_name = '{param_name}'"""
    data = pd.read_sql(sql, con = conn)
    conn.close()
    return data
##############################################################################
#   - Data retrieval
##############################################################################