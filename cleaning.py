"""
 Some simple cleaning - dealing with all 6.2 million rows is too much, so just
 a couple simple sweeping changes are made here
 
 ----------------------------------- 
 Created on Fri Feb 26 09:53:04 2021
 @author: matthew.mcfahn
"""

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import regex_cleaning

# To be cut after developed
import sqlite_helpers
from sqlite_helpers import __run_sql_on_db


def __grab_data_helper():
    db_file = f"{sqlite_helpers.outdir}/{sqlite_helpers.sqlite_name}.sqlite3" 
    
    # Get indicator information
    ind_query = """SELECT * 
                   FROM indicators;"""
    indicator_codes = __run_sql_on_db(db_file, query = ind_query)
    
    # Explore datatypes of different columns
    dataframe = sqlite_helpers.__load_db_to_pandas(db_file, 'indicator_data')
    return indicator_codes, dataframe
# To be cut after developed


def __isnumber(value):
    """
    Helper function to indicate whether an object passed is a float in another format.
    
    Example cases of truth:
        > value is an integer / float data type
        > value = '1,000', then the float value is 1000
        > value = '1 000 000.00' then the float value is 1000000
    Example cases of false:
        > Anything else
    
    Parameters
    ----------
    value : mixed type
        Can be an int, float, string, or None type

    Returns
    -------
    x : Bool
        A bool indicating whether the object passed is actually a float
    """
    if value == None: # Deal with Null values
        x = False
    elif type(value) == int or type(value) == float: # Easy case, numbers
        x = True
    elif type(value) == str: # Find the strings that contain numbers
        test_val = value.replace(',','').replace(' ','') # We need to deal with a couple corner cases - these come from only a couple indicators
        try:
            float(test_val)
            x = True
        except ValueError:
            x =  False
    else:
        raise Exception('Incompatible data type. Review logic.')
    
    return x

def __makenumber(value):
    """
    Helper function to change the poorly formatted numbers to floats
    
    Examples:
        > value is an integer / float data type -> float type returned
        > value = '1,000', then the float value is 1000
        > value = '1 000 000.00' then the float value is 1000000
    
    Parameters
    ----------
    value : mixed type
        Can be an int, float, or string

    Returns
    -------
    number : Float
        The value object returned as a float
    """
    if type(value) == float:
        number = value
    elif type(value) == int:
        number = float(value)
    elif type(value) == str:
        number = float(value.replace(',','').replace(' ',''))
    else:
        raise Exception('Incompatible data type. Review logic.')
    
    return number

def __likenumber(value):
    """
    Identify which values are 'like' numbers - determined by:
        > They are a string containing a number, AND
        > They aren't one of the two corner cases identified in __makenumber()
    Parameters
    ----------
    value : mixed type
        An entry value of type int, float, string, null

    Returns
    -------
    x : Bool
        Is this object 'like' a number (contains a number & isn't an exception)

    """
    if value == None: # Deal with Null values
        x = False
    elif type(value) == int or type(value) == float: # Deal with number dtypes
        x = False
    elif type(value) == str: # Main logic - for strings
        
        x = any(char.isdigit() for char in value) # Does the string contain a number?
        if x: # If the string contains a number, check if it can be transformed into a number. If so, ignore
            test_val = value.replace(',','').replace(' ','') 
            try:
                float(test_val)
                x = False
            except ValueError:
                pass
    return x

def __clean_numerical_values(dataframe):
    """Helper function to add NumericValue, Low and High if possible to get 
    this information from parsing the Value column.
    
    Parameters
    ----------
    dataframe : pd.DataFrame()
        The 'indicator_data' table from SQLite (with some cols dropped)
    Returns
    -------
    dataframe : pd.DataFrame()
        The parsed and updated dataframe
    """
    # Split into missing and present values. Change missing values inplace
    present_value = dataframe.loc[~dataframe['NumericValue'].isna()]
    missing_value = dataframe.loc[dataframe['NumericValue'].isna()]
    
    # Identify those where 'Value' is actually a number, and modify inplace
    missing_value['ValueIsNumber'] = missing_value['Value'].apply(lambda x: __isnumber(x))
    missing_value.loc[missing_value['ValueIsNumber'], 'NumericValue'] = missing_value.loc[missing_value['ValueIsNumber'], 'Value'].apply(lambda x: __makenumber(x))
    
    # Identify those CONTAINING a number. These are trickier to deal with, and we use regex
    missing_value['ValueLikeNumber'] = missing_value['Value'].apply(lambda x: __likenumber(x))
    non_updated_df = missing_value.loc[~missing_value['ValueLikeNumber']]
    likenumbers_df = missing_value.loc[missing_value['ValueLikeNumber']]
    likenumbers_df = regex_cleaning.__clean_likenumbers(likenumbers_df)

    
    
    
    return dataframe
    
def __clean_raw_ingest(dataframe):
    """
    Takes the data ingested as is, and does some cleaning supported by the 
    investigation on the scratchpad.
    
    Returns the modified df
    """
    # Couple columns we just don't need
    
    dataframe.drop(columns = {'SpatialDimType','TimeDimType','DataSourceDimType'}, inplace = True)
    
    # Get data source for each indicator, where poss
    # TODO: What uniquely identifies a datasource? E.g. is it {Indicator, Year, Country}? (It appears this doesn't always work...)
    data_sources = dataframe[['IndicatorCode', 'TimeDim', 'SpatialDim', 'DataSourceDim']]
    data_sources.drop_duplicates(inplace = True)   
    data_sources['DataSourceDim'] = data_sources['DataSourceDim'].replace({'NA':None})
    data_sources.dropna(inplace = True)
    
    dataframe.drop(columns = {'DataSourceDim'}, inplace = True)
    
    # Deal with Value and NumericValue columns!
    # TODO
    
    
    return dataframe, data_sources

# Notes on extra info needing to be gathered:
    # - SpatialDimType: Pull codes and such for other spatial types, get in extra table
