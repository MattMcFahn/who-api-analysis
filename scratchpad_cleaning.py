"""
Created on Thu Feb 25 11:52:30 2021

@author: matthew.mcfahn
"""

import sqlite_helpers
from sqlite_helpers import __run_sql_on_db

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

import matplotlib.pyplot as plt

db_file = f"{sqlite_helpers.outdir}/{sqlite_helpers.sqlite_name}.sqlite3" 

# Get indicator information
ind_query = """SELECT * 
               FROM indicators;"""
indicator_codes = __run_sql_on_db(db_file, query = ind_query)


# Explore datatypes of different columns
dataframe = sqlite_helpers.__load_db_to_pandas(db_file, 'indicator_data')

# Let's look at columns one by one, and figure out if we can remove them into other tables.
columns = list(dataframe.columns)
# ID we leave. Ideally, we should replace 'IndicatorCode' with an 'IndicatorID' column, but we'll do this later


spatial_types = dataframe.SpatialDimType.unique()
sliced = dataframe.loc[dataframe['SpatialDimType'] == 'WORLDBANKINCOMEGROUP']

# Look at time types
time_types = dataframe.TimeDimType.unique()
missing_df = dataframe.loc[dataframe['TimeDimType'].isna()] # This is actually that it's missing for all records for these indicators

# Quick view of amount of entries by year...
times = dataframe.groupby('TimeDim')['IndicatorCode'].nunique().reset_index()
plt.bar(x = times['TimeDim'], height = times['IndicatorCode'])


# Dim 1, 2, 3 seem a bit weird at the moment - a way to subdivide indicators.
# I should perhaps think about how to turn the 4 entities into a single identifier?
dim1types = dataframe.Dim1Type.unique()
sliced = dataframe.loc[dataframe['Dim1Type'] == 'WEALTHDECILE']

dim3Types = dataframe.Dim3Type.unique()
sliced = dataframe.loc[dataframe['Dim3Type'] == 'CHILDCAUSE']

# Let's look at the ones without 'NumericValue' present
sliced = dataframe.loc[dataframe['NumericValue'].isna()]
# Looks like to investigate properly, we'll need to define value types / codifications for indicators. E.g. mapping "Yes/No" to 1/0.

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
    
    return dataframe

# Notes on extra info needing to be gathered:
    # - SpatialDimType: Pull codes and such for other spacial types, get in extra table
    # - TimDimType: Doesn't matter at all
    
    