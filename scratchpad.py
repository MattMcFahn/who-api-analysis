"""
Created on Thu Feb 25 11:52:30 2021

@author: matthew.mcfahn
"""

import sqlite_helpers
from sqlite_helpers import __run_sql_on_db
import cleaning

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

# Exploring outputs from cleaning :~
indicator_codes, dataframe = cleaning.__grab_data_helper()
new_frame, data_sources = cleaning.__clean_raw_ingest(dataframe)
merge_col = ['ID']
ref_cols = ['IndicatorCode','TimeDim','SpatialDim','Dim1','Dim2','Dim3']
value_col = ['Value']
changed_cols = ['NumericValue','Low','High']

dataframe = dataframe[merge_col + ref_cols + value_col + changed_cols]
new_frame = new_frame[merge_col + changed_cols]

test = dataframe.merge(new_frame, on = merge_col, how = 'inner', suffixes = ('_Original','_New'), validate = 'one_to_one')

