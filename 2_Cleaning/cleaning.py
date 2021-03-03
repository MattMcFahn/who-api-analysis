"""
 A cleaning script for the staged data retrieved from the WHO GHO API.
 
 The main purpose of this is to:
     > Improve data quality in the 'indicator_data' table (as this contains
                                                           the most info)
     > Drop unnecessary information
     > Begin data modelling by drawing out separate data into characteristic 
     tables
 
 This script is a work in progress
 ----------------------------------- 
 Created on Fri Feb 26 09:53:04 2021
 @author: matthew.mcfahn
"""

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

import regex_cleaning
from who_helpers import __isnumber, __makenumber, __likenumber

def __clean_bespoke_indicators(missing_value):
    """
    A number of indicators in the WHO GHO database use spaces instead of commas,
    e.g. a number is formatted as 10 453 000 000.00 instead of 10,453,000,000.00
    or even 10453000000.00 as a float type.
    
    This function handles those cases upfront, rather than editing them later
    via regex. This isn't particularly needed, as we don't need to fill these
    columns for these indicators.

    Parameters
    ----------
    missing_value : pd.DataFrame
        The ingested data where 'NumericValue' is missing

    Returns
    -------
    missing_value : pd.DataFrame
        The same frame, where the 'Value' entries for these specific cases has
        had ' ' replaced by ','
    """
    targets = ['HIV_0000000022',
               'Rev_excise',
               'Rev_govt_total',
               'Rev_imp_other',
               'Rev_VAT',
               'R_Price_lowest_cost_estimate',
               'R_Price_premium_estimate']
               
    unchanged = missing_value.loc[~missing_value['IndicatorCode'].isin(targets)]
    messy_df = missing_value.loc[missing_value['IndicatorCode'].isin(targets)]
    messy_df['Value'] = messy_df['Value'].str.strip()
    messy_df['Value'] = messy_df['Value'].str.replace(' ',',')
    messy_df['Value'] = messy_df['Value'].replace({'Not,applicable':'Not applicable',
                                               'No,data':'No data',
                                               'Data,not,available':'Data not available'})
    missing_value = unchanged.append(messy_df)    
    return missing_value

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
    original_columns = list(dataframe.columns)
    # If 'Low', and 'High' are present, but 'NumericValue' isn't, fill it with the average
    # Long equation, split over two lines with \ char
    dataframe.loc[dataframe['Low'].notna() & dataframe['High'].notna() & dataframe['NumericValue'].isna(), 'NumericValue'] = \
    dataframe.loc[dataframe['Low'].notna() & dataframe['High'].notna() & dataframe['NumericValue'].isna()][['Low','High']].mean(axis = 1)
    
    # Split into missing and present values. Change missing values inplace
    present_value = dataframe.loc[~dataframe['NumericValue'].isna()]
    missing_value = dataframe.loc[dataframe['NumericValue'].isna()]
    
    # HACK - Adjust a couple rouge indicators where a space is used instead of a comma for numbers
    missing_value = __clean_bespoke_indicators(missing_value)
    
    # Identify those where 'Value' is actually a number, and modify inplace
    missing_value['ValueIsNumber'] = missing_value['Value'].apply(lambda x: __isnumber(x))
    missing_value.loc[missing_value['ValueIsNumber'], 'NumericValue'] = missing_value.loc[missing_value['ValueIsNumber'], 'Value'].apply(lambda x: __makenumber(x))
    
    # Identify those CONTAINING a number. These are trickier to deal with, and we use regex
    missing_value['ValueLikeNumber'] = missing_value['Value'].apply(lambda x: __likenumber(x))
    othercases_df = missing_value.loc[~missing_value['ValueLikeNumber']]
    likenumbers_df = missing_value.loc[missing_value['ValueLikeNumber']]
    likenumbers_df = regex_cleaning.__clean_likenumbers(likenumbers_df)
    
    dataframe = pd.concat([present_value, othercases_df, likenumbers_df])
    dataframe = dataframe[original_columns]
    
    return dataframe
    
def cleaning_main(dataframe):
    """
    Takes the 'indicator_data' ingested as is, and cleans it:
        > Removes unneeded columns
        > Fills some missing 'NumericValue', 'Low', 'High' columns by parsing 
        'Value' for strings containing numbers
        > AOB? 
        TODO
    Parameters
    ----------
    dataframe : pd.DataFrame()
        The 'indicator_data' table from the staging SQLite table
    Returns
    -------
    TODO: Determine
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
    
    # Deal with Value and NumericValue columns - We can pull some extra info from parsing Value
    dataframe = __clean_numerical_values(dataframe)
    
    return dataframe, data_sources

# Notes on extra info needing to be gathered:
    # - SpatialDimType: Pull codes and such for other spatial types, get in extra table

