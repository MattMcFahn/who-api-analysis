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
import numpy as np

import regex_cleaning
from who_helpers import __isnumber, __makenumber, __likenumber
import sqlite_helpers
import data_sources_scraping

db_file = db_file = f'{sqlite_helpers.outdir}/{sqlite_helpers.sqlite_name}.sqlite3'
out_db_file = f'{sqlite_helpers.outdir}/who_gho_cleaned.sqlite3'

def __data_suppression(indicator_dataframe):
    """Helper function to reduce data used by the large indicator dataframe"""
    # Couple columns we just don't need
    indicator_dataframe.drop(columns = {'SpatialDimType','TimeDimType','DataSourceDimType',
                              'Date','TimeDimensionValue','TimeDimensionBegin','TimeDimensionEnd'}, 
                   inplace = True)
    
    # Memory suppression of columns: Numerical
    indicator_dataframe['TimeDim'] = indicator_dataframe['TimeDim'].astype('Int16') # Note, we have to make it Int16 to support Nulls, rather than int16
    indicator_dataframe['ID'] = indicator_dataframe['ID'].astype('int32')
    
    # Memory suppression of columns: Object -> Category
    indicator_dataframe[['IndicatorCode','SpatialDim','Dim1','Dim1Type','Dim2','Dim2Type','Dim3','Dim3Type','DataSourceDim']] = \
    indicator_dataframe[['IndicatorCode','SpatialDim','Dim1','Dim1Type','Dim2','Dim2Type','Dim3','Dim3Type','DataSourceDim']].astype('category')
    
    return indicator_dataframe

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

def __strip_string(value):
    """Simple helper - strip string, don't change numbers"""
    if not type(value) == str:
        pass
    else:
        value = value.replace(' ','').replace(',','').strip()
    return value

def __report_datafill_stats(dataframe, numerics, lows, highs):
    """Simple reporting helper"""
    original_stats = {'NumericValue': {'Number':np.count_nonzero(numerics),
                                   'Percentage':round((np.count_nonzero(numerics) / len(dataframe))*100,2)},
                      'Low': {'Number':np.count_nonzero(lows),
                              'Percentage':round((np.count_nonzero(lows) / len(dataframe))*100,2)},
                      'High': {'Number':np.count_nonzero(highs),
                               'Percentage':round((np.count_nonzero(highs) / len(dataframe))*100,2)},
                      }
    # Identify which values are null/non-null in our resulting columns
    numerics = dataframe['NumericValue'].notna()
    lows = dataframe['Low'].notna()
    highs = dataframe['High'].notna()
    final_stats = {'NumericValue': {'Number':np.count_nonzero(numerics),
                                   'Percentage':round((np.count_nonzero(numerics) / len(dataframe))*100,2)},
                      'Low': {'Number':np.count_nonzero(lows),
                              'Percentage':round((np.count_nonzero(lows) / len(dataframe))*100,2)},
                      'High': {'Number':np.count_nonzero(highs),
                               'Percentage':round((np.count_nonzero(highs) / len(dataframe))*100,2)},
                      }
    
    print(f'The number of rows to deal with was: {len(dataframe)}')
    for column in original_stats.keys():
        start_vals = original_stats[column]
        end_vals = final_stats[column]
        
        print(f'For column: {column}')
        print(f'''Number of non-null rows:
              > Start: {start_vals['Number']}
              > End: {end_vals['Number']}
              > Increase = {end_vals['Number'] - start_vals['Number']}
              ''')
        print(f'''Percentage of rows that were non-null increased {round(end_vals['Percentage'] - start_vals['Percentage'], 2)}%,
              from {start_vals['Percentage']}% to {end_vals['Percentage']}%''')
    return None

def __clean_numerical_values(dataframe):
    """Helper function to add NumericValue, Low and High if possible to get 
    this information from parsing the Value column.
    
    We first cut out those with all of 'NumericValue', 'Low', 'High' present.
    We don't need to modify these.
    
    Parameters
    ----------
    dataframe : pd.DataFrame()
        The 'indicator_data' table from SQLite (with some cols dropped)
    Returns
    -------
    dataframe : pd.DataFrame()
        The parsed and updated dataframe
    """
    # Track the original columns, as we'll only want to return them
    original_columns = list(dataframe.columns)
    num_rows = len(dataframe)
    
    # Identify which values are null/non-null in our target columns
    numerics = dataframe['NumericValue'].notna()
    lows = dataframe['Low'].notna()
    highs = dataframe['High'].notna()
    
    # Collect our frames to concat back together at the end
    final_frames = []
    
    ### - If all of 'NumericValue', 'Low', 'High' are present, job done!    
    print('[ALLNUMS] Identifying rows with all numerics present, and leaving unchanged... ')
    complete_df = dataframe.loc[numerics & lows & highs]
    print(f" Number of rows of indicator data: {num_rows}")
    print(f" Number of rows with complete numeric information: {len(complete_df)}")
    final_frames += [complete_df]
    # Continue with remaining rows
    others = dataframe.loc[~(numerics & lows & highs)]
    num_rows = len(others)
    print('[ALLNUMS] Identifying rows with all numerics present, and leaving unchanged... DONE')
    print(f" Number of rows remaining to address: {num_rows}\n")

    del dataframe
    
    ### - Now, if 'Value' is a number, we can use this to fill ALL missing cols
    print("[ISNUMS] Identifying rows where 'Value' is a number, and using this to populate numerics numeric columns... ")
    isnumber = others.loc[others['Value'].apply(lambda x: __isnumber(x))]
    isnumber['Value'] = isnumber['Value'].apply(lambda x: __makenumber(x))
    isnumber.loc[isnumber['NumericValue'].isna(), 'NumericValue'] = isnumber.loc[isnumber['NumericValue'].isna(), 'Value']
    isnumber.loc[isnumber['Low'].isna(), 'Low'] = isnumber.loc[isnumber['Low'].isna(), 'Value']
    isnumber.loc[isnumber['High'].isna(), 'High'] = isnumber.loc[isnumber['High'].isna(), 'Value']
    final_frames += [isnumber]
    print(f" Number of rows of data to process: {num_rows}")
    print(f" Number of rows where 'Value' can be used to populate numerics: {len(isnumber)}")    
    print("[ISNUMS] Identifying rows where 'Value' is a number, and using this to populate numerics numeric columns... DONE")
    # Continue with remaining rows
    others = others.loc[~others['Value'].apply(lambda x: __isnumber(x))]
    num_rows = len(others)
    print(f" Number of rows remaining to address: {num_rows}\n")
    
    ### - Take those that 'Value' is 'like' a number (using regex), and parse Value to fill other columns
    print("[LIKENUMS] Identifying rows where 'Value' has multiple numbers, and parsing this info to fill numerics... ")
    likenumbers_df = others.loc[others['Value'].apply(lambda x: __likenumber(x))]
    likenumbers_df = regex_cleaning.__clean_likenumbers(likenumbers_df) # TODO: Fix this function
    final_frames += [likenumbers_df]
    print(f" Number of rows of data to process: {num_rows}")
    print(f" Number of rows where 'Value' can be used to populate numerics: {len(likenumbers_df)}")
    print("[LIKENUMS] Identifying rows where 'Value' has multiple numbers, and parsing this info to fill numerics... DONE")
    # Continue with remaining rows
    others = others.loc[~others['Value'].apply(lambda x: __likenumber(x))]
    num_rows = len(others)
    print(f" Number of rows remaining to address: {num_rows}\n")
    
    ### - All we can do now is: (1) fill 'NumericValue' as an average if 'Low' & 'High' present, or fill 'Low' and 'High' if 'NumericValue' present
    print("[OTHERS] Filling other corner cases, where possible... ")
    others.loc[others['Low'].notna() & others['High'].notna() & others['NumericValue'].isna(), 'NumericValue'] = others.loc[others['Low'].notna() & others['High'].notna() & others['NumericValue'].isna()][['Low','High']].mean(axis = 1)
    others.loc[others['NumericValue'].notna() & others['Low'].isna() & others['High'].isna(), 'Low'] = others.loc[others['NumericValue'].notna() & others['Low'].isna() & others['High'].isna(), 'NumericValue']
    others.loc[others['NumericValue'].notna() & others['Low'].isna() & others['High'].isna(), 'High'] = others.loc[others['NumericValue'].notna() & others['Low'].isna() & others['High'].isna(), 'NumericValue']
    print("[OTHERS] Filling other corner cases, where possible... DONE")
    final_frames += [others]
        
    # Recombine our modified data
    dataframe = pd.concat(final_frames)
    dataframe = dataframe[original_columns]
    
    # Report on how much data we've filled
    __report_datafill_stats(dataframe, numerics, lows, highs)
    
    return dataframe
    
def clean_indicator_data(dataframe):
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
    # Cut out information about DataSources, and drop from the indicator data
    # NOTE: {Indicator, Year, Country} works MOST of the time, but NOT always: 102 cases where there are 2 data sources
    data_sources = dataframe[['IndicatorCode', 'TimeDim', 'SpatialDim', 'DataSourceDim']]
    data_sources.drop_duplicates(inplace = True)   
    data_sources['DataSourceDim'] = data_sources['DataSourceDim'].replace({'NA':None})
    data_sources.dropna(subset = ['DataSourceDim'], inplace = True)
    
    dataframe.drop(columns = {'DataSourceDim'}, inplace = True)
    
    # Deal with Value and NumericValue columns - We can pull some extra info from parsing Value
    dataframe = __clean_numerical_values(dataframe)
    
    # Deal with other Nulls, or messy values: All that's implemented for now is dropping rows missing area data
    dataframe = dataframe.loc[dataframe['SpatialDim'].notna()] # 280 rows have no SpatialDim
    
    return dataframe, data_sources

def __clean_indicator_info(indicators):
    """
    Cleans the strings contained the the indicator / category dataframe
    
    Parameters
    ----------
    indicators : pd.DataFrame()
        A dataframe of indicator information

    Returns
    -------
    indicators : pd.DataFrame()
        A dataframe of indicator information
    """
    ### Cleaning 'IndicatorName'
    # Simple stuff - double spaces, etc.
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('  ',' ')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('<sub>','')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('</sub>',' ')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('<sup>','')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('</sup>',' ')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('&#956; ',' ')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('&#8805; ',' ')
    
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('1 000 000','1,000,000')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("1'000'000",'1,000,000')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("1000000",'1,000,000')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("million population",'1,000,000 population')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace('100 000','100,000')
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("100'000","100,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("100000","100,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("10'000","10,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("10 000","10,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("10000","10,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("1'000","1,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("1 000","1,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("1000","1,000")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace("\( ","(")
    indicators['IndicatorName'] = indicators['IndicatorName'].str.replace(" \)",")")
    
    indicators['IndicatorName'] = indicators['IndicatorName'].str.strip()
    
    ### Cleaning 'Category': Standardise spelling and map a couple variants
    categories = [x for x in indicators['CATEGORY'].unique() if not x == None]
    categories = {category: category.title() for category in categories}
    categories['Negelected tropical diseases'] = 'Neglected Tropical Diseases'
    categories['Rsud: Governance, Policy And Financing : Prevention'.upper()] = 'RSUD: Governance, Policy And Financing: Prevention'
    categories['UHC'] = 'Universal Health Coverage'
    categories['HIV/AIDS and other STIs'] = 'HIV/AIDS And Other STIs'
    categories['ICD'] = 'ICD'
    categories = {key: val.replace('Amr','AMR').replace('Goe','GOe').replace('Rsud','RSUD').replace('And','and') 
                 for (key, val) in categories.items()}
    indicators['CATEGORY'] = indicators['CATEGORY'].map(categories)
    
    # Add some categories based on desk research. 13 indicators still not mapped
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('GDO_'), 'CATEGORY'] = 'Global Dementia Observatory'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('EMF'), 'CATEGORY'] = 'Electromagnetic fields'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('FAMILYPLANNINGUNPDUHC'), 'CATEGORY'] = 'Sexual and Reproductive Health'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('SG_DMK_SRCR_FN_ZS'), 'CATEGORY'] = 'Sexual and Reproductive Health'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('GHED_'), 'CATEGORY'] = 'Health Financing'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('_UHC'), 'CATEGORY'] = 'Universal Health Coverage'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('IHRSPAR_'), 'CATEGORY'] = 'International Health Regulations (2005) monitoring framework,SPAR'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('NLIS_'), 'CATEGORY'] = 'Nutrition'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('PHE_'), 'CATEGORY'] = 'Public Health and Environment'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('RADON'), 'CATEGORY'] = 'Public Health and Environment'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('SA_'), 'CATEGORY'] = 'Global Information System on Alcohol and Health'
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('SE_'), 'CATEGORY'] = "Global strategy for women's, children's and adolescents' health"
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('SG_'), 'CATEGORY'] = "Global strategy for women's, children's and adolescents' health"
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('SH_'), 'CATEGORY'] = "Global strategy for women's, children's and adolescents' health"
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('SI_'), 'CATEGORY'] = "Global strategy for women's, children's and adolescents' health"
    indicators.loc[indicators['CATEGORY'].isna() & indicators['IndicatorCode'].str.contains('SP_'), 'CATEGORY'] = "Global strategy for women's, children's and adolescents' health"
    
    # Reorder columns
    indicators = indicators[['IndicatorCode', 'IndicatorName', 'CATEGORY', 'display_sequence', 'url','DEFINITION_XML']]
    
    return indicators

def __clean_sources(sources_df):
    """
    Bespoke function for cleaning up the Data Sources data that's been retrieved.
    In future, might need to do more data retrieval to add extra information.
    
    Parameters
    ----------
    sources_df : pd.DataFrame
        Dataframe of data sources, including their code, display name, 
        description and URL.
    Returns
    -------
    sources_df : pd.DataFrame
        The same frame, with columns parsed to clean this info into the correct
        columns.
    """
    # Pull some extra sources info
    sources_df = data_sources_scraping.__pull_source_info(sources_df)
    
    # Now turn to cleaning
    # TODO: More cleaning - this is messy AF
    return sources_df

def main(db_file, out_db_file):
    """
    Takes the data from the db_file, cleans it, and outputs it to the 
    out_db_file. Uses the seperate functions in this module to clean the data.
    
    Parameters
    ----------
    db_file : str
        The filepath to the staged data
    out_db_file : str
        The filepath to the database to be created
    Returns
    -------
    None
    """
    ### - Load data
    # Get tables from the staged db_file (these should be ['measures', 'countries', 'regions', 'indicator_data','indicators', 'data_sources'])
    starting_tables = sqlite_helpers.__get_table_schema(db_file)
    starting_tables.remove('measures')
    input_frames = sqlite_helpers.__load_db_to_pandas(db_file, starting_tables)
    final_frames = {}
    
    ### - Cleaning
    # Indicator data (this is the most intensive)
    indicator_dataframe = input_frames.pop('indicator_data')
    print('''[INDICATORS] Reducing memory usage of the indicator data... ''')
    print(f'Starting memory used: {round(indicator_dataframe.memory_usage().sum() / (1024**2),2)} Mb')
    indicator_dataframe = __data_suppression(indicator_dataframe)
    print('''[INDICATORS] Reducing memory usage of the indicator data... DONE''')
    print(f'Reduced to: {round(indicator_dataframe.memory_usage().sum() / (1024**2), 2)} Mb')
    
    print('''[INDICATORS] Now cleaning the indicator data... ''')
    indicator_dataframe, data_sources = clean_indicator_data(indicator_dataframe)
    print('''[INDICATORS] Now cleaning the indicator data... DONE''')
    final_frames['datasource_to_indicator_year_and_area'] = data_sources
    final_frames['indicator_data'] = indicator_dataframe
    
    
    # - Countries and regions
    print("[AREAS] Cleaning info for countries / regions... ")
    areas = pd.DataFrame(indicator_dataframe['SpatialDim'].unique()).rename(columns = {0:'Code'})
    countries_dataframe = input_frames.pop('countries')
    regions_dataframe = input_frames.pop('regions')
    areas = areas.merge(pd.concat([countries_dataframe, regions_dataframe]), 
                        how = 'left', on = 'Code')
    final_frames['areas'] = areas
    del countries_dataframe, regions_dataframe
    print("[AREAS] Cleaning info for countries / regions... DONE")
    
    
    # - Indicators ('Category' is the important bit of info here)
    print("[MEASURES] Cleaning info indicators and their categories... ")
    retrieved_indicators = pd.DataFrame(indicator_dataframe['IndicatorCode'].unique()).rename(columns = {0:'IndicatorCode'})
    indicators = input_frames.pop('indicators')
    indicators = indicators.merge(retrieved_indicators, how = 'inner', on = 'IndicatorCode', validate = 'one_to_one')
    indicators = __clean_indicator_info(indicators)
    final_frames['indicator_info'] = indicators
    print("[MEASURES] Cleaning info indicators and their categories... DONE")
    
    
    # - Data sources
    print("[SOURCES] Cleaning info for data sources... ")
    sources_df = input_frames.pop('data_sources')
    ind_sources = pd.DataFrame(data_sources['DataSourceDim'].unique()).rename(columns = {0:'DataSourceDim'})
    ind_sources['retrieved'] = 1
    sources_df = sources_df.merge(ind_sources, how = 'left', validate = 'one_to_one', left_on = 'label', right_on = 'DataSourceDim')
    sources_df.drop(columns = {'DataSourceDim'}, inplace = True)
    sources_df['retrieved'] = sources_df['retrieved'].fillna(0)
    sources_df = __clean_sources(sources_df)
    final_frames['data_sources'] = sources_df
    print("[SOURCES] Cleaning info for data sources... DONE")
    
    
    ### Cleaning completed - Output to a new SQLite database
    sqlite_helpers.__dimensions_to_sqlite(final_frames, db_file = out_db_file, val_is_frame = True)
    
    return None
