"""
 A helper script to build a visualisation model based on the full snowflake 
 model that's been developed.
 
 The visualisation model does a couple of things:
     > Combines some of the dimension tables for less joins for the model
       front end
     > Cuts the data down just to 'COUNTRY' entries
     > Drops other irrelevant data (e.g. indicator not in a category)
     > Replaces some IDs with _code's
 
 -----------------------------------
 Created on Tue Mar 16 12:58:02 2021
 @author: matthew.mcfahn
"""


import pandas as pd
import sqlite_helpers

db_file = f'{sqlite_helpers.outdir}/who_data_model.sqlite3'
out_db_file = f'{sqlite_helpers.outdir}/visualisation_model.sqlite3'

def __cut_values(values_table_df, areas_df, indicator_info_df, categories):
    """
    Helper function to cut to just countries and indicators with a category.
    Also eplaces the '_id' with '_code'
    """
    target_areas = areas_df.loc[areas_df['dimension'].isin(['COUNTRY'])].area_id
    target_indicators = indicator_info_df.loc[indicator_info_df['category_id'].notna()].indicator_id
    area_mapping = areas_df.loc[areas_df['dimension'].isin(['COUNTRY'])][['area_id','area_code','area_name']]
    indicator_mapping = indicator_info_df.loc[indicator_info_df['category_id'].notna()][['indicator_id','indicator_code','indicator_name','category_id']]
    indicator_mapping = indicator_mapping.merge(categories, on = 'category_id', how = 'left', validate = 'many_to_one')
    indicator_mapping.drop(columns = {'category_id'}, inplace = True)
    
    # Cut the data
    values_table_df = values_table_df.loc[values_table_df['area_id'].isin(target_areas)]
    values_table_df = values_table_df.loc[values_table_df['indicator_id'].isin(target_indicators)]
    values_table_df = values_table_df.loc[values_table_df['numeric_value'].notna()]
    
    # Overwrite the '_id' cols with '_name', or '_code'
    values_table_df = values_table_df.merge(area_mapping, on = 'area_id', how = 'left',
                                            validate = 'many_to_one')
    values_table_df = values_table_df.merge(indicator_mapping, on = 'indicator_id', how = 'left',
                                            validate = 'many_to_one')
    
    values_table_df = values_table_df[['measurement_id','area_code', 'area_name', 'indicator_code', 'indicator_name','category_name','measurement_year',
                                       'measurement_value', 'numeric_value', 'low', 'high', 'is_main_measure']]
    
    return values_table_df

def main(db_file, out_db_file):
    """
    Takes the data from the db_file, creates a simpler dimensional model, and 
    outputs to a new sqlite file
    
    Parameters
    ----------
    db_file : str
        The filepath to the snowflake data model
    out_db_file : str
        The filepath to the visualisation database to be created
    Returns
    -------
    None
    """
    # Get tables from the cleaned db_file
    starting_tables = sqlite_helpers.__get_table_schema(db_file)
    input_frames = sqlite_helpers.__load_db_to_pandas(db_file, starting_tables)
    final_frames = {}

    # Cut the 'values_table' based on country and region, AND split out granular data
    values_table_df = input_frames.pop('values_table')
    areas_df = input_frames.pop('areas')
    indicator_info_df = input_frames.pop('indicator_info')
    categories = input_frames.pop('categories')
    
    values_table_df = __cut_values(values_table_df, areas_df, indicator_info_df, categories)
    granular_table_df = values_table_df.loc[values_table_df['is_main_measure'] == 0]
    values_table_df = values_table_df.loc[values_table_df['is_main_measure'] == 1]
    
    
    final_frames['value_table'] = values_table_df
    final_frames['granular_table'] = granular_table_df
    
    # Leave data sources as-is
    datasource_bridge_table = input_frames.pop('datasource_bridge_table')
    datasources = input_frames.pop('datasources')
    final_frames['datasource_bridge_table'] = datasource_bridge_table
    final_frames['datasources'] = datasources
    
    # Make results_to_params have more info, and only for granular rows
    results_to_parameters = input_frames.pop('results_to_parameters')
    parameters = input_frames.pop('parameters')
    results_to_parameters = results_to_parameters.merge(parameters, on = 'parameter_id',
                                                        how = 'left', validate = 'many_to_one')
    results_to_parameters.drop(columns = {'result_to_param_id'}, inplace = True)
    present_rows = granular_table_df[['measurement_id']]
    results_to_parameters = results_to_parameters.merge(present_rows, how = 'right', validate = 'many_to_one')
    
    final_frames['results_to_parameters'] = results_to_parameters
    
    # Get an indicator to parameter matching
    ind_to_param = granular_table_df[['measurement_id','indicator_code']]
    ind_to_param = ind_to_param.merge(results_to_parameters, on = 'measurement_id', how = 'inner', validate = 'one_to_many')
    ind_to_param.drop(columns = {'measurement_id'}, inplace = True)
    ind_to_param.drop_duplicates(inplace = True)
    final_frames['indicator_to_parameters'] = ind_to_param
    
    # Leave comments as-is
    comments = input_frames.pop('comments')
    final_frames['comments'] = comments

    ### - OUTPUT
    sqlite_helpers.__dimensions_to_sqlite(dimensions = final_frames, 
                                          db_file = out_db_file, 
                                          val_is_frame = True)
    return None

