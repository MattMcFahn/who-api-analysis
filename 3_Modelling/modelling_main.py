"""
 Module developed to transform the cleaned data into a dimensional model, with
 the tables in normal form
 
 -----------------------------------
 Created on Mon Mar  8 14:38:41 2021
 @author: matthew.mcfahn
"""

import pandas as pd
import sqlite_helpers

db_file = f'{sqlite_helpers.outdir}/who_gho_cleaned.sqlite3'
out_db_file = f'{sqlite_helpers.outdir}/who_data_model.sqlite3'

def __change_code_to_id(indicator_data, other_df, target = 'indicator'):
    """
    Helper function to overwrite 'indicator_code' and 'area_code' with an ID 
    instead using the 'other_df'
    """
    if target == 'indicator':
        new_col = 'indicator_id'
        cur_col = 'indicator_code'
    elif target == 'area':
        new_col = 'area_id'
        cur_col = 'area_code'
    
    other_df = other_df[[new_col,cur_col]]
    indicator_data = indicator_data.merge(other_df, on = cur_col, how = 'left',
                                          validate = 'many_to_one')
    indicator_data.drop(columns = {cur_col}, inplace = True)
    return indicator_data

def __deal_with_indicators(indicator_data, granular_data):
    """Helper for modelling indicators and parameters, also pulling out comments"""
    indicator_data['is_main_measure'] = 1
    granular_data['is_main_measure'] = 0
    
    # Split out parameters
    parameters_df = pd.concat([granular_data[['dim1_type']].rename(columns = {'dim1_type':'parameter_name'}),
                               granular_data[['dim2_type']].rename(columns = {'dim2_type':'parameter_name'}),
                               granular_data[['dim3_type']].rename(columns = {'dim3_type':'parameter_name'}),
                               ]).drop_duplicates().dropna().reset_index(drop = True)
    results_to_params_df = pd.concat([granular_data[['id','dim1_type','dim1']].rename(columns = {'dim1_type':'parameter_name','dim1':'parameter_value'}),
                                      granular_data[['id','dim2_type','dim2']].rename(columns = {'dim2_type':'parameter_name','dim2':'parameter_value'}),
                                      granular_data[['id','dim3_type','dim3']].rename(columns = {'dim3_type':'parameter_name','dim3':'parameter_value'}),
                                      ]).drop_duplicates().dropna().reset_index(drop = True)
    # Add id for parameters, reorder cols
    parameters_df['parameter_id'] = range(0, len(parameters_df))
    parameters_df = parameters_df[['parameter_id','parameter_name']]
    # Deal with the bridge table: Add an ID and overwrite parameter name with parameter ID
    results_to_params_df = results_to_params_df.merge(parameters_df, on = 'parameter_name', how = 'left',
                                                      validate = 'many_to_one')
    results_to_params_df.rename(columns = {'id':'measurement_id'}, inplace = True)
    results_to_params_df['result_to_param_id'] = range(0, len(results_to_params_df))    
    results_to_params_df = results_to_params_df[['result_to_param_id','measurement_id','parameter_id','parameter_value']]
    
    ### - Now we can recombine some indicator data!
    granular_data.drop(columns = {'dim1_type','dim1','dim2_type','dim2','dim3_type','dim3'}, inplace = True)
    indicator_data = pd.concat([indicator_data, granular_data])
    
    indicator_data.rename(columns = {'id':'measurement_id',
                                     'spatial_dim':'area_code',
                                     'time_dim':'measurement_year',
                                     'value':'measurement_value'}, inplace = True)
    # This is now fine, apart from needing to overwrite indicator_id, and area_id later instead of codes
    
    # Pull out comments, then drop from indicator_data
    comments_df = indicator_data[['measurement_id','comments']].dropna()
    comments_df['comment_id'] = range(0, len(comments_df))
    comments_df = comments_df[['comment_id','measurement_id','comments']]
    
    indicator_data.drop(columns = {'comments'}, inplace = True)
    
    return indicator_data, parameters_df, results_to_params_df, comments_df

def __get_indicator_category_tables(indicator_info_df):
    """Sequence for getting indicator and category info"""
    indicator_info_df.drop(columns = {'display_sequence'}, inplace = True)
    category_df = indicator_info_df[['category']].drop_duplicates().sort_values(by = 'category').reset_index(drop = True)
    category_df['category_id'] = range(0, len(category_df))
    category_df.rename(columns = {'category':'category_name'}, inplace = True)
    category_df.dropna(inplace = True)

    indicator_info_df['indicator_id'] = range(0, len(indicator_info_df))
    
    indicator_info_df.rename(columns = {'category':'category_name'}, inplace = True)
    indicator_info_df = indicator_info_df.merge(category_df, on = 'category_name', how = 'left',
                                                validate = 'many_to_one')
    indicator_info_df.drop(columns = {'category_name'}, inplace = True)
    
    # Columns order
    indicator_info_df = indicator_info_df[['indicator_id','indicator_code','indicator_name','url','definition_xml','category_id']]
    category_df = category_df[['category_id','category_name']]
    
    return indicator_info_df, category_df
    
def main(db_file, out_db_file):
    """
    Takes the data from the db_file, creates a dimensional model, and outputs
    to a new sqlite file
    
    Parameters
    ----------
    db_file : str
        The filepath to the cleaned data
    out_db_file : str
        The filepath to the database to be created
    Returns
    -------
    None
    """
    # Get tables from the cleaned db_file
    starting_tables = sqlite_helpers.__get_table_schema(db_file)
    input_frames = sqlite_helpers.__load_db_to_pandas(db_file, starting_tables)
    final_frames = {}


    ### - Recombine indicator and granular data, splitting out parameters
    print('Dealing with measurement data, parameters, and comments... ')
    indicator_data = input_frames.pop('indicator_data')
    granular_data = input_frames.pop('granular_data')
    
    indicator_data, parameters_df, results_to_params_df, comments_df = __deal_with_indicators(indicator_data, granular_data)
    # Write the frames that are ready to our output dict
    final_frames['comments'] = comments_df
    final_frames['results_to_parameters'] = results_to_params_df
    final_frames['parameters'] = parameters_df
    print('Dealing with measurement data, parameters, and comments... DONE')
    
    
    ### - Indicators & categories
    print('Dealing with indicators and categories... ')
    indicator_info_df = input_frames.pop('indicator_info')
    
    indicator_info_df, category_df = __get_indicator_category_tables(indicator_info_df)
    # Write the frames that are ready to our output dict
    final_frames['indicator_info'] = indicator_info_df
    final_frames['categories'] = category_df
    print('Dealing with indicators and categories... DONE')


    ### - Datasources, and bridge table - PK to data sources
    print('Dealing with datasources and a bridge table... ')
    datasources = input_frames.pop('data_sources')
    datasources.rename(columns = {'label':'datasource_code',
                                  'url':'source_url',
                                  'source_description':'description'}, inplace = True)
    datasources.drop(columns = {'display_sequence','retrieved'}, inplace = True)
    datasources['datasource_id'] = range(0, len(datasources))
    datasources = datasources[['datasource_id','datasource_code', 'display', 'source_url', 'description']]
        
    datasource_bridge_df = input_frames.pop('datasource_to_indicator_year_and_area')
    datasource_bridge_df.rename(columns = {'time_dim':'measurement_year',
                                          'spatial_dim':'area_code',
                                          'data_source_dim':'datasource_code'}, inplace = True)
    datasource_bridge_df = datasource_bridge_df.merge(indicator_data[['measurement_id','indicator_code','area_code','measurement_year']], 
                                                      on = ['indicator_code','area_code','measurement_year'],how = 'left')
    datasource_bridge_df.drop(columns = {'indicator_code','area_code','measurement_year'}, inplace = True)
    datasource_bridge_df = datasource_bridge_df.drop_duplicates()
    
    datasource_bridge_df['matching_id'] = range(0, len(datasource_bridge_df))
    datasource_bridge_df = datasource_bridge_df.merge(datasources[['datasource_id', 'datasource_code']], on = 'datasource_code',
                                                      how = 'left', validate = 'many_to_one')
    datasource_bridge_df = datasource_bridge_df[['matching_id','datasource_id','measurement_id']]
    
    final_frames['datasources'] = datasources
    final_frames['datasource_bridge_table'] = datasource_bridge_df  
    print('Dealing with datasources and a bridge table... DONE')


    ### - Areas
    print('Dealing with areas... ')
    areas_df = input_frames.pop('areas')
    areas_df.rename(columns = {'code':'area_code',
                            'title':'area_name'}, inplace = True)
    areas_df.drop(columns = {'parent_dimension', 'parent_title'}, inplace = True)
    areas_df = areas_df.sort_values(by = ['dimension','area_code']).reset_index(drop = True)
    areas_df['area_id'] = range(0, len(areas_df))
    areas_df = areas_df[['area_id','area_code','area_name','dimension','parent_code']]
    final_frames['areas'] = areas_df
    print('Dealing with areas... DONE')


    ### - Final tidying of measurement data
    print('Tidying the measurement data... ')
    indicator_data = __change_code_to_id(indicator_data, indicator_info_df, target = 'indicator')
    indicator_data = __change_code_to_id(indicator_data, areas_df, target = 'area')
    
    indicator_data = indicator_data[['measurement_id','indicator_id','area_id','measurement_year',
                                     'is_main_measure','measurement_value','numeric_value','low','high']]
    final_frames['values_table'] = indicator_data
    print('Tidying the measurement data... DONE')
    
    # Do outputs
    print('[OUTPUT] Outputting to SQLite')
    sqlite_helpers.__output_modelled_data(final_frames, out_db_file)
    
    return None
