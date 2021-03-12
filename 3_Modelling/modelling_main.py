"""
 Module developed to transform the cleaned data into a dimensional model, with
 the tables in normal form
 
 -----------------------------------
 Created on Mon Mar  8 14:38:41 2021
 @author: matthew.mcfahn
"""

import sqlite_helpers

db_file = f'{sqlite_helpers.outdir}/who_gho_cleaned.sqlite3'
out_db_file = f'{sqlite_helpers.outdir}/who_data_model.sqlite3'

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
    starting_tables.remove('granular_data')
    input_frames = sqlite_helpers.__load_db_to_pandas(db_file, starting_tables)
    final_frames = {}
    
    # Split out comments, to drop sparsity
    indicator_data = input_frames.pop('indicator_data')
    indicator_data.rename(columns = {'spatial_dim':'area_code',
                                     'time_dim':'year'}, inplace = True)
    comments = indicator_data[['id','comments']].dropna()
    indicator_data.drop(columns = {'comments'}, inplace = True)
    final_frames['indicator_data'] = indicator_data
    final_frames['comments'] = comments
    
    # Bridge table - PK to data sources
    grain_to_datasource = input_frames.pop('datasource_to_indicator_year_and_area')
    grain_to_datasource.rename(columns = {'time_dim':'year',
                                          'spatial_dim':'area_code',
                                          'data_source_dim':'datasource_code'}, inplace = True)
    grain_to_datasource['matching_id'] = range(0, len(grain_to_datasource))
    
    final_frames['grain_to_datasource'] = grain_to_datasource
    
    # Datasources
    datasources = input_frames.pop('data_sources')
    datasources.rename(columns = {'label':'datasource_code',
                                  'url':'source_url',
                                  'source_description':'description'}, inplace = True)
    datasources.drop(columns = {'display_sequence','retrieved'}, inplace = True)
    final_frames['datasources'] = datasources
    
    # Areas
    areas = input_frames.pop('areas')
    areas.rename(columns = {'code':'area_code',
                            'title':'area'}, inplace = True)
    areas.drop(columns = {'parent_dimension', 'parent_title'}, inplace = True)
    areas = areas.sort_values(by = ['dimension','area_code']).reset_index(drop = True)
    final_frames['areas'] = areas
    
    # Indicators, categories
    indicator_info = input_frames.pop('indicator_info')
    indicator_info.drop(columns = {'display_sequence'}, inplace = True)
    categories = indicator_info[['category']].drop_duplicates().sort_values(by = 'category').reset_index(drop = True)
    categories['category_id'] = range(0, len(categories))
    categories.rename(columns = {'category':'category_name'}, inplace = True)
    categories.dropna(inplace = True)
    final_frames['categories'] = categories
    indicator_info.rename(columns = {'category':'category_name'}, inplace = True)
    indicator_info = indicator_info.merge(categories, on = 'category_name', how = 'left',
                                          validate = 'many_to_one')
    indicator_info.drop(columns = {'category_name'}, inplace = True)
    final_frames['indicator_info'] = indicator_info
    
    # Do outputs
    sqlite_helpers.__output_modelled_data(final_frames, out_db_file)
    
    return None
