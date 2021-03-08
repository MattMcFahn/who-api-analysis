"""
Created on Wed Mar  3 12:47:41 2021

@author: matthew.mcfahn
"""

import requests
import json
import pandas as pd

# Test using the Athena API for the data sources info
path = r'https://apps.who.int/gho/athena/data/'

query_params = '?format=json' # We want json returns, rather than XML

response = requests.get(f'{path}{query_params}')
# Load to JSON
json_content = json.loads(response.content)

# Looks like we can use json_content['dimension'] from the above to figure out how to pull some metadata we want!

desired_metadata = {'indicator_categories':{'url':f'{path}GHOCAT/{query_params}'},
                    'indicators':{'url':f'{path}GHO/{query_params}'},
                    'datasources':{'url':f'{path}DATASOURCE/{query_params}'},
                    'location':{'url':f'{path}LOCATION/{query_params}'}
                    }

# Tailor the data retrieval function for the Athena API
def __get_main_measures(desired_metadata):
    """
    Just use the requests module to make requests to the API for the top level
    metadata we need to retrieve
    
    Parameters
    ----------
    desired_metadata : dict
        A nested JSON like structure of {'measure':content} format. For each 
        measure, the content is a dictionary, which has 'url' containing the
        API address for that measure
    Returns
    -------
    desired_metadata : dict
        The original dictionary modified in place to add responses, json, and 
        DataFrames from the successful API requests
    """
    
    for dim, contents in desired_metadata.items():
        print(dim)
        # Make request
        response = requests.get(contents['url'])
        # Load to JSON
        json_content = json.loads(response.content)
        
        # Write back to dimensions dict
        desired_metadata[dim]['response'] = response
        desired_metadata[dim]['json'] = json_content
    
    return desired_metadata

desired_metadata = __get_main_measures(desired_metadata)

# OK, of above, only 'datasources' is useful. We need to extract the pandas/tabular info using:
#   JSON['dimension'][0]['code'] < - A list of dictionaries
# From each dictionary, we want to pull out: ['label', 'display','url'], and ['attr'][0]['value'] when attr is not empty.
# Using this, we'll probably still have a lot of data sources where we don't have anything descriptive for the source, but we can try and pursue other methods to get info for those missing

# OK, for indicators, we can get more by pulling out the json['dimension'][0]['code'] dict
# From this, we can extract label and display as the 'label' and 'display' keys for each dict, and
# Then we can get categories with ['attr'], combine all the dicts, and take the ones with key 'CATEGORY', and 'DEFINITION_XML'
# We can use the 'DEFINITION_XML' to make more requests to get extra info, if we need!

# For a dedicated wrapper, I'll want to switch to the Athena API, I think

### - Pull out a pandas dataframe from DATASOURCES
datasources = desired_metadata['datasources']['json']['dimension'][0]['code']
datasources = [{**x, **{'source_description':None}} for x in datasources if len(x['attr']) == 0] + [{**x, **{'source_description':x['attr'][0]['value']}} for x in datasources if len(x['attr']) == 1]

sources_df = pd.DataFrame(datasources)
sources_df.drop(columns = {'attr'}, inplace = True)

### - Similarly for 'indicators', getting groupings
indicators = desired_metadata['indicators']['json']['dimension'][0]['code']
indicators = [x for x in indicators if len(x['attr']) == 0] + \
    [{**x, **{y['category']:y['value'] for y in x['attr']}} for x in indicators if len(x['attr']) > 0]

indicators_df = pd.DataFrame(indicators)
indicators_df['attr'].apply(pd.Series)
indicators_df.drop(columns = {'RENDERER_ID', 'attr', 'IMR_ID'}, inplace = True)
### - Indicator categories doesn't make much sense to me... leave it!

# We'll import the existing indicator table, and just update these ones with the new info, and rewrite
import sqlite_helpers
db_file = f'{sqlite_helpers.outdir}/{sqlite_helpers.sqlite_name}.sqlite3'
table_name = 'indicators'

curr_inds_df = sqlite_helpers.__load_db_to_pandas(db_file, table_name)
curr_inds_df = curr_inds_df.merge(indicators_df, left_on = ['IndicatorCode'], right_on = ['label'],
                                  how = 'left', validate = 'one_to_one')
# Cut columns, write the indicators and sources tables to SQLite3
curr_inds_df = curr_inds_df[['IndicatorCode', 'IndicatorName','display_sequence','url', 'DEFINITION_XML','CATEGORY']]

sqlite_helpers.__frame_to_sqlite(curr_inds_df, 'indicators', db_file, if_exists = 'replace')
sqlite_helpers.__frame_to_sqlite(sources_df, 'data_sources', db_file, if_exists = 'replace')



