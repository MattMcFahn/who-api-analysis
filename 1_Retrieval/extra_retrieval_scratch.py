"""
Created on Wed Mar  3 12:47:41 2021

@author: matthew.mcfahn
"""

import requests
import json
import pandas as pd

import data_retrieval

# Test using the Athena API for the data sources info
path = r'https://apps.who.int/gho/athena/data/'

query_params = '?format=json' # We want json returns

response = requests.get(f'{path}{query_params}')
# Load to JSON
json_content = json.loads(response.content)

# Looks like we can use json_content['dimension'] to figure out how to pull some metadata we want!

# Stuff we want:
#   * GHOCAT: Indicator Categories
#   * DATASOURCE: Data source(s)
#   * PUBLISHSTATE: Publish States (not sure what this is, but worth investigating)
# Then from the 'dataset' return
#   * FCD: Fact Core Data
#   * FD: Fact Data

desired_metadata = {'indicator_categories':{'url':f'{path}GHOCAT/{query_params}'},
                    'indicators':{'url':f'{path}GHO/{query_params}'},
                    'datasources':{'url':f'{path}DATASOURCE/{query_params}'}
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

# OK, of above, only 'datasources' is useful. We need to extract the pandas/tabular info using:
#   JSON['dimension'][0]['code'] < - A list of dictionaries
# From each dictionary, we want to pull out: ['label', 'display','url'], and ['attr'][0]['value'] when attr is not empty.
# Using this, we'll probably still have a lot of data sources where we don't have anything descriptive for the source, but we can try and pursue other methods to get info for those missing

# OK, for indicators, we can get more by pulling out the json['dimension'][0]['code'] dict
# From this, we can extract label and display as the 'label' and 'display' keys for each dict, and
# Then we can get categories with ['attr'], combine all the dicts, and take the ones with key 'CATEGORY', and 'DEFINITION_XML'
# We can use the 'DEFINITION_XML' to make more requests to get extra info, if we need!

# For a dedicated wrapper, I'll want to switch to the Athena API, I think
