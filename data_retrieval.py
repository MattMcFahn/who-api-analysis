"""
 Little scratchpad for accessing the WHO GHO API.
 
 # Basic URL structure is: http://HOST/INSTANCE/XXX
 
 Parameters in the WHO endpoint:
     Fixed:
         HOST > ghoapi.azureedge.net
         INSTANCE > api
    Optional:
        [TO DO...]
        This is just due to OData2 protocol. Not sure how to generalise.
        
    https://www.who.int/data/gho/info/gho-odata-api
    
 -----------------------------------
 Created on Tue Feb 23 16:41:09 2021
 @author: matthew.mcfahn
"""

import requests
import json
import pandas as pd
import time

from async_helpers import main as main
import asyncio


# Set URLs
root = 'https://ghoapi.azureedge.net/api/'
dimensions = {'measures':{'url':f'{root}/Dimension'},
              'countries':{'url':f'{root}/DIMENSION/COUNTRY/DimensionValues'},
              'regions':{'url':f'{root}/DIMENSION/REGION/DimensionValues'},
              'indicators':{'url':f'{root}/Indicator'}
              }

### - Retrieval routines
def __get_main_measures(dimensions):
    """
    Get the main measures via requests, and format as needed.
    Parameters
    ----------
    dimensions : dict
        The measures, and just their URLs

    Returns
    -------
    dimensions : dict
        The original dict modified in place to add responses, json and DataFrames
    """
    # Get the measure, country, region and indicator information 
    for dim, contents in dimensions.items():
        # Make request
        response = requests.get(contents['url'])
        # Load to JSON
        json_content = json.loads(response.content)
        # Get the content to a DataFrame
        frame = pd.DataFrame(json_content['value'])
        # Write back to dimensions dict
        dimensions[dim]['response'] = response
        dimensions[dim]['json'] = json
        dimensions[dim]['content'] = frame
    return dimensions

# Now, we load indicator data, using an async method
async def __get_maindata_async(dimensions, test = False):
    """
    I think the method for this currently is crap. Need to review (I'm not good
    with async method).
    
    From the list of indicators in the dimensions['indicator'] dataframe 
    (ind_frame), make a load of async calls to retrive the request responses.
    
    Handle errors and empty returns by recalling.

    Parameters
    ----------
    dimensions : dict ( key: dict (key : pd.DataFrame))
        The dimensions['indicators']['content'] DataFrame is all that's needed

    Returns
    -------
    responses : dict
        A dictionary of {indicator: request response}
    """
    indicators_frame = dimensions['indicators']['content']
    indicator_codes = indicators_frame.IndicatorCode.unique()
    indicators_urls = {code: f'{root}/{code}' for code in indicator_codes}
    if test:
        indicators_urls = {k: indicators_urls[k] for k in list(indicators_urls)[:250]}
    amount = len(indicators_urls)
    
    # Async get the bulk, retrying whilst fails
    final_responses = []
    iu = indicators_urls.copy()
    start_amount = len(iu)
    start = time.time()
    while len(iu) > 0:
        amount = len(iu)
        task = asyncio.create_task(main(iu, amount))
        responses = await task
        if task.done():
            print('\n\n\n')
            # Add successful responses
            final_responses += [x for x in responses if type(x) != str]
            
            # Failed responses
            failed_resp = [x for x in responses if type(x) == str]
            iu = {code: val for code, val in indicators_urls.items() if code in failed_resp}
            print(f'Of {amount} requests, {len(iu)} failed. Retrying')
            print('\n\n\n')
    end = time.time()
    print(f'Took {end - start} seconds to pull {start_amount} websites.')
    
    del responses, failed_resp, iu, amount, start, end
    responses = {}
    for d in final_responses:
        responses.update(d)
    
    return responses
