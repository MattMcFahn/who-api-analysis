"""
 A module to retrieve the main data necessary from the WHO Global Health
 Observatory, using their API:
     https://www.who.int/data/gho/info/gho-odata-api
 
 There is another API available for the GHO, using Athena rather than OData. 
 As a more standard protocol, the OData API has been used here.
    
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

# Set URLs for the API endpoints of the main things we need to scrape
root = 'https://ghoapi.azureedge.net/api/'
dimensions = {'measures':{'url':f'{root}/Dimension'},
              'countries':{'url':f'{root}/DIMENSION/COUNTRY/DimensionValues'},
              'regions':{'url':f'{root}/DIMENSION/REGION/DimensionValues'},
              'indicators':{'url':f'{root}/Indicator'}
              }

### - Retrieval routines
def __get_main_measures(dimensions):
    """
    Just use the requests module to make requests to the API for the top level
    dimensions we need to retrieve
    
    Parameters
    ----------
    dimensions : dict
        A nested JSON like structure of {'measure':content} format. For each 
        measure, the content is a dictionary, which has 'url' containing the
        API address for that measure
    Returns
    -------
    dimensions : dict
        The original dictionary modified in place to add responses, json, and 
        DataFrames from the successful API requests
    """
    
    for dim, contents in dimensions.items():
        # Make request
        response = requests.get(contents['url'])
        # Load to JSON
        json_content = json.loads(response.content)
        # Get the content to a DataFrame
        frame = pd.DataFrame(json_content['value'])
        
        # Write back to dimensions dict
        dimensions[dim]['response'] = response
        dimensions[dim]['json'] = json_content
        dimensions[dim]['content'] = frame
    
    return dimensions

def __extract_indicator_urls(dimensions):
    """Helper function to pull out a list of indicators and URLs from dimensions
    """
    indicators_frame = dimensions['indicators']['content']
    indicator_codes = indicators_frame.IndicatorCode.unique()
    indicators_urls = {code: f'{root}/{code}' for code in indicator_codes}
    return indicators_urls

# Now, we load indicator data, using an async method
async def __get_maindata_async(indicators_urls, test = False):
    """
    Makes requests asynchronously for all the indicator URLs to pull >2,300 API
    endpoints.
    
    TODO: Review and improve the way this async functionality has been used.
    Parameters
    ----------
    indicators_urls : dict (indicator: url)
        A dictionary of indicators and their API URLs to retrieve
    Returns
    -------
    responses : dict
        A dictionary of {indicator: request response}
    """
    # If we want to just test this function, we'll pull the first 250 only
    if test:
        indicators_urls = {k: indicators_urls[k] for k in list(indicators_urls)[:250]}
    
    # Set up some control parameters
    final_responses = []
    iu = indicators_urls.copy()
    start_amount = len(iu)
    
    # Use a while loop to kep trying to async scrape the API, retrying as long as there are still failed responses
    start = time.time()
    while len(iu) > 0:
        amount = len(iu)
        
        # Create a task for scraping all the 'iu' URLs, and await it
        task = asyncio.create_task(main(iu))
        responses = await task
        
        # Only once it's done do we want to progress, so we wait for task.done()
        if task.done():
            print('\n\n\n')
            # Add successful responses
            final_responses += [x for x in responses if type(x) != str]
            
            # Failed responses
            failed_resp = [x for x in responses if type(x) == str]
            iu = {code: val for code, val in indicators_urls.items() if code in failed_resp}
            print(f'Of {amount} requests, {len(iu)} failed. Retrying')
            print('\n\n\n')
    # Report on time taken once we've got successful responses for all URLs
    end = time.time()
    print(f'Took {end - start} seconds to pull {start_amount} websites.')
    
    # Format final_responses nicely in a dictionary
    del responses, failed_resp, iu, amount, start, end
    responses = {}
    for d in final_responses:
        responses.update(d)
    
    return responses
