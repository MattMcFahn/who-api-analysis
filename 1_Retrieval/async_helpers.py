"""
 Async helpers for requests. These likely aren't well developed for the use 
 case at present, but act as a decent starting point for turning the syncronous
 scraping into an ascynchronous process.
 
 -----------------------------------
 Created on Wed Feb 24 15:00:48 2021
 @author: matthew.mcfahn
"""

import asyncio
import aiohttp

async def get(url, indicator):
    """
    A simple async get function, tailored to tracking failures

    Parameters
    ----------
    url : str
        The URL to make a request to
    indicator : str
        The name of the indicator being retrieved (to make tracking easier)
    Returns
    -------
    IF request fails, returns
    indicator : str
        The same indicator name, so the request can be retried later
    {indicator : resp}: dict(str: response}
        The indicator and the HTTP response from the request
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url) as response:
                resp = await response.read()
                if len(resp) > 0:
                    print(f'Successfully got url {url} with response of length {len(resp)}.')
                    return {indicator:resp}
                else:
                    print(f'Response for url {url} was of length 0')
                    return indicator
    except Exception as e:
        print(f'Unable to get url {url} due to {e.__class__}.')
        return indicator

async def main(indicators_urls):
    """
    A wrapper around the async get function to make all requests
    
    Parameters
    ----------
    indicators_urls : dict
        A dictionary of the indicators to get, and their corresponding URLs
    Returns
    -------
    ret : list
        A list of all the responses from the get function
    """
    ret = await asyncio.gather(*[get(url, indicator) for indicator, url in indicators_urls.items()])
    print(f'Finalized all {len(ret)} outputs.')
    return ret
