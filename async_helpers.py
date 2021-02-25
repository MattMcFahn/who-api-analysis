"""
 Async helpers for requests
 
 -----------------------------------
 Created on Wed Feb 24 15:00:48 2021
 @author: matthew.mcfahn
"""

# Async packages for grabbing all URLS
import asyncio
import aiohttp

# Async get functions for getting 1,000s of responses quicker than sequential
async def get(url, indicator):
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

async def main(indicators_urls, amount):
    ret = await asyncio.gather(*[get(url, indicator) for indicator, url in indicators_urls.items()])
    print(f'Finalized all {len(ret)} outputs.')
    return ret
