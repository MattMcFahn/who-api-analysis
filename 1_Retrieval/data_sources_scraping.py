"""
 Extra module for scraping extra information about data sources from HTML pages,
 using BeautifulSoup.
 
 TODO: Could probably pull more indicator information using this same method.
 
 -----------------------------------
 Created on Mon Mar  8 09:47:29 2021
 @author: matthew.mcfahn
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd

def __pull_source_info(sources_df):
    """
    

    Parameters
    ----------
    sources_df : pd.DataFrame()
        A dataframe of the data sources present, and URLs to more info about 
        them, where present
    Returns
    -------
    sources_df : pd.DataFrame()
        The dataframe modified in place - source descriptions scraped from
        webpages where possible
    """
    print("[SOURCES] Scraping extra info on sources from the web. Will take a while")
    # Cut just to the info we'll be pulling
    retrieval_needed = sources_df.loc[sources_df['retrieved'] == 1]
    retrieval_needed = retrieval_needed.loc[~(retrieval_needed['url'] == '')]
    retrieval_needed = retrieval_needed.loc[retrieval_needed['url'].notna()]
    # Adhoc cutting - we only get the NLIS ones
    retrieval_needed = retrieval_needed.loc[retrieval_needed['label'].str.contains('NLIS')]
    retrieval_needed = retrieval_needed.loc[retrieval_needed['source_description'].isna()][['label','url']].drop_duplicates()
    sources_dict = dict(zip(retrieval_needed.label, retrieval_needed.url))
    
    # Do the retrieval
    responses = {}
    counter = 1
    print(f'There are: {len(sources_dict)} datasets we need to scrape info for')
    for source, url in sources_dict.items():
        print(f'Grabbing source: {counter}')
        # Turns out we can use pandas built in functionality!
        try:
            source_info = pd.read_html(url)[0].loc[2].loc[0]
        except:
            print(f'Source {source} couldnt be scraped. Passing.')
        source_info = source_info[0:source_info.find('Author') - 2]
        source_info = source_info[source_info.find(':') + 2:]
        responses[source] = source_info
        counter += 1
    
    # Write to our original frame
    retrieved_df = pd.DataFrame.from_dict(responses, orient='index').reset_index().rename(columns = {'index':'label',0:'description'})
    sources_df = sources_df.merge(retrieved_df, on = 'label', how = 'left', validate = 'one_to_one')
    
    # Some adhoc filling
    sources_df.loc[(sources_df['label'].astype(str).str.contains('DHS_') | sources_df['label'].astype(str).str.contains('_DHS')) & sources_df['source_description'].isna(), 'description'] = 'Demographic and Health Survey http://www.dhsprogram.com/'
    sources_df.loc[(sources_df['label'].astype(str).str.contains('MICS_') | sources_df['label'].astype(str).str.contains('_MICS')) & sources_df['source_description'].isna(), 'description'] = 'UNICEF Multiple Indicator Cluster Surveys, Household survey'
    
    # Update, return
    sources_df.loc[sources_df['source_description'].isna(), 'source_description'] = sources_df.loc[sources_df['source_description'].isna(), 'description']
    sources_df.drop(columns = {'description'}, inplace = True)
    
    return sources_df
