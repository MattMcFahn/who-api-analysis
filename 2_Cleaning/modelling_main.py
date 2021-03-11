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
    # Main thing here is that we need to identify which 'dim' corresponds to 'all' for each 'dim_type'
    # And then we need to create an identifier for mapping for 'id' to multiple dimensions.
    # Or do we leave slightly denormalised?
    
    # Get tables from the cleaned db_file
    starting_tables = sqlite_helpers.__get_table_schema(db_file)
    input_frames = sqlite_helpers.__load_db_to_pandas(db_file, starting_tables)
    final_frames = {}
    return None
