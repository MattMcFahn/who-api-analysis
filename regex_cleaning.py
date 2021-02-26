"""
 Specific module for regex mainpulation of 'Value' entries containing numbers
 to give return values for 'NumericValue', 'Low', and 'High'.
 
 There's been some subjective choice over what this logic should be. It's 
 outlined in the logic within the module, but a few examples are:
     * Value = 12 [10 - 15] >>>
         NumericValue = 12
         Low = 10
         High = 15
     * Value = >10 [9 - >10] >>>
         NumericValue = 10
         Low = 9
         High = Null
     * Value = 1- least risky >>>
         NumericValue = 1
         Low = Null
         High = Null
     * Value = Less than 10 >>>
         NumericValue = 10
         Low = Null
         High = 10
     * Value = 42Hz
         NumericValue = 42
         Low = 42
         High = 42
 
 The above list is by no means exhaustive, but provides a good outline for the 
 kind of rules applied. NOT ALL 'likenumber' entities ae mapped to numbers, 
 only cases where there was a clear parsing.
 
 -----------------------------------
 Created on Fri Feb 26 13:23:54 2021
 @author: matthew.mcfahn
"""

import re


def __clean_likenumbers(likenumbers_df):
    """
    TODO

    Parameters
    ----------
    likenumbers_df : pd.DataFrame()
        A dataframe of the records where 'Value' contains a numeric character

    Returns
    -------
    df : pd.DataFrame()
        The input object, with 'NumericValue', 'Low', 'High' updated.

    """
    df = likenumbers_df.copy()
    
    return df
