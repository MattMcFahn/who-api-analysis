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

def __identify_cases(string):
    """
    Uses regex matching to identify what kind of string is contained:
        'Number 1 [Number 2 - Number 3]': Case 1
        'Number 1 - Number 2': Case 2
        '>Number1 [Number2 - >Number1]': Case 3
        '<Number1 [<Number1 - Number2]': Case 4
        '>Number': Case 5
        '<Number': Case 6
        'Less than Number': Case 7
        'More than Number': Case 8
        'Number1 to Number2': Case 9
        'Number and "-+" after': Case 10
        'More than Number1 but less than Number2': Case 11
        'Less than Number1 but more than Number2': Case 12
        'Number1/Number2/Number3': Case 13
        'Number1/Number2': Case 14
        'Number"UNIT"': Case 15
        'Number1/Number2"UNIT"': Case 16 (will be caught by 13)
        'around Number': Case 17
        'Contains "risky" and a number': Case 18
        " 'Number1-Number2 | 'Number1, Number 2": Case 19
        "[Number]": Case 20
    
    TODO: 
        2) Numbers contained in square brackets (for some of the existing patterns above - optional)
        3) Optional ' ' or '=' after the < or > signs
        4) Deal with ',' or ' ' in some numbers < remove them. (This might actually be slightly involved...)
        5) Deal with leading or trailing whitespace
    
    Parameters
    ----------
    string : str
        The string containing some kind of numeric information to be parsed
    
    Returns
    -------
    case : int
        A number identifying what kind of case this string falls under
    """
    # Define patterns
    patterns = {}
    patterns[1] = "(\d+(\.\d+)?)( )?\[(\d+(\.\d+)?)( )?-( )?(\d+(\.\d+)?)\]" # Case 1, a bit involved due to decimals
    patterns[2] = ".*(\[)?(\d+(\.\d+)?)( )?-( )?(\d+(\.\d+)?)(\])?" # Simpler case 2
    patterns[3] = ">(=)?( )?(\d+(\.\d+)?)( )?\[(\d+(\.\d+)?)( )?-( )?>(=)?( )?(\d+(\.\d+)?)\]" # Case 3 even more complex
    patterns[4] = "<(=)?( )?(\d+(\.\d+)?)( )?\[(\d+(\.\d+)?)( )?-( )?<(=)?( )?(\d+(\.\d+)?)\]" 
    patterns[5] = ">(=)?( )?(\d+(\.\d+)?)"
    patterns[6] = "<(=)?( )?(\d+(\.\d+)?)"
    patterns[7] = "(?i).*less"
    patterns[8] = "(?i).*more"
    patterns[9] = "(\d+(\.\d+)?)( )?to( )?(\d+(\.\d+)?)"
    patterns[10] = "(\d+(\.\d+)?)[+-]+?"
    patterns[11] = "(?i).*more.*less"
    patterns[12] = "(?i).*less.*more"
    patterns[13] = "(\[)?(\d+(\.\d+)?)(\])?/(\[)?(\d+(\.\d+)?)(\])?/(\[)?(\d+(\.\d+)?)(\])?"
    patterns[14] = "(\[)?(\d+(\.\d+)?)(\])?/(\[)?(\d+(\.\d+)?)(\])?"
    patterns[15] = "(\d+(\.\d+)?)" #TODO - Figure this one out
    patterns[17] = "(?i)around( )?(\d+(\.\d+)?)|approx.( )?(\d+(\.\d+)?)"
    patterns[18] = "(?i)risky"
    patterns[19] = "'(\d+(\.\d+)?)-( )?(\d+(\.\d+)?)"
    patterns[20] = "\[[1-9(\.), ]"
    patterns[21] = "(\[)?(\d+(\.\d+)?)(\.)?(\d+(\.\d+)?)"
    
    # TODO: Replace with just single key
    codes = set()
    for key, pattern in patterns.items():
        if bool(re.match(pattern, string)):
            codes |= {key}
    if codes == set():
        codes = {0}
    
    return codes

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
    # Strip trailing and leading whitespace
    df['Value'] = df['Value'].str.strip()
    
    df['Cases'] = df['Value'].apply(lambda x: __identify_cases(x))
    df['FirstCase'] = df['Cases'].apply(min)
    # Hacks... should work into regex really.
    df.loc[df['Value'].str.contains('men'), 'Cases'] = None
    df.loc[df['Value'].str.contains('Year'), 'Cases'] = None
    df.loc[df['Value'].str.contains('Grades'), 'Cases'] = None
    df.loc[df['Value'].str.contains('tobacco'), 'Cases'] = None
    df.loc[df['Cases'] == {10, 15}, 'FirstCase'] = 15
    df.loc[df['Cases'] == {7, 8, 11}, 'FirstCase'] = 11
    
    
    return df
