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
import numpy as np

def __identify_cases(string):
    """
    NOTE: This function is a mess. It works, but barely.
    
    Uses regex matching to identify what kind of string is contained. Some
    examples are below.
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
        Case 21?
        "[Number1 Number1 ..." (i.e. ' ' should be ','): Case 22
        "Number1 with 2 commas / Number2 with 2 commas": Case 23
    
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
    patterns[2] = "(\[)?(\d+(\.\d+)?)( )?-( )?(\d+(\.\d+)?)(\])?" # Simpler case 2
    patterns[3] = ">(=)?( )?(\d+(\.\d+)?)( )?\[(>)?(\d+(\.\d+)?)( )?-( )?>(=)?( )?(\d+(\.\d+)?)\]" # Case 3 even more complex
    patterns[4] = "<=? ?(\d+(\.\d+)?) ?\[<?(\d+(\.\d+)?) ?- ?<?=? ?(\d+(\.\d+)?)\]" 
    patterns[5] = ">(=)?( )?(\d+(\.\d+)?)"
    patterns[6] = "<(=)?( )?(\d+(\.\d+)?)"
    patterns[7] = "(?i).*less"
    patterns[8] = "(?i).*more"
    patterns[9] = "(\d+(\.\d+)?)( )?to( )?(\d+(\.\d+)?)"
    patterns[10] = "(\d+(\.\d+)?)[+-]+?"
    patterns[11] = "(?i).*more.*less"

    patterns[13] = "(\[)?(\d+([\.|,]\d+)?)(\])?/(\[)?(\d+([\.|,]\d+)?)(\])?/(\[)?(\d+([\.|,]\d+)?)(\])?"
    patterns[14] = "(\[)?(\d+(\.\d+)?)(\])?/(\[)?(\d+(\.\d+)?)(\])?"
    patterns[15] = "(\d+(\.\d+)?)" #TODO - Figure this one out

    patterns[19] = "'(\d+(\.\d+)?)-( )?(\d+(\.\d+)?)"
    patterns[20] = "\[[1-9(\.), ]"
    patterns[21] = "(\[)?(\d+(\.\d+)?)(\.)?(\d+(\.\d+)?)"

    patterns[23] = "\[\d+,\d+,\d+]/\[\d+,\d+,\d+]"

    patterns[25] = "\d+(,\d+){2}?(\.\d+)?/\d+(,\d+){2}?(\.\d+)?"
    patterns[26] = "\d+(,\d+){1}?(\.\d+)?/\d+(,\d+){2}?(\.\d+)?/\d+(,\d+){2}?(\.\d+)?"
    patterns[27] = '\d+(,\d+){1}?(\.\d+)?/\d+(,\d+){1}?(\.\d+)?'
    patterns[28] = '\d+  - \d+'
    
    # TODO: Replace with just single key
    codes = set()
    for key, pattern in patterns.items():
        if bool(re.match(pattern, string)):
            codes |= {key}
    if codes == set():
        codes = {0}
    
    return codes

def __group_cases(df):
    """
    Used to group cases based on what logic will apply for filling the 
    'NumericValue', 'Low' and 'High' fields (multiple cases have the same logic)
    
    Parameters
    ----------
    df : pd.DataFrame()
        The ingest data with cases identified for each 'Value' entry

    Returns
    -------
    df : pd.DataFrame
        Adds a 'MainCase' and 'Group' identifier
    """
    # Hacks... should work into regex really.
    df.loc[df['Value'].str.contains('men'), 'Cases'] = None
    df.loc[df['Value'].str.contains('Male'), 'Cases'] = None
    df.loc[df['Value'].str.contains('Year'), 'Cases'] = None
    df.loc[df['Value'].str.contains('Grades'), 'Cases'] = None
    df.loc[df['Value'].str.contains('tobacco'), 'Cases'] = None
    df.loc[df['Value'].str.contains('yyyy'), 'Cases'] = None
    df.loc[df['Value'].str.contains('excise'), 'Cases'] = None
    df.loc[df['Value'].str.contains('revenue'), 'Cases'] = None
    df.loc[df['Value'].str.contains('Drivers'), 'Cases'] = None
    df.loc[df['Value'].str.contains('GB'), 'Cases'] = None
    df['Cases'] = df['Cases'].fillna({})
    
    # Identify the 'MainCase'
    df.loc[~(df['Cases'].isna()), 'MainCase'] = df.loc[~(df['Cases'].isna()), 'Cases'].apply(min)
    df['MainCase'].fillna(0, inplace = True)
    
    df.loc[df['Cases'] == {10, 15}, 'MainCase'] = 15
    df.loc[df['Cases'] == {7, 8, 11}, 'MainCase'] = 11
    df.loc[df['Cases'] == {20, 23}, 'MainCase'] = 23
    df.loc[df['Cases'] == {15,25}, 'MainCase'] = 25
    df.loc[df['Cases'] == {15,27}, 'MainCase'] = 27
    df.loc[df['Cases'] == {27, 21, 15}, 'MainCase'] = 27
    df.loc[df['Cases'] == {26, 27, 21, 15}, 'MainCase'] = 26
    df.loc[df['Cases'] == {21, 15, 28}, 'MainCase'] = 28
    
    # Map to the groups that have the same logic applied to them
    df['Group'] = df['MainCase'].map({1:1,
                                      2:2,
                                      3:3,
                                      4:1,
                                      5:4,
                                      6:5,
                                      7:5,
                                      8:4,
                                      9:2,
                                      10:6,
                                      11:2,
                                      13:1,
                                      14:2,
                                      15:7,
                                      19:2,
                                      20:7,
                                      21:7,
                                      22:2,
                                      23:2,
                                      24:2,
                                      25:2,
                                      26:1,
                                      27:2,
                                      28:2})
    
    return df

def __make_replacements(df):
    """
    Based on the 'Group' column, extracts the numeric parts of the 'Value' col
    and fills in three new columns 'NumericValue_New', 'Low_New' and 'High_New'
    of the parsed information, for updating the original columns where they
    are missing values.
    
    Parameters
    ----------
    df : pd.DataFrame
        The 'Likenumber' cases with groupings identified

    Returns
    -------
    df : pd.DataFrame
        The same frame, with new 'NumericValue_New', 'Low_New' and 'High_New'
        appended
    """
    df['Value'] = df['Value'].str.replace(',','')
    decimal_regex = r'\d+(\.\d+)?'
    
    df['Numbers'] = df['Value'].apply(lambda string: [x.group() for x in re.finditer(decimal_regex, string)])
    df['Numbers'] = df['Numbers'].apply(lambda x: sorted(x, reverse = True))
    
    number_of_values_by_group = {1:3,2:2,3:3,4:1,5:1,6:1,7:1}
    
    # Group 1: Write directly for all three vals
    group_number = 1
    df.loc[df['Group'] == group_number & df['High'].notna(), 'High'] = df.loc[df['Group'] == group_number & df['High'].notna(), 'Numbers'].apply(lambda x: x[0])
    df.loc[df['Group'] == group_number, 'NumericValue'] = df.loc[df['Group'] == group_number, 'Numbers'].apply(lambda x: x[1])
    df.loc[df['Group'] == group_number & df['Low'].notna(), 'Low'] = df.loc[df['Group'] == group_number & df['Low'].notna(), 'Numbers'].apply(lambda x: x[2])
    
    # Group 2: Write high and low, and average numeric
    group_number = 2
    df.loc[df['Group'] == group_number & df['High'].notna(), 'High'] = df.loc[df['Group'] == group_number & df['High'].notna(), 'Numbers'].apply(lambda x: x[0])
    df.loc[df['Group'] == group_number & df['Low'].notna(), 'Low'] = df.loc[df['Group'] == group_number & df['Low'].notna(), 'Numbers'].apply(lambda x: x[1])
    df.loc[df['Group'] == group_number, 'NumericValue'] = df.loc[df['Group'] == group_number][['Low','High']].mean(axis = 1)
    
    # Group 3: Write low and numeric only from 2 unique vals
    group_number = 3
    df.loc[df['Group'] == group_number & df['Low'].notna(), 'Low'] = df.loc[df['Group'] == group_number & df['Low'].notna(), 'Numbers'].apply(lambda x: x[len(x) - 1])
    df.loc[df['Group'] == group_number, 'NumericValue'] = df.loc[df['Group'] == group_number, 'Numbers'].apply(lambda x: x[0])
    
    # Group 4: Write low and numeric only from 1 unique val
    group_number = 4
    df.loc[df['Group'] == group_number & df['Low'].notna(), 'Low'] = df.loc[df['Group'] == group_number & df['Low'].notna(), 'Numbers'].apply(lambda x: x[0])
    df.loc[df['Group'] == group_number, 'NumericValue'] = df.loc[df['Group'] == group_number, 'Numbers'].apply(lambda x: x[0])
    
    # Group 5: Write numeric and high only from 1 unique val
    group_number = 5
    df.loc[df['Group'] == group_number & df['High'].notna(), 'High'] = df.loc[df['Group'] == group_number & df['High'].notna(), 'Numbers'].apply(lambda x: x[0])
    df.loc[df['Group'] == group_number, 'NumericValue'] = df.loc[df['Group'] == group_number, 'Numbers'].apply(lambda x: x[0])

    # Group 6: Write numeric only from 1 unique val
    group_number = 6
    df.loc[df['Group'] == group_number, 'NumericValue'] = df.loc[df['Group'] == group_number, 'Numbers'].apply(lambda x: x[0])

    # Group 7: Write low, numeric, and high from 1 unique val
    group_number = 7
    df.loc[df['Group'] == group_number & df['High'].notna(), 'High'] = df.loc[df['Group'] == group_number & df['High'].notna(), 'Numbers'].apply(lambda x: x[0])
    df.loc[df['Group'] == group_number, 'NumericValue'] = df.loc[df['Group'] == group_number, 'Numbers'].apply(lambda x: x[0])
    df.loc[df['Group'] == group_number & df['Low'].notna(), 'Low'] = df.loc[df['Group'] == group_number & df['Low'].notna(), 'Numbers'].apply(lambda x: x[0])

    return df

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
    original_cols = df.columns
    # Strip trailing and leading whitespace
    df['Value'] = df['Value'].str.strip()
    # Replace weird dashes
    df['Value'] = df['Value'].str.replace('–','-')
    
    
    df['Cases'] = df['Value'].apply(lambda x: __identify_cases(x))
    # Determine specific case, and group, based on some hacky logic
    df = __group_cases(df)
    
    # Update 'NumericVal', 'Low', 'High' based on the 'Group' column
    df = __make_replacements(df)
    
    # Drop the accumulated columns, return
    df = df[original_cols]
    return df
