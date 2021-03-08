"""
 A collection of generic helper functions utilised in other stages of the 
 data pipeline. Usually they are object level operations applied in production
 with pandas .apply(lambda x: function(x))
 
 -----------------------------------
 Created on Wed Mar  3 10:59:00 2021
 @author: matthew.mcfahn
"""

import re

def __camel_to_snake(string):
    """Turn a CamelCase string to a snake_case string"""
    string = re.sub(r'(?<!^)(?=[A-Z])', '_', string).lower()
    return string

def __strip_string(value):
    """Simple helper - strip string, don't change numbers"""
    if not type(value) == str:
        pass
    else:
        value = value.replace(' ','').replace(',','').strip()
    return value

def __isnumber(value):
    """
    Helper function to indicate whether an object passed is a float in another format.
    
    Example cases of truth:
        > value is an integer / float data type
        > value = '1,000', then the float value is 1000
        > value = '1 000 000.00' then the float value is 1000000
    Example cases of false:
        > Anything else
    
    Parameters
    ----------
    value : mixed type
        Can be an int, float, string, or None type

    Returns
    -------
    x : Bool
        A bool indicating whether the object passed is actually a float
    """
    if value == None: # Deal with Null values
        x = False
    elif type(value) == int or type(value) == float: # Easy case, numbers
        x = True
    elif type(value) == str: # Find the strings that contain numbers
        test_val = value.replace(',','').replace(' ','') # We need to deal with a couple corner cases - these come from only a couple indicators
        try:
            float(test_val)
            x = True
        except ValueError:
            x =  False
    else:
        raise Exception('Incompatible data type. Review logic.')
    
    return x

def __makenumber(value):
    """
    Helper function to change the poorly formatted numbers to floats
    
    Examples:
        > value is an integer / float data type -> float type returned
        > value = '1,000', then the float value is 1000
        > value = '1 000 000.00' then the float value is 1000000
    
    Parameters
    ----------
    value : mixed type
        Can be an int, float, or string

    Returns
    -------
    number : Float
        The value object returned as a float
    """
    if type(value) == float:
        number = value
    elif type(value) == int:
        number = float(value)
    elif type(value) == str:
        number = float(value.replace(',','').replace(' ',''))
    else:
        raise Exception('Incompatible data type. Review logic.')
    
    return number

def __likenumber(value):
    """
    Identify which values are 'like' numbers - determined by:
        > They are a string containing a number, AND
        > They aren't one of the two corner cases identified in __makenumber()
    Parameters
    ----------
    value : mixed type
        An entry value of type int, float, string, null

    Returns
    -------
    x : Bool
        Is this object 'like' a number (contains a number & isn't an exception)

    """
    if value == None: # Deal with Null values
        x = False
    elif type(value) == int or type(value) == float: # Deal with number dtypes
        x = False
    elif type(value) == str: # Main logic - for strings
        
        x = any(char.isdigit() for char in value) # Does the string contain a number?
        if x: # If the string contains a number, check if it can be transformed into a number. If so, ignore
            test_val = value.replace(',','').replace(' ','') 
            try:
                float(test_val)
                x = False
            except ValueError:
                pass
    return x
