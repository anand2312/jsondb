import typing
from contextlib import suppress

import jsondb.exceptions

def _filter(condition: dict, all_data: list, *, inverse: bool=False) -> tuple:
    """Take any condition, redirect an appropriate filter from the functions defined below."""
    """Expected situations in the `conditions` dict:
    - Key doesn't start with a $ sign; meaning direct matching to be done
    - Key does start with a $ sign; specific filtering (lte/gte etc) to be done.

    When Key does start with a $ sign, it is a nested dictionary,
    like {"$lt":{<some key>: <some value>}}
    """
    
    for key, value in condition.items():
        if key[0] == "$":
            if key == "$lt":
                print('inside lt')
                all_data = _filter_lt(value, all_data, inverse=inverse)
            elif key == "$lte":
                print("inside lte")
                all_data = _filter_lte(value, all_data, inverse=inverse)
            elif key == "$gt":
                print('inside gt')
                all_data = _filter_lte(value, all_data, inverse=not inverse)
            elif key == "$gte":
                print('inside gte')
                all_data = _filter_lt(value, all_data, inverse=not inverse)
            else:
                raise jsondb.exceptions.UnsupportedOperatorError("Not a supported $ operator.")
        else:
            return _filter_direct_matching(condition, all_data, inverse=inverse)
    else:
        return all_data

def _filter_direct_matching(key: dict, all_data: list, *, inverse: bool=False) -> tuple:
    """Filter through all data to return only the documents which match the key.
    `inverse` keyword-only argument is for those cases when we want to retrieve all documents which do NOT match the `check`."""
    if not inverse:
        def check(element: dict) -> bool:
            return all([True if i in element.items() else False for i in key.items()])
    else:
        def check(element: dict) -> bool:
            return all([False if i in element.items() else True for i in key.items()])
    results = filter(check, all_data)
    return tuple(results)

def _filter_lte(key: dict, all_data: list, *, inverse: bool=False) -> tuple:
    """Filter through all data to return only documents where the specified `key` (of a dictionary)
    has a value _less than or equal to_ a specified value.
    `inverse` keyword-only argument is for those cases when we want to retreve all documents which do NOT match these criteria."""
    # expected `key` is of form {<key from dictionary>: value to be compared with}
    if not inverse:
        def check(element: dict) -> bool:
            with suppress(KeyError):
                return all([True if element[i] <= j else False for i, j in key.items()])
    else:
        # inverse=True with lte is same as `gt`
        def check(element: dict) -> bool:
            with suppress(KeyError):
                return all([False if element[i] <= j else True for i, j in key.items()])
        
    results = filter(check, all_data)
    return tuple(results)

def _filter_lt(key: dict, all_data: list, *, inverse: bool=False) -> tuple:
    """Filter through all data to return only documents where the specified `key` (of a dictionary)
    has a value _less than_ a specified value.
    `inverse` keyword-only argument is for those cases when we want to retreve all documents which do NOT match these criteria."""
    # expected `key` is of form {<key from dictionary>: value to be compared with}
    if not inverse:
        def check(element: dict) -> bool:
            with suppress(KeyError):
                return all([True if element[i] < j else False for i, j in key.items()])
    else:
        # inverse=True with lt is same as `gte`
        def check(element: dict) -> bool:
            with suppress(KeyError):
                return all([False if element[i] < j else True for i, j in key.items()])
        
    results = filter(check, all_data)
    return tuple(results)
