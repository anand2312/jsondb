import typing

def _filter(key: dict, all_data: list, *, inverse: bool=False) -> tuple:
    """Filter through all data to return only the documents which match the key.
    `inverse` keyword-only argument is for those cases when we want to retrieve all documents which do NOT match the `check`."""
    if key == {}:
        return all_data
    def check(element: dict) -> bool:
        return all([True if i in element.items() else False for i in key.items()])

    results = filter(check, all_data)
    return tuple(results)