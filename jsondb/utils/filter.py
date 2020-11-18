import typing

def filter(check: dict, all_data: list, *, inverse: bool=False) -> tuple:
    """Filter through all data to return only the documents which match the check.
    `inverse` keyword-only argument is for those cases when we want to retrieve all documents which do NOT match the `check`."""
    def check(element: dict) -> bool:
        return all([True if i in element.items() else False for i in _filter.items()])

    results = filter(check, self._data)
    return tuple(results)