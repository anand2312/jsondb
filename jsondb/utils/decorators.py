"""Helper decorators."""
import typing
import jsondb.exceptions as exceptions

def check_datatype(many: bool):
    """Checks if data/filter to be inserted is a dictionary"""
    def wrapper(func):
        def inner_wrapper(self, _filter={}, _data=None, **kwargs):
            if _data is None:   # statements without two args - find, insert etc
                if many:  # statements that expect a list of dictionaries: insert_many
                    if isinstance(_filter, typing.Sequence):
                        return func(self, _filter, **kwargs)
                    else:
                        raise TypeError("Unexpected Datatype.")
                if isinstance(_filter, dict):
                    return func(self, _filter, **kwargs)
                else:
                    raise TypeError("Unexpected Datatype.")
            else: # update statements
                if isinstance(_filter, dict) and isinstance(_data, dict):
                    return func(self, _filter, _data, **kwargs)
                else:
                    raise TypeError("Unexpected  Datatype.")
        return inner_wrapper
    return wrapper

def not_closed(func):
    """Checks if the connection object isn't closed"""
    def wrapper(self, *args, **kwargs):
        if self._closed is True:
            raise exceptions.ClosedConnectionError("Database connection is already closed.")
        else:
            return func(self, *args, **kwargs)
    return wrapper
        