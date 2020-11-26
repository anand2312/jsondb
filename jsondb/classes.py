import typing
import json
import contextlib

import jsondb.exceptions
import jsondb.utils.decorators as deco
from jsondb.utils.filter import _filter as filter_data

class QueryResult:
    """Represents a QueryResult object that will be returned on find, update, insert, delete operations."""
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        for i in self.args:
            yield i
    
    def __repr__(self) -> str:
        return f"<QueryResult; {', '.join([f'{key}: {value}' for key, value in self.kwargs.items()])}>"

class Connection:
    def __init__(self, filepath: str, **kwargs):
        """Makes a Connection object. `.json` file extension is not needed to be added, the program does it for you."""
        self.__fp = filepath + ".json"

        self._closed = False

        try:
            # Try opening file. If it exists, load it's data.
            with open(self.__fp, "r") as f:
                self._data = json.load(f)
        except Exception as e:
            # If file not found, create it by opening it in write mode.
            if isinstance(e, FileNotFoundError):
                with open(self.__fp, "w") as f:
                    pass
                self._data = []
            # If there's no data in the file, set data attribute to empty list.
            elif isinstance(e, json.decoder.JSONDecodeError):
                self._data = []

        try:
            # parsing through kwargs
            self.__indent = kwargs['indent']
        except KeyError:
            self.__indent = None 

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    @deco.check_datatype(many=False)
    @deco.not_closed
    def find(self, _filter: dict={}, **kwargs) -> QueryResult:
        if _filter == {}:
            matched_count = len(self._data)
            return QueryResult(*self._data, matched_count=matched_count)
        else:
            try:
                inverse = kwargs['inverse']
            except KeyError:
                inverse =  False
            matched = filter_data(_filter, self._data, inverse=inverse)
            matched_count = len(matched)
            return QueryResult(*matched, matched_count=matched_count)

    @deco.check_datatype(many=False)
    @deco.not_closed
    def find_one(self, _filter: dict={}, **kwargs) -> dict:
        if _filter == {}:
            return self._data[0]
        else:
            try:
                inverse = kwargs['inverse']
            except KeyError:
                inverse = False
            results = filter_data(_filter, self._data, inverse=inverse)
            return results[0]
    
    @deco.check_datatype(many=False)
    @deco.not_closed
    def insert_one(self, data: dict={}) -> QueryResult:
        if data == {}:
            raise jsondb.exceptions.EmptyInsertError("Cannot insert empty document.")

        self._data.append(data)
        return QueryResult(inserted_count=1)

    @deco.check_datatype(many=True)
    @deco.not_closed
    def insert_many(self, data: list=[]) -> QueryResult:
        if data in [[], {}, tuple()]:
            raise jsondb.exceptions.EmptyInsertError("Cannot insert empty document.")

        self._data.extend(data)
        return QueryResult(inserted_count=len(data))

    @deco.check_datatype(many=False)
    @deco.not_closed
    def update_one(self, _filter: dict, _update: dict, **kwargs) -> QueryResult:
        """The _update dict completely replaces the _filter document in the file."""
        match = self.find_one(_filter)

        for i, j in enumerate(self._data):
            if j == match:
                self._data[i] = _update
                return QueryResult(matched_count=1, updated_count=1)
        else:
            return QueryResult(matched_count=0, updated_count=0)
    
    @deco.check_datatype(many=True)
    @deco.not_closed
    def update_many(self, _filter: dict, _update: dict, **kwargs) -> str:
        match = filter_data(_filter, self._data)
        matched_count = len(match)
        updated_count = 0

        if matched_count == 0:
            return QueryResult(matched_count=0)

        for i, j in enumerate(self._data):
            if updated_count < matched_count:
                if j in match:
                    self._data[i] = _update
                    updated_count += 1
            else:
                return QueryResult(matched_count=matched_count, updated_count=updated_count)

    @deco.check_datatype(many=False)
    @deco.not_closed
    def delete_one(self, _filter: dict, **kwargs) -> QueryResult:
        match = filter_data(_filter, self._data)
        if match != ():
            self._data.remove(match[0])
            return QueryResult(deleted_count=1)
        else:
            return QueryResult(deleted_count=0)

    @deco.check_datatype(many=False)
    @deco.not_closed
    def delete_many(self, _filter: dict={}, **kwargs) -> QueryResult:
        deleted_count = 0
        if _filter == {}:
            deleted_count = len(self._data)
            self._data = []
            return QueryResult(deleted_count=deleted_count)    
        
        match = filter_data(_filter, self._data)
        matched_count = len(match)
        if matched_count == 0:
            return QueryResult(matched_count=0)

        for i, j in enumerate(self._data):
            if deleted_count <= matched_count:
                if j in match:
                    self._data.pop(i)
                    deleted_count += 1
            else:
                return QueryResult(deleted_count=deleted_count) 

    @deco.not_closed 
    def rollback(self) -> None:
        """Rolls back ALL changes till the last flushed changes."""
        with open(self.__fp, "r") as f:
            self._data = json.load(f)

    @deco.not_closed
    def commit(self) -> None:
        with open(self.__fp, "w") as f:
            json.dump(self._data, f)
    
    @deco.not_closed
    def close(self) -> None:
        self._closed = True
        with open(self.__fp, "w") as f:
            json.dump(self._data, f, indent=self.__indent)
