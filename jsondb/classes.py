import typing
import json
import contextlib

import jsondb.exceptions
import jsondb.utils.decorators as deco
import jsondb.utils.filter.filter as filter_data

class Connection:
    def __init__(self, filepath: str, **kwargs):
        """Makes a Connection object. `.json` file extension is not needed to be added, the program does it for you."""
        self.__fp = filepath + ".json"

        self.__closed = False

        with open(self.__fp, "r") as f:
            self._data = json.load(f)

        with contextlib.suppress(KeyError):
            self._id_start = kwargs['id']


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    @deco.check_datatype(many=False)
    @deco.not_closed
    def find(self, _filter: dict={}, **kwargs) -> tuple:
        if _filter == {}:
            return tuple(self._data)
        else:
            with contextlib.suppress(KeyError):
                inverse = kwargs['inverse']
            return filter_data(_filter, self._data, inverse)

    @deco.check_datatype(many=False)
    @deco.not_closed
    def find_one(self, _filter: dict={}, **kwargs) -> dict:
        if _filter == {}:
            return self._data[0]
        else:
            with contextlib.suppress(KeyError):
                inverse = kwargs['inverse']

            results = filter_data(_filter, self._data, inverse=inverse)
            return results[0]
    
    @deco.check_datatype(many=False)
    @deco.not_closed
    def insert_one(self, data: dict) -> str:
        if data == {}:
            raise jsondb.exceptions.EmptyInsertError

        self._data.append(data)
        return "Query OK; 1 document inserted"

    @deco.check_datatype(many=True)
    @deco.not_closed
    def insert_many(self, data: list) -> str:
        if data in [[], {}, tuple()]:
            raise jsondb.exceptions.EmptyInsertError
        
        self._data.extend(data)
        return f"Query OK; {len(data)} documents inserted"

    @deco.check_datatype(many=False)
    @deco.not_closed
    def update_one(self, _filter: dict, _update: dict, **kwargs) -> str:
        """The _update dict completely replaces the _filter document in the file."""
        match = self.find_one(_filter)

        for i in self._data:
            if i == match:
                i = _update
                break
        
        return "Query OK; 1 row updated."

    @deco.check_datatype(many=True)
    @deco.not_closed
    def update_many(self, _filter: dict, _update: dict, **kwargs) -> str:
        match = filter_data(_filter, self._data)
        updated_count = 0
        for i in self._data:
            if i in match:
                i = _update
                updated_count += 1
        return f"Query OK; updated count: {updated_count}"


    @deco.not_closed 
    def rollback(self) -> None:
        """Rolls back ALL changes till the last flushed changes."""
        with open(self.__fp, "r") as f:
            self._data = json.load(f)

    @deco.not_closed
    def flush(self) -> None:
        with open(self.__fp, "w") as f:
            json.dump(self._data, f)
    
    @deco.not_closed
    def close(self) -> None:
        self._closed = True
        with open(self.__fp, "w") as f:
            json.dump(self._data, f)
    
    def __check_data_integrity(self) -> bool:
        # Will I ever use this.
        with open(self.__fp) as f:
            return self._data == json.load(f)

if __name__ == "__main__":
    pass