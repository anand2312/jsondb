import typing
import json

class Connection:
    def __init__(self, filepath: str, **kwargs):
        self._handle = open(filepath + ".json", "a+")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()
    
    def close(self):
        self._handle.close()

if __name__ == "__main__":
    pass