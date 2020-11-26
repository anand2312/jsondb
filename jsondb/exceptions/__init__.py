"""Exception classes"""

class EmptyInsertError(Exception):
    pass

class ClosedConnectionError(Exception):
    pass

class UnsupportedOperatorError(TypeError):
    pass
