from enum import Enum


class UnsupportedError(Exception):
    """ Error raised when a migration fails due to not being supported by the backend """


class Unsupported(Enum):
    ADD_NON_NULLABLE_COLUMN = 'ADD_NON_NULLABLE_COLUMN'
    DROP_COLUMN_FIELD = 'DROP_COLUMN_FIELD'
    ENFORCE_PRIMARY_KEY = 'ENFORCE_PRIMARY_KEY'
    ENFORCE_FOREIGN_KEY = 'ENFORCE_FOREIGN_KEY'
    SET_COLUMN_DATATYPE = 'SET_COLUMN_DATATYPE'
