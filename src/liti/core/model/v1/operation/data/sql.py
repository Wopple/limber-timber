from typing import ClassVar

from liti.core.model.v1.operation.data.base import Operation


class ExecuteSql(Operation):
    """ Run arbitrary SQL

    Only use this as a last resort.
    Limber Timber is not designed to be used this way.
    The primary use case is DML migrations what cannot be described by liti types.
    All fields are relative paths from the target directory to a SQL file.
    The paths are serialized to the metadata, not the SQL contents.
    It is highly recommended not to change the content of the SQL files, Limber Timber will not detect the changes.

    :param up: path to a SQL script to execute the up migration, must be an atomic operation
    :param down: path to a SQL script to execute the down migration, must be an atomic operation
    :param is_up: path to a SQL file with a boolean value query
        the query must return TRUE if the up migration has been applied
        the query must return FALSE if the up migration has not been applied
        TRUE and FALSE behave as if the query returned that value
    :param is_down: path to a SQL file with a boolean value query
        the query must return TRUE if the down migration has been applied
        the query must return FALSE if the down migration has not been applied
        TRUE and FALSE behave as if the query returned that value
    """

    up: str
    down: str
    is_up: str | bool = False
    is_down: str | bool = False

    KIND: ClassVar[str] = 'execute_sql'
