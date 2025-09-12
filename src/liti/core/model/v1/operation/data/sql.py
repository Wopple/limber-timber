from typing import ClassVar

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import QualifiedName


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
    :param entity_names: mapping from python string format keys to fully qualified names
        SQL can be written as a python format string with named parameters
        the parameters will be replaced with the fully qualified names using str.format
        this field is provided to allow for templating tables, views, etc.
    """

    up: str
    down: str
    is_up: str | bool = False
    is_down: str | bool = False
    entity_names: dict[str, QualifiedName] | None = None

    KIND: ClassVar[str] = 'execute_sql'
