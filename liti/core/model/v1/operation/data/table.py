from typing import ClassVar

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import Column, ColumnName, Identifier, Table, TableName


class CreateTable(Operation):
    table: Table

    KIND: ClassVar[str] = 'create_table'


class DropTable(Operation):
    table_name: TableName

    KIND: ClassVar[str] = 'drop_table'


class RenameTable(Operation):
    from_name: TableName
    to_name: Identifier

    KIND: ClassVar[str] = 'rename_table'


class SetClustering(Operation):
    table_name: TableName
    columns: list[ColumnName]

    KIND: ClassVar[str] = 'set_clustering'


class AddColumn(Operation):
    table_name: TableName
    column: Column

    KIND: ClassVar[str] = 'add_column'


class DropColumn(Operation):
    table_name: TableName
    column_name: ColumnName

    KIND: ClassVar[str] = 'drop_column'


class RenameColumn(Operation):
    table_name: TableName
    from_name: ColumnName
    to_name: ColumnName

    KIND: ClassVar[str] = 'rename_column'
