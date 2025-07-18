from typing import Any, ClassVar

from pydantic import Field, field_validator

from liti.core.model.v1.datatype import Datatype, parse_datatype
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import Column, ColumnName, FieldPath, Identifier, RoundingModeLiteral, Table, \
    TableName


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


class SetDescription(Operation):
    table_name: TableName
    description: str | None = None

    KIND: ClassVar[str] = 'set_description'


class SetLabels(Operation):
    table_name: TableName
    labels: dict[str, str] | None = None

    KIND: ClassVar[str] = 'set_labels'


class SetTags(Operation):
    table_name: TableName
    tags: dict[str, str] | None = None

    KIND: ClassVar[str] = 'set_tags'


class SetDefaultRoundingMode(Operation):
    table_name: TableName
    rounding_mode: RoundingModeLiteral = Field(default_factory=RoundingModeLiteral)

    KIND: ClassVar[str] = 'set_default_rounding_mode'


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


class SetColumnDatatype(Operation):
    table_name: TableName
    column_name: ColumnName
    datatype: Datatype

    KIND: ClassVar[str] = 'set_column_datatype'

    @field_validator('datatype', mode='before')
    @classmethod
    def validate_datatype(cls, value: Datatype | str | dict[str, Any]) -> Datatype:
        return parse_datatype(value)


class AddColumnField(Operation):
    table_name: TableName
    field_path: FieldPath
    datatype: Datatype

    KIND: ClassVar[str] = 'add_column_field'

    @field_validator('datatype', mode='before')
    @classmethod
    def validate_datatype(cls, value: Datatype | str | dict[str, Any]) -> Datatype:
        return parse_datatype(value)


class DropColumnField(Operation):
    table_name: TableName
    field_path: FieldPath

    KIND: ClassVar[str] = 'drop_column_field'


class SetColumnNullable(Operation):
    table_name: TableName
    column_name: ColumnName
    nullable: bool

    KIND: ClassVar[str] = 'set_column_nullable'


class SetColumnDescription(Operation):
    table_name: TableName
    column_name: ColumnName
    description: str | None = None

    KIND: ClassVar[str] = 'set_column_description'


class SetColumnRoundingMode(Operation):
    table_name: TableName
    column_name: ColumnName
    rounding_mode: RoundingModeLiteral = Field(default_factory=RoundingModeLiteral)

    KIND: ClassVar[str] = 'set_column_rounding_mode'
