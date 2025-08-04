from typing import ClassVar

from pydantic import Field, field_validator

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import ColumnName, ForeignKey, Identifier, PrimaryKey, RoundingMode, Table, \
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


class SetPrimaryKey(Operation):
    table_name: TableName
    primary_key: PrimaryKey | None = None

    KIND: ClassVar[str] = 'set_primary_key'


class AddForeignKey(Operation):
    table_name: TableName
    foreign_key: ForeignKey

    KIND: ClassVar[str] = 'add_foreign_key'


class DropConstraint(Operation):
    table_name: TableName
    constraint_name: Identifier

    KIND: ClassVar[str] = 'drop_constraint'


class SetClustering(Operation):
    table_name: TableName
    column_names: list[ColumnName] | None = None

    KIND: ClassVar[str] = 'set_clustering'

    @field_validator('column_names', mode='before')
    @classmethod
    def validate_column_names(cls, value: list[ColumnName] | None) -> list[ColumnName] | None:
        if value:
            return value
        else:
            return None


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
    rounding_mode: RoundingMode | None = None

    KIND: ClassVar[str] = 'set_default_rounding_mode'
