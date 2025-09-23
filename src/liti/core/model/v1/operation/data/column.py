from typing import Any, ClassVar

from pydantic import field_serializer, field_validator
from pydantic_core.core_schema import FieldSerializationInfo

from liti.core.model.v1.datatype import Datatype, parse_datatype
from liti.core.model.v1.operation.data.base import EntityKind, Operation
from liti.core.model.v1.schema import Column, ColumnName, FieldPath, QualifiedName, RoundingMode


class AddColumn(Operation):
    table_name: QualifiedName
    column: Column

    KIND: ClassVar[str] = 'add_column'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class DropColumn(Operation):
    table_name: QualifiedName
    column_name: ColumnName

    KIND: ClassVar[str] = 'drop_column'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class RenameColumn(Operation):
    table_name: QualifiedName
    from_name: ColumnName
    to_name: ColumnName

    KIND: ClassVar[str] = 'rename_column'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetColumnDatatype(Operation):
    table_name: QualifiedName
    column_name: ColumnName
    datatype: Datatype

    KIND: ClassVar[str] = 'set_column_datatype'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}

    @field_validator('datatype', mode='before')
    @classmethod
    def validate_datatype(cls, value: Datatype | str | dict[str, Any]) -> Datatype:
        return parse_datatype(value)

    @field_serializer('datatype')
    @classmethod
    def serialize_datatype(cls, value: Datatype, info: FieldSerializationInfo) -> str | dict[str, Any]:
        # necessary to call the subclass serializer, otherwise pydantic uses Datatype
        return value.model_dump(exclude_none=info.exclude_none)


class AddColumnField(Operation):
    table_name: QualifiedName
    field_path: FieldPath
    datatype: Datatype

    KIND: ClassVar[str] = 'add_column_field'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}

    @field_validator('datatype', mode='before')
    @classmethod
    def validate_datatype(cls, value: Datatype | str | dict[str, Any]) -> Datatype:
        return parse_datatype(value)

    @field_serializer('datatype')
    @classmethod
    def serialize_datatype(cls, value: Datatype, info: FieldSerializationInfo) -> str | dict[str, Any]:
        # necessary to call the subclass serializer, otherwise pydantic uses Datatype
        return value.model_dump(exclude_none=info.exclude_none)


class DropColumnField(Operation):
    table_name: QualifiedName
    field_path: FieldPath

    KIND: ClassVar[str] = 'drop_column_field'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetColumnNullable(Operation):
    table_name: QualifiedName
    column_name: ColumnName
    nullable: bool

    KIND: ClassVar[str] = 'set_column_nullable'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetColumnDescription(Operation):
    table_name: QualifiedName
    column_name: ColumnName
    description: str | None = None

    KIND: ClassVar[str] = 'set_column_description'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetColumnRoundingMode(Operation):
    table_name: QualifiedName
    column_name: ColumnName
    rounding_mode: RoundingMode | None = None

    KIND: ClassVar[str] = 'set_column_rounding_mode'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}
