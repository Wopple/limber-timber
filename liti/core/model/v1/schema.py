import string
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_serializer, field_validator, model_validator

from liti.core.model.v1.data_type import DataType, parse_data_type, serialize_data_type

type DbName = str
type SchemaName = str
type ColumnName = str

type RoundingMode = Literal[
    'ROUNDING_MODE_UNSPECIFIED',
    'ROUND_HALF_AWAY_FROM_ZERO',
    'ROUND_HALF_EVEN',
]


IDENTIFIER_CHARS = set(string.ascii_letters + string.digits + '_')
DATABASE_CHARS = set(string.ascii_letters + string.digits + '_-')


def is_identifier(value: str) -> bool:
    return all(c in IDENTIFIER_CHARS for c in value)


def is_database(value: str) -> bool:
    return all(c in DATABASE_CHARS for c in value)


class TableName(BaseModel):
    # Make hashable to use as a key
    model_config = ConfigDict(frozen=True)

    database: str
    schema_name: str
    table_name: str

    def __init__(self, name: str | None = None, **kwargs):
        """ Allows TableName('database.schema_name.table_name') """

        if name is None:
            super().__init__(**kwargs)
        else:
            database, schema_name, table_name = self.name_parts(name)

            super().__init__(
                database=database,
                schema_name=schema_name,
                table_name=table_name,
            )

    def __str__(self) -> str:
        return self.string

    @property
    def string(self) -> str:
        return f'{self.database}.{self.schema_name}.{self.table_name}'

    def model_post_init(self, _context: Any):
        if not is_database(self.database):
            raise ValueError(f'Invalid database: {self.database}')

        if not is_identifier(self.schema_name):
            raise ValueError(f'Invalid schema: {self.schema_name}')

        if not is_identifier(self.table_name):
            raise ValueError(f'Invalid table name: {self.table_name}')

    @classmethod
    def name_parts(cls, name: str) -> list[str]:
        parts = name.split('.')
        assert len(parts) == 3, f'Expected string in format "database.schema_name.table_name": "{name}"'
        return parts

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: str | dict[str, str]) -> dict[str, str]:
        if isinstance(data, str):
            database, schema_name, table_name = cls.name_parts(data)

            return {
                'database': database,
                'schema_name': schema_name,
                'table_name': table_name,
            }
        else:
            return data

    def with_table_name(self, table_name: str) -> Self:
        return TableName(
            database=self.database,
            schema_name=self.schema_name,
            table_name=table_name,
        )


class ForeignKey(BaseModel):
    table_name: TableName
    column_name: ColumnName


class ColumnOptions(BaseModel):
    description: str | None = None
    rounding_mode: RoundingMode | None = None


class Column(BaseModel):
    name: ColumnName
    data_type: DataType
    primary_key: bool = False
    primary_enforced: bool = False
    foreign_key: ForeignKey | None = None
    foreign_enforced: bool = False
    default_expression: str | None = None
    nullable: bool = False
    options: ColumnOptions | None = None

    def model_post_init(self, _context: Any):
        if not is_identifier(self.name):
            raise ValueError(f'Invalid name: {self.name}')

    @field_validator('data_type', mode='before')
    @classmethod
    def validate_data_type(cls, value: DataType | str | list[Any] | dict[str, Any]):
        return parse_data_type(value)

    @field_serializer('data_type')
    @classmethod
    def serialize_data_type(cls, value: DataType) -> str:
        return serialize_data_type(value)

    def with_name(self, name: ColumnName) -> Self:
        return self.model_copy(update={'name': name})


class Table(BaseModel):
    name: TableName
    columns: list[Column]

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns}
