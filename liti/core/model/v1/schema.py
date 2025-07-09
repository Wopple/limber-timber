from string import ascii_letters, digits
from typing import Any, Literal, Self

from pydantic import field_serializer, field_validator, model_validator

from liti.core.base import LitiModel
from liti.core.model.v1.data_type import DataType, parse_data_type, serialize_data_type

DATABASE_CHARS = set(ascii_letters + digits + '_-')
IDENTIFIER_CHARS = set(ascii_letters + digits + '_')

type RoundingMode = Literal[
    'ROUNDING_MODE_UNSPECIFIED',
    'ROUND_HALF_AWAY_FROM_ZERO',
    'ROUND_HALF_EVEN',
]


class DatabaseName(LitiModel):
    string: str

    def __init__(self, string: str | None = None, **kwargs):
        """ Allows DatabaseName('database') """
        if string is None:
            super().__init__(**kwargs)
        else:
            super().__init__(string=string)

    def __hash__(self):
        return hash((self.__class__.__name__, self.string))

    def __str__(self) -> str:
        return self.string

    def model_post_init(self, _context: Any):
        if any(c not in DATABASE_CHARS for c in self.string):
            raise ValueError(f'Invalid database name: {self.string}')

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: str | dict[str, str]) -> dict[str, str]:
        if isinstance(data, str):
            return {'string': data}
        else:
            return data


class Identifier(LitiModel):
    string: str

    def __init__(self, string: str | None = None, **kwargs):
        """ Allows Identifier('identifier') """
        if string is None:
            super().__init__(**kwargs)
        else:
            super().__init__(string=string)

    def __hash__(self):
        return hash((self.__class__.__name__, self.string))

    def __str__(self) -> str:
        return self.string

    def model_post_init(self, _context: Any):
        if any(c not in IDENTIFIER_CHARS for c in self.string):
            raise ValueError(f'Invalid identifier: {self.string}')

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: str | dict[str, str]) -> dict[str, str]:
        if isinstance(data, str):
            return {'string': data}
        else:
            return data


class SchemaName(Identifier):
    def __init__(self, string: str | None = None, **kwargs):
        super().__init__(string, **kwargs)


class ColumnName(Identifier):
    def __init__(self, string: str | None = None, **kwargs):
        super().__init__(string, **kwargs)


class TableName(LitiModel):
    database: DatabaseName
    schema_name: SchemaName
    table_name: Identifier

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

    def __hash__(self):
        return hash((self.__class__.__name__, self.database, self.schema_name, self.table_name))

    def __str__(self) -> str:
        return self.string

    @property
    def string(self) -> str:
        return f'{self.database}.{self.schema_name}.{self.table_name}'

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

    def with_table_name(self, table_name: Identifier) -> Self:
        return TableName(
            database=self.database,
            schema_name=self.schema_name,
            table_name=table_name,
        )


class ForeignKey(LitiModel):
    table_name: TableName
    column_name: ColumnName


class ColumnOptions(LitiModel):
    description: str | None = None
    rounding_mode: RoundingMode | None = None

    @field_validator('rounding_mode', mode='before')
    @classmethod
    def validate_rounding_mode(cls, value: str | None) -> str | None:
        return value and value.upper()


class Column(LitiModel):
    name: ColumnName
    data_type: DataType
    primary_key: bool = False
    primary_enforced: bool = False
    foreign_key: ForeignKey | None = None
    foreign_enforced: bool = False
    default_expression: str | None = None
    nullable: bool = False
    options: ColumnOptions | None = None

    @field_validator('data_type', mode='before')
    @classmethod
    def validate_data_type(cls, value: DataType | str | dict[str, Any]) -> DataType:
        return parse_data_type(value)

    @field_serializer('data_type')
    @classmethod
    def serialize_data_type(cls, value: DataType) -> str | dict[str, Any]:
        return serialize_data_type(value)

    def with_name(self, name: ColumnName) -> Self:
        return self.model_copy(update={'name': name})


class Partitioning(LitiModel):
    kind: Literal['TIME', 'INT']
    column: ColumnName | None = None
    time_unit: Literal['YEAR', 'MONTH', 'DAY', 'HOUR'] | None = None
    int_start: int | None = None
    int_end: int | None = None
    int_step: int | None = None
    expiration_ms: int | None = None
    require_filter: bool = False

    VALIDATE_METHOD = 'validate_partitioning'

    @field_validator('kind', mode='before')
    @classmethod
    def validate_kind(cls, value: str | None) -> str | None:
        return value and value.upper()

    @field_validator('time_unit', mode='before')
    @classmethod
    def validate_time_unit(cls, value: str | None) -> str | None:
        return value and value.upper()


class Table(LitiModel):
    name: TableName
    columns: list[Column]
    partitioning: Partitioning | None = None
    clustering: list[ColumnName] | None = None

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns}
