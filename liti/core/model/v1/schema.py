from string import ascii_letters, digits
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_serializer, field_validator, model_validator

from liti.core.model.v1.data_type import DataType, parse_data_type, serialize_data_type

DATABASE_CHARS = set(ascii_letters + digits + '_-')
IDENTIFIER_CHARS = set(ascii_letters + digits + '_')

type RoundingMode = Literal[
    'ROUNDING_MODE_UNSPECIFIED',
    'ROUND_HALF_AWAY_FROM_ZERO',
    'ROUND_HALF_EVEN',
]


class DatabaseName(BaseModel):
    model_config = ConfigDict(frozen=True)

    string: str

    def __init__(self, string: str | None = None, **kwargs):
        """ Allows DatabaseName('database') """
        if string is None:
            super().__init__(**kwargs)
        else:
            super().__init__(string=string)

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


class Identifier(BaseModel):
    model_config = ConfigDict(frozen=True)

    string: str

    def __init__(self, string: str | None = None, **kwargs):
        """ Allows Identifier('identifier') """
        if string is None:
            super().__init__(**kwargs)
        else:
            super().__init__(string=string)

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


class TableName(BaseModel):
    # Make hashable to use as a key
    model_config = ConfigDict(frozen=True)

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


class Partitioning(BaseModel):
    kind: Literal['time', 'int']
    column: ColumnName | None = None
    time_unit: Literal['year', 'month', 'day', 'hour'] | None = None
    int_start: int | None = None
    int_end: int | None = None
    int_step: int | None = None
    expiration_ms: int | None = None
    require_filter: bool = False

    def model_post_init(self, _context: Any):
        if self.kind == 'time':
            required = ['kind', 'time_unit', 'expiration_ms', 'require_filter']
            allowed = required + ['column']
        elif self.kind == 'int':
            required = ['kind', 'column', 'int_start', 'int_end', 'int_step', 'expiration_ms', 'require_filter']
            allowed = required
        else:
            raise ValueError(f'Invalid partitioning kind: {self.kind}')

        missing = [
            field_name
            for field_name in required
            if getattr(self, field_name) is None
        ]

        present = [
            field_name
            for field_name in Partitioning.model_fields.keys()
            if field_name not in allowed and getattr(self, field_name) is not None
        ]

        errors = [
            *[f'Missing required field for {self.kind}: {field_name}' for field_name in missing],
            *[f'Disallowed field present for {self.kind}: {field_name}' for field_name in present],
        ]

        if errors:
            raise ValueError('\n'.join(errors))


class Table(BaseModel):
    name: TableName
    columns: list[Column]
    partitioning: Partitioning | None = None
    clustering: list[ColumnName] | None = None

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns}
