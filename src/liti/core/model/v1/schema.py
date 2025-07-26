from datetime import datetime
from string import ascii_letters, digits
from typing import Any, ClassVar, Iterator, Literal

from pydantic import Field, field_serializer, field_validator, model_serializer, model_validator
from pydantic_core.core_schema import FieldSerializationInfo

from liti.core.base import LitiModel
from liti.core.model.v1.datatype import Datatype, parse_datatype

DATABASE_CHARS = set(ascii_letters + digits + '_-')
IDENTIFIER_CHARS = set(ascii_letters + digits + '_')
FIELD_PATH_CHARS = set(ascii_letters + digits + '_.')

RoundingMode = Literal[
    'ROUND_HALF_AWAY_FROM_ZERO',
    'ROUND_HALF_EVEN',
]


class IntervalLiteral(LitiModel):
    year: int = 0
    month: int = 0
    day: int = 0
    hour: int = 0
    minute: int = 0
    second: int = 0
    microsecond: int = 0
    sign: Literal['+', '-'] = '+'

    @field_validator('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond', mode='before')
    @classmethod
    def validate_not_negative(cls, value: int):
        if value >= 0:
            return value
        else:
            raise ValueError(f'Interval values must be non-negative: {value}')


class RoundingModeLiteral(LitiModel):
    string: RoundingMode | None = None

    DEFAULT_METHOD: ClassVar[str] = 'rounding_mode_defaults'

    def __init__(self, string: RoundingMode | None = None, **kwargs):
        """ Allows RoundingModeLiteral('rounding_mode') """
        if string is None:
            super().__init__(**kwargs)
        else:
            super().__init__(string=string)

    def __str__(self) -> str:
        return str(self.string)

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: RoundingMode | dict[str, RoundingMode]) -> dict[str, str]:
        if isinstance(data, str):
            return {'string': data}
        else:
            return data

    @field_validator('string', mode='before')
    @classmethod
    def validate_upper(cls, value: str | None) -> str | None:
        return value and value.upper()


class ValidatedString(LitiModel):
    string: str

    VALID_CHARS: ClassVar[set[str]]

    def __init__(self, string: str | None = None, **kwargs):
        """ Allows ValidatedString('value') """
        if string is None:
            super().__init__(**kwargs)
        else:
            super().__init__(string=string)

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.string))

    def __str__(self) -> str:
        return self.string

    def model_post_init(self, context: Any):
        if any(c not in self.VALID_CHARS for c in self.string):
            raise ValueError(f'Invalid {self.__class__.__name__}: {self.string}')

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: str | dict[str, str]) -> dict[str, str]:
        if isinstance(data, str):
            return {'string': data}
        else:
            return data

    @model_serializer
    def serialize(self) -> str:
        return self.string


class DatabaseName(ValidatedString):
    VALID_CHARS = DATABASE_CHARS

    def __init__(self, string: str | None = None, **kwargs):
        super().__init__(string, **kwargs)


class Identifier(ValidatedString):
    VALID_CHARS = IDENTIFIER_CHARS

    def __init__(self, string: str | None = None, **kwargs):
        super().__init__(string, **kwargs)


class FieldPath(ValidatedString):
    """ . delimited path to the field (e.g. 'column_name.sub_field_1.sub_field_2') """
    VALID_CHARS = FIELD_PATH_CHARS

    def __init__(self, string: str | None = None, **kwargs):
        super().__init__(string, **kwargs)

    def __iter__(self) -> Iterator[str]:
        return iter(self.segments)

    @property
    def segments(self) -> list[str]:
        return self.string.split('.')


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

    def __hash__(self) -> int:
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

    def with_table_name(self, table_name: Identifier) -> 'TableName':
        return TableName(
            database=self.database,
            schema_name=self.schema_name,
            table_name=table_name,
        )


class PrimaryKey(LitiModel):
    column_names: list[ColumnName]
    enforced: bool | None = None

    @field_validator('column_names', mode='before')
    @classmethod
    def validate_column_names(cls, value: list[ColumnName]) -> list[ColumnName]:
        if len(value) == 0:
            raise ValueError('A primary key\'s column_names must not be empty')

        return value


class ForeignKey(LitiModel):
    name: str | None = None
    local_column_names: list[ColumnName]
    foreign_table_name: TableName
    foreign_column_names: list[ColumnName]
    enforced: bool | None = None

    @model_validator(mode='after')
    def validate_model(self) -> 'ForeignKey':
        if not self.name:
            local_names = '_'.join(col.string for col in self.local_column_names)
            foreign_table = f'{self.database}_{self.schema_name}_{self.table_name}'
            foreign_names = '_'.join(col.string for col in self.foreign_column_names)
            self.name = f'fk__{local_names}__{foreign_table}__{foreign_names}'

        if len(self.local_column_names) != len(self.foreign_column_names):
            raise ValueError(
                f'A foreign key must have the same number of local and foreign column names:'
                f' {len(self.local_column_names)} != {len(self.foreign_column_names)}'
            )

        return self

    @field_validator('local_column_names', mode='before')
    @classmethod
    def validate_local_column_names(cls, value: list[ColumnName]) -> list[ColumnName]:
        if len(value) == 0:
            raise ValueError('A foreign key\'s local_column_names must not be empty')

        return value

    @field_validator('foreign_column_names', mode='before')
    @classmethod
    def validate_foreign_column_names(cls, value: list[ColumnName]) -> list[ColumnName]:
        if len(value) == 0:
            raise ValueError('A foreign key\'s foreign_column_names must not be empty')

        return value


class Column(LitiModel):
    name: ColumnName
    datatype: Datatype
    default_expression: str | None = None
    nullable: bool = False
    description: str | None = None
    rounding_mode: RoundingModeLiteral | None = None

    @field_validator('datatype', mode='before')
    @classmethod
    def validate_datatype(cls, value: Datatype | str | dict[str, Any]) -> Datatype:
        return parse_datatype(value)

    @field_serializer('datatype')
    @classmethod
    def serialize_datatype(cls, value: Datatype, info: FieldSerializationInfo) -> str | dict[str, Any]:
        # necessary to call the subclass serializer, otherwise pydantic uses Datatype
        return value.model_dump(
            exclude_defaults=info.exclude_defaults,
            exclude_none=info.exclude_none,
        )

    def with_name(self, name: ColumnName) -> 'Column':
        return self.model_copy(update={'name': name})


class Partitioning(LitiModel):
    kind: Literal['TIME', 'INT']
    column: ColumnName | None = None
    time_unit: Literal['YEAR', 'MONTH', 'DAY', 'HOUR'] | None = None
    int_start: int | None = None
    int_end: int | None = None
    int_step: int | None = None
    expiration_days: float | None = None
    require_filter: bool = False

    DEFAULT_METHOD = 'partitioning_defaults'
    VALIDATE_METHOD = 'validate_partitioning'

    @field_validator('kind', 'time_unit', mode='before')
    @classmethod
    def validate_upper(cls, value: str | None) -> str | None:
        return value and value.upper()


class Table(LitiModel):
    name: TableName
    columns: list[Column]
    primary_key: PrimaryKey | None = None
    foreign_keys: list[ForeignKey] | None = None
    partitioning: Partitioning | None = None
    clustering: list[ColumnName] | None = None
    friendly_name: str | None = None
    description: str | None = None
    labels: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    expiration_timestamp: datetime | None = None
    default_rounding_mode: RoundingModeLiteral = Field(default_factory=RoundingModeLiteral)
    max_staleness: IntervalLiteral | None = None
    enable_change_history: bool | None = None
    enable_fine_grained_mutations: bool | None = None
    kms_key_name: str | None = None
    connection_name: str | None = None
    storage_uri: str | None = None
    file_format: Literal['PARQUET'] | None = None
    table_format: Literal['ICEBERG'] | None = None

    DEFAULT_METHOD = 'table_defaults'

    @model_validator(mode='after')
    def validate_model(self) -> 'Table':
        # canonicalize
        if self.foreign_keys:
            self.foreign_keys.sort(key=lambda fk: fk.name)

        return self

    @field_validator('file_format', 'table_format', mode='before')
    @classmethod
    def validate_upper(cls, value: str | None) -> str | None:
        return value and value.upper()

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns}
