from datetime import datetime
from string import ascii_letters, digits
from typing import Any, Literal, Self

from pydantic import field_serializer, field_validator, model_validator

from liti.core.base import LitiModel
from liti.core.model.v1.datatype import DataType, parse_data_type, serialize_data_type

DATABASE_CHARS = set(ascii_letters + digits + '_-')
IDENTIFIER_CHARS = set(ascii_letters + digits + '_')

type RoundingMode = Literal[
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

    def __str__(self):
        from_str: str = next(filter(lambda x: x[0] != 0, [
            (self.year, 'YEAR'),
            (self.month, 'MONTH'),
            (self.day, 'DAY'),
            (self.hour, 'HOUR'),
            (self.minute, 'MINUTE'),
            (self.second, 'SECOND'),
            (self.microsecond, 'SECOND'),
        ]))[1]

        to_str: str = next(filter(lambda x: x[0] != 0, [
            (self.microsecond, 'MICROSECOND'),
            (self.second, 'SECOND'),
            (self.minute, 'MINUTE'),
            (self.hour, 'HOUR'),
            (self.day, 'DAY'),
            (self.month, 'MONTH'),
            (self.year, 'YEAR'),
        ]))[1]

        if self.year > 0:
            if to_str == 'YEAR':
                year_part = f'{self.year}'
            else:
                year_part = f'{self.year}-'
        else:
            year_part = ''

        if self.month > 0:
            if to_str == 'MONTH':
                month_part = f'{self.month}'
            else:
                month_part = f'{self.month} '
        else:
            month_part = ''

        if self.month > 0:
            if to_str == 'MONTH':
                month_part = f'{self.month}'
            else:
                month_part = f'{self.month} '
        else:
            month_part = ''

        if self.day > 0:
            if to_str == 'DAY':
                day_part = f'{self.day}'
            else:
                day_part = f'{self.day} '
        else:
            day_part = ''

        if self.hour > 0:
            if to_str == 'HOUR':
                hour_part = f'{self.hour}'
            else:
                hour_part = f'{self.hour}:'
        else:
            hour_part = ''

        if self.minute > 0:
            if to_str == 'MINUTE':
                minute_part = f'{self.minute}'
            else:
                minute_part = f'{self.minute}:'
        else:
            minute_part = ''

        if self.second > 0:
            if to_str == 'SECOND':
                second_part = f'{self.second}'
            else:
                second_part = f'{self.second}.'
        else:
            second_part = ''

        if self.microsecond > 0:
            microsecond_part = f'{self.microsecond}'
        else:
            microsecond_part = ''

        year_month_part = f'{year_part}{month_part}'
        time_part = f'{hour_part}{minute_part}{second_part}{microsecond_part}'
        sign_part = '-' if self.sign == '-' else ''

        if year_month_part:
            year_month_part = f'{sign_part}{year_month_part}'

        if day_part:
            day_part = f'{sign_part}{day_part}'

        if time_part:
            time_part = f'{sign_part}{time_part}'

        if to_str == 'MICROSECOND':
            to_str = 'SECOND'

        if from_str == to_str:
            range_part = from_str
        else:
            range_part = f'{from_str} TO {to_str}'

        return f'INTERVAL "{year_month_part}{day_part}{time_part}" {range_part}'

    @field_validator('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond', mode='before')
    @classmethod
    def validate_not_negative(cls, value: int):
        if value >= 0:
            return value
        else:
            raise ValueError(f'Interval values must be non-negative: {value}')


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


class Column(LitiModel):
    name: ColumnName
    data_type: DataType
    primary_key: bool = False
    primary_enforced: bool = False
    foreign_key: ForeignKey | None = None
    foreign_enforced: bool = False
    default_expression: str | None = None
    nullable: bool = False
    description: str | None = None
    rounding_mode: RoundingMode | None = None

    @field_validator('data_type', mode='before')
    @classmethod
    def validate_data_type(cls, value: DataType | str | dict[str, Any]) -> DataType:
        return parse_data_type(value)

    @field_serializer('data_type')
    @classmethod
    def serialize_data_type(cls, value: DataType) -> str | dict[str, Any]:
        return serialize_data_type(value)

    @field_validator('rounding_mode', mode='before')
    @classmethod
    def validate_upper(cls, value: str | None) -> str | None:
        return value and value.upper()

    def with_name(self, name: ColumnName) -> Self:
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
    partitioning: Partitioning | None = None
    clustering: list[ColumnName] | None = None
    friendly_name: str | None = None
    description: str | None = None
    labels: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    expiration_timestamp: datetime | None = None
    default_rounding_mode: RoundingMode | None = None

    # TODO validate milliseconds between 0 and 86400000 for big query
    max_staleness: IntervalLiteral | None = None

    enable_change_history: bool | None = None
    enable_fine_grained_mutations: bool | None = None
    kms_key_name: str | None = None
    storage_uri: str | None = None
    file_format: Literal['PARQUET'] | None = None
    table_format: Literal['ICEBERG'] | None = None

    DEFAULT_METHOD = 'table_defaults'

    @field_validator('default_rounding_mode', 'file_format', 'table_format', mode='before')
    @classmethod
    def validate_upper(cls, value: str | None) -> str | None:
        return value and value.upper()

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns}
