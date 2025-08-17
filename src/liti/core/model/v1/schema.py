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

RoundingModeLiteral = Literal[
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


class RoundingMode(LitiModel):
    string: RoundingModeLiteral

    def __init__(self, string: RoundingModeLiteral, **kwargs):
        """ Allows RoundingModeLiteral('rounding_mode') """
        if string is None:
            super().__init__(**kwargs)
        else:
            super().__init__(string=string)

    def __str__(self) -> str:
        return str(self.string)

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: RoundingModeLiteral | dict[str, RoundingModeLiteral]) -> dict[str, str]:
        if isinstance(data, str):
            return {'string': data}
        else:
            return data

    @field_validator('string', mode='before')
    @classmethod
    def validate_upper(cls, value: str) -> str:
        return value.upper()


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


class QualifiedName(LitiModel):
    database: DatabaseName
    schema: SchemaName
    name: Identifier

    def __init__(self, name: str | None = None, /, **kwargs):
        """ Allows QualifiedName('database.schema.table_name') """

        if name is None:
            super().__init__(**kwargs)
        else:
            database, schema, name = self.name_parts(name)

            super().__init__(
                database=database,
                schema=schema,
                name=name,
            )

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.database, self.schema, self.name))

    def __str__(self) -> str:
        return self.string

    @property
    def string(self) -> str:
        return f'{self.database}.{self.schema}.{self.name}'

    @classmethod
    def name_parts(cls, name: str) -> list[str]:
        parts = name.split('.')
        assert len(parts) == 3, f'Expected string in format "database.schema.table_name": "{name}"'
        return parts

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: str | dict[str, str]) -> dict[str, str]:
        if isinstance(data, str):
            database, schema, table_name = cls.name_parts(data)

            return {
                'database': database,
                'schema': schema,
                'table_name': table_name,
            }
        else:
            return data

    def with_name(self, name: Identifier) -> 'QualifiedName':
        return self.model_copy(update={'name': name})


class PrimaryKey(LitiModel):
    column_names: list[ColumnName] = Field(min_length=1)
    enforced: bool | None = None


class ForeignReference(LitiModel):
    local_column_name: ColumnName
    foreign_column_name: ColumnName


class ForeignKey(LitiModel):
    name: str | None = None
    foreign_table_name: QualifiedName
    references: list[ForeignReference] = Field(min_length=1)
    enforced: bool | None = None

    @model_validator(mode='after')
    def validate_model(self) -> 'ForeignKey':
        # TODO: only generate the name when writing to a backend to avoid untemplated names
        if not self.name:
            local_names = '_'.join(ref.local_column_name.string for ref in self.references)
            foreign_table = self.foreign_table_name.string.replace('.', '_').replace('-', '_')
            foreign_names = '_'.join(ref.foreign_column_name.string for ref in self.references)
            self.name = f'fk__{local_names}__{foreign_table}__{foreign_names}'

        return self

    @field_validator('name', mode='before')
    def validate_name(cls, value: str | None) -> str | None:
        """ Custom validation to handle backend generated foriegn key values like 'fk$1'

        If one of these values is provided, it will be replaced with a valid liti generated value.
        """
        if isinstance(value, str) and any(c not in IDENTIFIER_CHARS for c in value):
            return None  # will cause the model validation logic to generate a name
        else:
            return value


class Column(LitiModel):
    name: ColumnName
    datatype: Datatype | None = None
    default_expression: str | None = None
    nullable: bool = False
    description: str | None = None
    rounding_mode: RoundingMode | None = None

    def __init__(
        self,
        name: str | ColumnName,
        datatype: Datatype | None = None,
        default_expression: str | None = None,
        nullable: bool = False,
        description: str | None = None,
        rounding_mode: RoundingMode | None = None,
    ):
        """ Allows shorthand instantiation """

        name = ColumnName(name) if isinstance(name, str) else name

        super().__init__(
            name=name,
            datatype=datatype,
            default_expression=default_expression,
            nullable=nullable,
            description=description,
            rounding_mode=rounding_mode,
        )

    @field_validator('datatype', mode='before')
    @classmethod
    def validate_datatype(cls, value: Datatype | str | dict[str, Any] | None) -> Datatype | None:
        return value and parse_datatype(value)

    @field_serializer('datatype')
    @classmethod
    def serialize_datatype(cls, value: Datatype, info: FieldSerializationInfo) -> str | dict[str, Any]:
        # necessary to call the subclass serializer, otherwise pydantic uses Datatype
        return value.model_dump(exclude_none=info.exclude_none)

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


class BigLake(LitiModel):
    connection_id: str
    storage_uri: str
    file_format: Literal['PARQUET'] = 'PARQUET'
    table_format: Literal['ICEBERG'] = 'ICEBERG'


class Relation(LitiModel):
    name: QualifiedName
    columns: list[Column] | None = None
    primary_key: PrimaryKey | None = None
    foreign_keys: list[ForeignKey] | None = None
    partitioning: Partitioning | None = None
    clustering: list[ColumnName] | None = None
    friendly_name: str | None = None
    description: str | None = None
    labels: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    expiration_timestamp: datetime | None = None
    default_rounding_mode: RoundingMode | None = None
    max_staleness: IntervalLiteral | None = None
    enable_change_history: bool | None = None
    enable_fine_grained_mutations: bool | None = None
    kms_key_name: str | None = None
    big_lake: BigLake | None = None

    @model_validator(mode='after')
    def validate_model(self) -> 'Table':
        if self.foreign_keys:
            if len(self.foreign_keys) != len(set(fk.name for fk in self.foreign_keys)):
                raise ValueError('Foreign keys must have unique names')

        self.canonicalize()
        return self

    def canonicalize(self):
        # canonicalize for comparisons

        if self.foreign_keys:
            self.foreign_keys.sort(key=lambda fk: fk.name)

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns}

    @property
    def foreign_key_map(self) -> dict[Identifier, ForeignKey]:
        # Recreate the map to ensure it is up-to-date
        if self.foreign_keys:
            return {fk.name: fk for fk in self.foreign_keys}
        else:
            return {}

    def add_foreign_key(self, foreign_key: ForeignKey):
        self.foreign_keys.append(foreign_key)
        self.canonicalize()

    def drop_constraint(self, constraint_name: Identifier):
        self.foreign_keys = [
            fk for fk in self.foreign_keys if fk.name != constraint_name
        ]


class Table(Relation):
    DEFAULT_METHOD = 'table_defaults'


class View(Relation):
    select_sql: str | None = None
    select_file: str | None = None
    privacy_policy: dict[str, Any] | None = None

    DEFAULT_METHOD = 'view_defaults'
    VALIDATE_METHOD = 'validate_view'

    def __eq__(self, other):
        if not isinstance(other, View):
            return False

        return self.model_dump(exclude={'select_file'}) == other.model_dump(exclude={'select_file'})
