from datetime import datetime, timedelta
from string import ascii_letters, digits
from typing import Any, ClassVar, Iterator, Literal

from pydantic import Field, field_serializer, field_validator, model_serializer, model_validator, TypeAdapter
from pydantic_core.core_schema import FieldSerializationInfo

from liti.core.base import LitiModel
from liti.core.model.v1.datatype import Datatype, parse_datatype

DATABASE_CHARS = set(ascii_letters + digits + '_-')
IDENTIFIER_CHARS = set(ascii_letters + digits + '_')
CONSTRAINT_NAME_CHARS = set(ascii_letters + digits + '_$')
FIELD_PATH_CHARS = set(ascii_letters + digits + '_.')

RoundingModeLiteral = Literal[
    'ROUND_HALF_AWAY_FROM_ZERO',
    'ROUND_HALF_EVEN',
]

StorageBilling = Literal[
    'LOGICAL',
    'PHYSICAL',
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

    def __eq__(self, other) -> bool:
        return other is not None and self.string == other.string

    def __lt__(self, other) -> bool:
        return self.string < other.string

    def __le__(self, other) -> bool:
        return not self < other

    def __gt__(self, other) -> bool:
        return self.string > other.string

    def __ge__(self, other) -> bool:
        return not self > other

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


class ConstraintName(ValidatedString):
    VALID_CHARS = CONSTRAINT_NAME_CHARS

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
    database: DatabaseName | None = None
    schema_name: SchemaName | None = None
    name: Identifier | None = None

    def __init__(self, name: str | None = None, /, **kwargs):
        """ Allows QualifiedName('database.schema.table_name') """

        if name is None:
            super().__init__(**kwargs)
        else:
            database_part = None 
            schema_part = None 
            name_part = None 
            parts = self.name_parts(name)

            if len(parts) == 3:
                database_part, schema_part, name_part = parts
            elif len(parts) == 2:
                schema_part, name_part = parts
            elif len(parts) == 1:
                name_part = parts[0]

            super().__init__(
                database=database_part and DatabaseName(database_part),
                schema_name=schema_part and SchemaName(schema_part),
                name=name_part and Identifier(name_part),
            )

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.database, self.schema_name, self.name))

    def __str__(self) -> str:
        return self.string

    @property
    def string(self) -> str:
        parts = []

        if self.database:
            parts.append(self.database.string)

        if self.schema_name:
            parts.append(self.schema_name.string)

        if self.name:
            parts.append(self.name.string)

        return '.'.join(parts)

    @classmethod
    def name_parts(cls, name: str) -> list[str]:
        return name.split('.')

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(data, str):
            database_part = None
            schema_part = None
            name_part = None
            parts = cls.name_parts(data)

            if len(parts) == 3:
                database_part, schema_part, name_part = parts
            elif len(parts) == 2:
                schema_part, name_part = parts
            elif len(parts) == 1:
                name_part = parts[0]

            return {
                'database': database_part and DatabaseName(database_part),
                'schema_name': schema_part and SchemaName(schema_part),
                'name': name_part and Identifier(name_part),
            }
        else:
            return data

    def is_fully_qualified(self) -> bool:
        if self.database and self.schema_name and self.name:
            return True
        else:
            return False

    def is_schema(self) -> bool:
        if self.database and self.schema_name and self.name is None:
            return True
        else:
            return False

    def with_name(self, name: Identifier) -> 'QualifiedName':
        return self.model_copy(update={'name': name})


class PrimaryKey(LitiModel):
    column_names: list[ColumnName] = Field(min_length=1)
    enforced: bool | None = None


class ForeignReference(LitiModel):
    local_column_name: ColumnName
    foreign_column_name: ColumnName


class ForeignKey(LitiModel):
    name: ConstraintName | None = None
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
            self.name = ConstraintName(f'fk__{local_names}__{foreign_table}__{foreign_names}')

        return self

    @field_validator('name', mode='before')
    @classmethod
    def validate_name(cls, value: str | ConstraintName | None) -> ConstraintName | None:
        """ Custom validation to handle backend generated foreign key values like 'fk$1'

        If one of these values is provided, it will be replaced with a valid liti generated value.
        """

        if isinstance(value, str):
            if any(c not in IDENTIFIER_CHARS for c in value):
                # Returning None will cause the model validation logic to generate a name
                return None
            else:
                return ConstraintName(value)
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
    def serialize_datatype(cls, value: Datatype | None, info: FieldSerializationInfo) -> str | dict[str, Any] | None:
        # necessary to call the subclass serializer, otherwise pydantic uses Datatype
        return value and value.model_dump(exclude_none=info.exclude_none)

    def with_name(self, name: ColumnName) -> 'Column':
        return self.model_copy(update={'name': name})


class Partitioning(LitiModel):
    kind: Literal['TIME', 'INT']
    column: ColumnName | None = None
    column_datatype: Datatype | None = None
    time_unit: Literal['YEAR', 'MONTH', 'DAY', 'HOUR'] | None = None
    int_start: int | None = None
    int_end: int | None = None
    int_step: int | None = None
    expiration: timedelta | None = None
    require_filter: bool = False

    @field_validator('kind', 'time_unit', mode='before')
    @classmethod
    def validate_upper(cls, value: str | None) -> str | None:
        return value and value.upper()

    @field_validator('column_datatype', mode='before')
    @classmethod
    def validate_column_datatype(cls, value: Datatype | str | dict[str, Any] | None) -> Datatype | None:
        return value and parse_datatype(value)

    @field_serializer('column_datatype')
    @classmethod
    def serialize_column_datatype(cls, value: Datatype | None, info: FieldSerializationInfo) -> str | dict[str, Any] | None:
        # necessary to call the subclass serializer, otherwise pydantic uses Datatype
        return value and value.model_dump(exclude_none=info.exclude_none)

    @field_serializer('expiration')
    @classmethod
    def serialize_timedelta(cls, value: timedelta | None) -> str | None:
        return value and TypeAdapter(timedelta).dump_python(value, mode='json')


class BigLake(LitiModel):
    connection_id: str
    storage_uri: str
    file_format: Literal['PARQUET'] = 'PARQUET'
    table_format: Literal['ICEBERG'] = 'ICEBERG'


class Entity(LitiModel):
    name: QualifiedName
    friendly_name: str | None = None
    description: str | None = None
    labels: dict[str, str] | None = None
    tags: dict[str, str] | None = None


class Schema(Entity):
    location: str | None = None
    default_collate: str | None = None
    default_table_expiration: timedelta | None = None
    default_partition_expiration: timedelta | None = None
    default_rounding_mode: RoundingMode | None = None
    default_kms_key_name: str | None = None
    failover_reservation: str | None = None
    is_case_sensitive: bool | None = None
    is_primary_replica: bool | None = None
    primary_replica: str | None = None
    max_time_travel: timedelta | None = None
    storage_billing: StorageBilling | None = None

    @field_serializer('default_table_expiration', 'default_partition_expiration', 'max_time_travel')
    @classmethod
    def serialize_timedelta(cls, value: timedelta | None) -> str | None:
        return value and TypeAdapter(timedelta).dump_python(value, mode='json')


class Relation(Entity):
    expiration_timestamp: datetime | None = None


class Table(Relation):
    columns: list[Column] | None = None
    default_collate: str | None = None
    primary_key: PrimaryKey | None = None
    foreign_keys: list[ForeignKey] | None = None
    partitioning: Partitioning | None = None
    clustering: list[ColumnName] | None = None
    default_rounding_mode: RoundingMode | None = None
    max_staleness: IntervalLiteral | None = None
    enable_change_history: bool | None = None
    enable_fine_grained_mutations: bool | None = None
    kms_key_name: str | None = None
    big_lake: BigLake | None = None

    @model_validator(mode='after')
    def validate_model(self) -> 'Relation':
        if self.foreign_keys:
            if len(self.foreign_keys) != len(set(fk.name for fk in self.foreign_keys)):
                raise ValueError('Foreign keys must have unique names')

        self.canonicalize()
        return self

    def canonicalize(self):
        # canonicalize for comparisons

        if self.foreign_keys:
            self.foreign_keys.sort(key=lambda fk: fk.name)
        else:
            self.foreign_keys = None

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns or []}

    @property
    def foreign_key_map(self) -> dict[ConstraintName, ForeignKey]:
        # Recreate the map to ensure it is up-to-date
        if self.foreign_keys:
            return {fk.name: fk for fk in self.foreign_keys}
        else:
            return {}

    def add_foreign_key(self, foreign_key: ForeignKey):
        if self.foreign_keys:
            self.foreign_keys.append(foreign_key)
        else:
            self.foreign_keys = [foreign_key]

        self.canonicalize()

    def drop_constraint(self, constraint_name: ConstraintName):
        self.foreign_keys = [
            fk for fk in self.foreign_keys if fk.name != constraint_name
        ]

        self.canonicalize()


class ViewLike(LitiModel):
    select_sql: str | None = None
    select_file: str | None = None
    entity_names: dict[str, QualifiedName] | None = None

    @property
    def formatted_select_sql(self) -> str | None:
        if self.select_sql is not None:
            if self.entity_names:
                return self.select_sql.format(**self.entity_names)
            else:
                return self.select_sql
        else:
            return None


class View(Relation, ViewLike):
    columns: list[Column] | None = None
    privacy_policy: dict[str, Any] | None = None

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        exclude = {
            'privacy_policy',
            'select_file',
        }

        return self.model_dump(exclude=exclude) == other.model_dump(exclude=exclude)

    @property
    def column_map(self) -> dict[ColumnName, Column]:
        # Recreate the map to ensure it is up-to-date
        return {column.name: column for column in self.columns or []}


class MaterializedView(Relation, ViewLike):
    partitioning: Partitioning | None = None
    clustering: list[ColumnName] | None = None
    allow_non_incremental_definition: bool | None = None
    max_staleness: IntervalLiteral | None = None
    enable_refresh: bool | None = None
    refresh_interval: timedelta | None = None

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        exclude = {
            'select_file',
        }

        return self.model_dump(exclude=exclude) == other.model_dump(exclude=exclude)

    @field_serializer('refresh_interval')
    @classmethod
    def serialize_timedelta(cls, value: timedelta | None) -> str | None:
        return value and TypeAdapter(timedelta).dump_python(value, mode='json')
