import string
from typing import Any

from pydantic import BaseModel, ConfigDict, field_serializer, field_validator, model_validator

from liti.core.model.v1.data_type import BOOL, DataType, FLOAT64, INT64, STRING

type DbName = str
type SchemaName = str
type ColumnName = str


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
        return self.str

    @property
    def str(self) -> str:
        return f'{self.database}.{self.schema_name}.{self.table_name}'

    def model_post_init(self, _context: Any):
        chars = string.ascii_letters + string.digits + '_'
        database_chars = chars + '-'
        chars = set(chars)
        database_chars = set(database_chars)

        if any(c not in database_chars for c in self.database):
            raise ValueError(f'Invalid database: {self.database}')

        if any(c not in chars for c in self.schema_name):
            raise ValueError(f'Invalid schema: {self.schema_name}')

        if any(c not in chars for c in self.table_name):
            raise ValueError(f'Invalid table name: {self.table_name}')

    @classmethod
    def name_parts(cls, name: str) -> list[str]:
        parts = name.split('.')
        assert len(parts) == 3, f'Expected string in format "database.schema_name.table_name": "{name}"'
        return parts

    @model_validator(mode='before')
    @classmethod
    def allow_string_init(cls, data):
        if isinstance(data, str):
            database, schema_name, table_name = cls.name_parts(data)

            return {
                'database': database,
                'schema_name': schema_name,
                'table_name': table_name,
            }
        else:
            return data

class Column(BaseModel):
    name: ColumnName
    data_type: DataType
    nullable: bool = False

    def model_post_init(self, _context: Any):
        chars = set(string.ascii_letters + string.digits + '_')

        if any(c not in chars for c in self.name):
            raise ValueError(f'Invalid column name: {self.name}')

    @field_validator('data_type', mode='before')
    @classmethod
    def validate_data_type(cls, value):
        if isinstance(value, str):
            match value:
                case 'BOOL' | 'BOOLEAN': return BOOL
                case 'INT64': return INT64
                case 'FLOAT64': return FLOAT64
                case 'STRING': return STRING
                case _: raise ValueError(f'Cannot parse data type: {value}')
        else:
            return value

    @field_serializer('data_type')
    @classmethod
    def serialize_data_type(cls, value: DataType) -> str:
        if value == BOOL:
            return 'BOOL'
        elif value == INT64:
            return 'INT64'
        elif value == FLOAT64:
            return 'FLOAT64'
        elif value == STRING:
            return 'STRING'
        else:
            raise ValueError(f'Cannot serialize data type: {value}')

class Table(BaseModel):
    name: TableName
    columns: list[Column]

    def column_map(self) -> dict[ColumnName, Column]:
        if not hasattr(self, '_column_map'):
            self._column_map = {column.name: column for column in self.columns}

        return self._column_map
