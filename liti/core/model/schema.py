import string
from typing import Any

from pydantic.main import BaseModel, Field

from liti.core.model.data_type import DataType

type DbName = str
type SchemaName = str
type ColumnName = str


class TableName(BaseModel):
    database: str
    schema: str
    table: str

    def __str__(self) -> str:
        return self.str

    @property
    def str(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def model_post_init(self, _context: Any):
        chars = string.ascii_letters + string.digits + "_"
        database_chars = chars + "-"
        chars = set(chars)
        database_chars = set(database_chars)

        if any(c not in database_chars for c in self.database):
            raise ValueError(f"Invalid database: {self.database}")

        if any(c not in chars for c in self.schema):
            raise ValueError(f"Invalid schema: {self.schema}")

        if any(c not in chars for c in self.table):
            raise ValueError(f"Invalid table name: {self.table}")


class Column(BaseModel):
    name: ColumnName
    data_type: DataType
    nullable: bool = False

    def model_post_init(self, _context: Any):
        chars = set(string.ascii_letters + string.digits + "_")

        if any(c not in chars for c in self.name):
            raise ValueError(f"Invalid column name: {self.name}")


class Table(BaseModel):
    name: TableName
    columns: dict[ColumnName, Column]

    def model_post_init(self, _context: Any):
        for name, column in self.columns.items():
            if name != column.name:
                raise ValueError(f"Column name mismatch: {name} != {column.name}")
