from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.schema import Column, ColumnName, DatabaseName, Identifier, RoundingModeLiteral, \
    SchemaName, Table, \
    TableName


class MemoryDbBackend(DbBackend):
    def __init__(self):
        self.tables: dict[TableName, Table] = {}

    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        return [
            CreateTable(table=table)
            for name, table in self.tables.items()
            if name.database == database and name.schema_name == schema
        ]

    def scan_table(self, name: TableName) -> CreateTable | None:
        if name in self.tables:
            return CreateTable(table=self.tables[name])
        else:
            return None

    def has_table(self, name: TableName) -> bool:
        return name in self.tables

    def get_table(self, name: TableName) -> Table | None:
        return self.tables.get(name)

    def create_table(self, table: Table):
        self.tables[table.name] = table.model_copy(deep=True)

    def drop_table(self, name: TableName):
        del self.tables[name]

    def rename_table(self, from_name: TableName, to_name: Identifier):
        self.tables[from_name.with_table_name(to_name)] = self.tables.pop(from_name)

    def set_clustering(self, table_name: TableName, columns: list[ColumnName] | None):
        self.tables[table_name].clustering = columns

    def set_description(self, table_name: TableName, description: str | None):
        self.tables[table_name].description = description

    def set_default_rounding_mode(self, table_name: TableName, rounding_mode: RoundingModeLiteral):
        self.tables[table_name].default_rounding_mode = rounding_mode

    def add_column(self, table_name: TableName, column: Column):
        self.get_table(table_name).columns.append(column.model_copy(deep=True))

    def drop_column(self, table_name: TableName, column_name: ColumnName):
        table = self.get_table(table_name)
        table.columns = [col for col in table.columns if col.name != column_name]

    def rename_column(self, table_name: TableName, from_name: ColumnName, to_name: ColumnName):
        table = self.get_table(table_name)
        table.columns = [col if col.name != from_name else col.with_name(to_name) for col in table.columns]

    def set_column_description(self, table_name: TableName, column_name: ColumnName, description: str | None):
        table = self.get_table(table_name)
        column = table.column_map[column_name]
        column.description = description

    def set_column_rounding_mode(self, table_name: TableName, column_name: ColumnName, rounding_mode: RoundingModeLiteral):
        table = self.get_table(table_name)
        column = table.column_map[column_name]
        column.rounding_mode = rounding_mode


class MemoryMetaBackend(MetaBackend):
    def __init__(self, applied_operations: list[Operation] | None = None):
        self.applied_operations = applied_operations or []

    def get_applied_operations(self) -> list[Operation]:
        return self.applied_operations

    def apply_operation(self, operation: Operation):
        self.applied_operations.append(operation)

    def unapply_operation(self, operation: Operation):
        most_recent = self.applied_operations.pop()
        assert operation == most_recent, 'Expected the operation to be the most recent one'
