from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import Column, ColumnName, Table, TableName


class MemoryDbBackend(DbBackend):
    def __init__(self):
        self.tables = {}

    def get_table(self, name: TableName) -> Table | None:
        return self.tables.get(name)

    def create_table(self, table: Table):
        self.tables[table.name] = table.model_copy(deep=True)

    def drop_table(self, name: TableName):
        del self.tables[name]

    def rename_table(self, from_name: TableName, to_name: str):
        self.tables[from_name.with_table_name(to_name)] = self.tables.pop(from_name)

    def add_column(self, table_name: TableName, column: Column):
        self.get_table(table_name).columns.append(column.model_copy(deep=True))

    def drop_column(self, table_name: TableName, column_name: ColumnName):
        table = self.get_table(table_name)
        table.columns = [col for col in table.columns if col.name != column_name]

    def rename_column(self, table_name: TableName, from_name: ColumnName, to_name: ColumnName):
        table = self.get_table(table_name)
        table.columns = [col if col.name != from_name else col.with_name(to_name) for col in table.columns]


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
