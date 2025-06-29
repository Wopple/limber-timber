from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.operation.base import Operation


class MemoryDbBackend(DbBackend):
    def __init__(self):
        self.tables = {}

    def get_table(self, table_name):
        return self.tables.get(table_name, None)

    def create_table(self, table):
        self.tables[table.name] = table

    def drop_table(self, table_name):
        del self.tables[table_name]


class MemoryMetaBackend(MetaBackend):
    def __init__(self, applied_operations: list[Operation] | None = None):
        self.applied_operations = applied_operations or []

    def get_applied_operations(self) -> list[Operation]:
        return self.applied_operations

    def apply_operation(self, operation: Operation):
        self.applied_operations.append(operation)

    def unapply_operation(self, operation: Operation) -> bool:
        if self.applied_operations[-1] == operation:
            self.applied_operations.pop()
            return True
        else:
            return False
