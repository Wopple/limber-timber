from abc import ABC, abstractmethod

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import Column, ColumnName, Table, TableName


class DbBackend(ABC):
    """ DB backends make changes to and read the state of the database """

    @abstractmethod
    def get_table(self, name: TableName) -> Table | None:
        pass

    @abstractmethod
    def create_table(self, table: Table):
        pass

    @abstractmethod
    def drop_table(self, name: TableName):
        pass

    @abstractmethod
    def rename_table(self, from_name: TableName, to_name: str):
        pass

    @abstractmethod
    def add_column(self, table_name: TableName, column: Column):
        pass

    @abstractmethod
    def drop_column(self, table_name: TableName, column_name: ColumnName):
        pass

    @abstractmethod
    def rename_column(self, table_name: TableName, from_name: ColumnName, to_name: ColumnName):
        pass


class MetaBackend(ABC):
    """ Meta backends manage the state of what migrations have been applied """

    def initialize(self):
        pass

    @abstractmethod
    def get_applied_operations(self) -> list[Operation]:
        pass

    @abstractmethod
    def apply_operation(self, operation: Operation):
        """ Add the operation to the metadata """
        pass

    @abstractmethod
    def unapply_operation(self, operation: Operation):
        """ Remove the operation from the metadata

        The operation must be the most recent one.
        """
        pass

    def get_previous_operations(self) -> list[Operation]:
        return self.get_applied_operations()[:-1]

    def get_migration_plan(self, target: list[Operation]) -> dict[str, list[Operation]]:
        applied = self.get_applied_operations()
        common_operations = 0

        for applied_op, target_op in zip(applied, target):
            if applied_op == target_op:
                common_operations += 1
            else:
                break

        return {
            'down': list(reversed(applied[common_operations:])),
            'up': target[common_operations:],
        }
