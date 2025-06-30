from abc import ABC, abstractmethod

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import Table, TableName


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
    def unapply_operation(self, operation: Operation) -> bool:
        """ Remove the operation from the metadata, returning True if it was successfully unapplied

        The operation must be the most recent one.
        """
        pass

    def get_migration_plan(self, target: list[Operation]) -> dict[str, list[Operation]]:
        applied = self.get_applied_operations()

        common_operations = 0

        for i, (applied_op, target_op) in enumerate(zip(applied, target)):
            if applied_op != target_op:
                common_operations = i
                break

        return {
            'down': applied[common_operations:],
            'up': target[common_operations:],
        }
