from abc import ABC, abstractmethod

from liti.core.base import Defaulter, Validator
from liti.core.model.v1.datatype import Datatype
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.schema import Column, ColumnName, DatabaseName, Identifier, RoundingModeLiteral, \
    SchemaName, Table, \
    TableName


class DbBackend(ABC, Defaulter, Validator):
    """ DB backends make changes to and read the state of the database """

    @abstractmethod
    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        raise NotImplementedError('not supported')

    @abstractmethod
    def scan_table(self, name: TableName) -> CreateTable | None:
        raise NotImplementedError('not supported')

    @abstractmethod
    def has_table(self, name: TableName) -> bool:
        raise NotImplementedError('not supported')

    @abstractmethod
    def get_table(self, name: TableName) -> Table | None:
        raise NotImplementedError('not supported')

    @abstractmethod
    def create_table(self, table: Table):
        raise NotImplementedError('not supported')

    @abstractmethod
    def drop_table(self, name: TableName):
        raise NotImplementedError('not supported')

    @abstractmethod
    def rename_table(self, from_name: TableName, to_name: Identifier):
        raise NotImplementedError('not supported')

    def set_clustering(self, table_name: TableName, columns: list[ColumnName] | None):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_description(self, table_name: TableName, description: str | None):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_labels(self, table_name: TableName, labels: dict[str, str] | None):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_tags(self, table_name: TableName, tags: dict[str, str] | None):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_default_rounding_mode(self, table_name: TableName, rounding_mode: RoundingModeLiteral):
        raise NotImplementedError('not supported')

    @abstractmethod
    def add_column(self, table_name: TableName, column: Column):
        raise NotImplementedError('not supported')

    @abstractmethod
    def drop_column(self, table_name: TableName, column_name: ColumnName):
        raise NotImplementedError('not supported')

    @abstractmethod
    def rename_column(self, table_name: TableName, from_name: ColumnName, to_name: ColumnName):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_column_datatype(self, table_name: TableName, column_name: ColumnName, from_datatype: Datatype, to_datatype: Datatype):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_column_nullable(self, table_name: TableName, column_name: ColumnName, nullable: bool):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_column_description(self, table_name: TableName, column_name: ColumnName, description: str | None):
        raise NotImplementedError('not supported')

    @abstractmethod
    def set_column_rounding_mode(self, table_name: TableName, column_name: ColumnName, rounding_mode: RoundingModeLiteral):
        raise NotImplementedError('not supported')


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
