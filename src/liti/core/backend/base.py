from abc import ABC, abstractmethod
from datetime import datetime

from liti.core.base import Defaulter, Validator
from liti.core.model.v1.datatype import Array, Datatype, Struct
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.schema import Column, ColumnName, DatabaseName, FieldPath, ForeignKey, Identifier, \
    IntervalLiteral, PrimaryKey, RoundingMode, SchemaName, Table, QualifiedName, View


class DbBackend(ABC, Defaulter, Validator):
    """ DB backends make changes to and read the state of the database """

    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        raise NotImplementedError('not supported')

    def scan_table(self, name: QualifiedName) -> CreateTable | None:
        raise NotImplementedError('not supported')

    def has_table(self, name: QualifiedName) -> bool:
        raise NotImplementedError('not supported')

    def get_table(self, name: QualifiedName) -> Table | None:
        raise NotImplementedError('not supported')

    def create_table(self, table: Table):
        raise NotImplementedError('not supported')

    def drop_table(self, name: QualifiedName):
        raise NotImplementedError('not supported')

    def rename_table(self, from_name: QualifiedName, to_name: Identifier):
        raise NotImplementedError('not supported')

    def set_primary_key(self, table_name: QualifiedName, primary_key: PrimaryKey | None):
        raise NotImplementedError('not supported')

    def add_foreign_key(self, table_name: QualifiedName, foreign_key: ForeignKey):
        raise NotImplementedError('not supported')

    def drop_constraint(self, table_name: QualifiedName, constraint_name: Identifier):
        raise NotImplementedError('not supported')

    def set_partition_expiration(self, table_name: QualifiedName, expiration_days: float | None):
        raise NotImplementedError('not supported')

    def set_require_partition_filter(self, table_name: QualifiedName, require_filter: bool):
        raise NotImplementedError('not supported')

    def set_clustering(self, table_name: QualifiedName, column_names: list[ColumnName] | None):
        raise NotImplementedError('not supported')

    def set_description(self, table_name: QualifiedName, description: str | None):
        raise NotImplementedError('not supported')

    def set_labels(self, table_name: QualifiedName, labels: dict[str, str] | None):
        raise NotImplementedError('not supported')

    def set_tags(self, table_name: QualifiedName, tags: dict[str, str] | None):
        raise NotImplementedError('not supported')

    def set_expiration_timestamp(self, table_name: QualifiedName, expiration_timestamp: datetime | None):
        raise NotImplementedError('not supported')

    def set_default_rounding_mode(self, table_name: QualifiedName, rounding_mode: RoundingMode | None):
        raise NotImplementedError('not supported')

    def set_max_staleness(self, table_name: QualifiedName, max_staleness: IntervalLiteral | None):
        raise NotImplementedError('not supported')

    def set_enable_change_history(self, table_name: QualifiedName, enabled: bool):
        raise NotImplementedError('not supported')

    def set_enable_fine_grained_mutations(self, table_name: QualifiedName, enabled: bool):
        raise NotImplementedError('not supported')

    def set_kms_key_name(self, table_name: QualifiedName, key_name: str | None):
        raise NotImplementedError('not supported')

    def add_column(self, table_name: QualifiedName, column: Column):
        raise NotImplementedError('not supported')

    def drop_column(self, table_name: QualifiedName, column_name: ColumnName):
        raise NotImplementedError('not supported')

    def rename_column(self, table_name: QualifiedName, from_name: ColumnName, to_name: ColumnName):
        raise NotImplementedError('not supported')

    def set_column_datatype(self, table_name: QualifiedName, column_name: ColumnName, from_datatype: Datatype, to_datatype: Datatype):
        raise NotImplementedError('not supported')

    def add_column_field(self, table_name: QualifiedName, field_path: FieldPath, datatype: Datatype) -> Table:
        # circular imports
        from liti.core.function import extract_nested_datatype

        *path_fields, new_field = field_path.segments
        table = self.get_table(table_name)
        struct = extract_nested_datatype(table, FieldPath('.'.join(path_fields)))

        if isinstance(struct, Array):
            struct = struct.inner

        if isinstance(struct, Struct):
            if new_field not in struct.fields:
                struct.fields[new_field] = datatype
                return table
            else:
                raise ValueError(f'Field path {field_path} already exists in table {table_name}')
        else:
            raise ValueError(f'Expected struct datatype for {struct}')

    def drop_column_field(self, table_name: QualifiedName, field_path: FieldPath) -> Table:
        # circular imports
        from liti.core.function import extract_nested_datatype

        *path_fields, new_field = field_path.segments
        table = self.get_table(table_name)
        struct = extract_nested_datatype(table, FieldPath('.'.join(path_fields)))

        if isinstance(struct, Array):
            struct = struct.inner

        if isinstance(struct, Struct):
            if new_field in struct.fields:
                del struct.fields[new_field]
                return table
            else:
                raise ValueError(f'Field path {field_path} does not exist in table {table_name}')
        else:
            raise ValueError(f'Expected struct datatype for {struct}')

    def set_column_nullable(self, table_name: QualifiedName, column_name: ColumnName, nullable: bool):
        raise NotImplementedError('not supported')

    def set_column_description(self, table_name: QualifiedName, column_name: ColumnName, description: str | None):
        raise NotImplementedError('not supported')

    def set_column_rounding_mode(
        self,
        table_name: QualifiedName,
        column_name: ColumnName,
        rounding_mode: RoundingMode | None,
    ):
        raise NotImplementedError('not supported')

    def has_view(self, name: QualifiedName) -> bool:
        raise NotImplementedError('not supported')

    def get_view(self, name: QualifiedName) -> View | None:
        raise NotImplementedError('not supported')

    def create_or_replace_view(self, view: View):
        raise NotImplementedError('not supported')

    def drop_view(self, name: QualifiedName):
        raise NotImplementedError('not supported')

    def execute_sql(self, sql: str):
        raise NotImplementedError('not supported')

    def execute_bool_value_query(self, sql: str) -> bool:
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
