from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from liti.core.model.v1.datatype import Array, Datatype, Struct
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.operation.data.view import CreateMaterializedView, CreateView
from liti.core.model.v1.schema import Column, ColumnName, ConstraintName, DatabaseName, FieldPath, ForeignKey, \
    Identifier, IntervalLiteral, MaterializedView, PrimaryKey, QualifiedName, Relation, RoundingMode, Schema, \
    SchemaName, StorageBilling, Table, View
from liti.core.observe.observer import Defaulter, Validator

CreateRelation = CreateTable | CreateView | CreateMaterializedView


class DbBackend(ABC, Defaulter, Validator):
    """ DB backends make changes to and read the state of the database """

    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        raise NotImplementedError('not supported')

    def scan_relation(self, name: QualifiedName) -> CreateRelation | None:
        raise NotImplementedError('not supported')

    def get_entity(self, name: QualifiedName) -> Schema | Relation | None:
        return self.get_schema(name) or self.get_relation(name)

    def get_relation(self, name: QualifiedName) -> Relation | None:
        if name.is_fully_qualified():
            return self.get_table(name) or self.get_view(name) or self.get_materialized_view(name)
        else:
            return None

    def has_schema(self, name: QualifiedName) -> bool:
        return self.get_schema(name) is not None

    def get_schema(self, name: QualifiedName) -> Schema | None:
        raise NotImplementedError('not supported')

    def create_schema(self, schema: Schema):
        raise NotImplementedError('not supported')

    def drop_schema(self, name: QualifiedName):
        raise NotImplementedError('not supported')

    def set_default_table_expiration(self, schema_name: QualifiedName, expiration: timedelta | None):
        raise NotImplementedError('not supported')

    def set_default_partition_expiration(self, schema_name: QualifiedName, expiration: timedelta | None):
        raise NotImplementedError('not supported')

    def set_default_kms_key_name(self, schema_name: QualifiedName, key_name: str | None):
        raise NotImplementedError('not supported')

    def set_failover_reservation(self, schema_name: QualifiedName, reservation: str | None):
        raise NotImplementedError('not supported')

    def set_case_sensitive(self, schema_name: QualifiedName, case_sensitive: bool):
        raise NotImplementedError('not supported')

    def set_is_primary_replica(self, schema_name: QualifiedName, is_primary: bool):
        raise NotImplementedError('not supported')

    def set_primary_replica(self, schema_name: QualifiedName, replica: str | None):
        raise NotImplementedError('not supported')

    def set_max_time_travel(self, schema_name: QualifiedName, duration: timedelta | None):
        raise NotImplementedError('not supported')

    def set_storage_billing(self, schema_name: QualifiedName, storage_billing: StorageBilling):
        raise NotImplementedError('not supported')

    def has_table(self, name: QualifiedName) -> bool:
        return self.get_table(name) is not None

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

    def drop_constraint(self, table_name: QualifiedName, constraint_name: ConstraintName):
        raise NotImplementedError('not supported')

    def set_partition_expiration(self, table_name: QualifiedName, expiration: timedelta | None):
        raise NotImplementedError('not supported')

    def set_require_partition_filter(self, table_name: QualifiedName, require_filter: bool):
        raise NotImplementedError('not supported')

    def set_clustering(self, table_name: QualifiedName, column_names: list[ColumnName] | None):
        raise NotImplementedError('not supported')

    def set_friendly_name(self, entity_name: QualifiedName, friendly_name: str | None):
        raise NotImplementedError('not supported')

    def set_description(self, entity_name: QualifiedName, description: str | None):
        raise NotImplementedError('not supported')

    def set_labels(self, entity_name: QualifiedName, labels: dict[str, str] | None):
        raise NotImplementedError('not supported')

    def set_tags(self, entity_name: QualifiedName, tags: dict[str, str] | None):
        raise NotImplementedError('not supported')

    def set_expiration_timestamp(self, entity_name: QualifiedName, expiration_timestamp: datetime | None):
        raise NotImplementedError('not supported')

    def set_default_rounding_mode(self, entity_name: QualifiedName, rounding_mode: RoundingMode | None):
        raise NotImplementedError('not supported')

    def set_max_staleness(self, entity_name: QualifiedName, max_staleness: IntervalLiteral | None):
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

        *path_fields, old_field = field_path.segments
        table = self.get_table(table_name)
        struct = extract_nested_datatype(table, FieldPath('.'.join(path_fields)))

        if isinstance(struct, Array):
            struct = struct.inner

        if isinstance(struct, Struct):
            if old_field in struct.fields:
                del struct.fields[old_field]
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
        return self.get_view(name) is not None

    def get_view(self, name: QualifiedName) -> View | None:
        raise NotImplementedError('not supported')

    def create_view(self, view: View):
        raise NotImplementedError('not supported')

    def drop_view(self, name: QualifiedName):
        raise NotImplementedError('not supported')

    def has_materialized_view(self, name: QualifiedName) -> bool:
        return self.get_materialized_view(name) is not None

    def get_materialized_view(self, name: QualifiedName) -> MaterializedView | None:
        raise NotImplementedError('not supported')

    def create_materialized_view(self, materialized_view: MaterializedView):
        raise NotImplementedError('not supported')

    def drop_materialized_view(self, name: QualifiedName):
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
