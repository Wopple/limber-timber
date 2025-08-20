from datetime import datetime

from liti.core.backend.base import CreateRelation, DbBackend, MetaBackend
from liti.core.model.v1.datatype import Datatype
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.operation.data.view import CreateOrReplaceMaterializedView, CreateOrReplaceView
from liti.core.model.v1.schema import Column, ColumnName, DatabaseName, ForeignKey, Identifier, IntervalLiteral, \
    MaterializedView, PrimaryKey, RoundingMode, SchemaName, Table, QualifiedName, View


class MemoryDbBackend(DbBackend):
    def __init__(self):
        self.tables: dict[QualifiedName, Table] = {}
        self.views: dict[QualifiedName, View] = {}
        self.materialized_views: dict[QualifiedName, MaterializedView] = {}

    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        tables = [
            CreateTable(table=table)
            for name, table in self.tables.items()
            if name.database == database and name.schema == schema
        ]

        views = [
            CreateOrReplaceView(view=view)
            for name, view in self.views.items()
            if name.database == database and name.schema == schema
        ]

        materialized_views = [
            CreateOrReplaceMaterializedView(materialized_view=materialized_view)
            for name, materialized_view in self.materialized_views.items()
            if name.database == database and name.schema == schema
        ]

        return tables + materialized_views + views

    def scan_relation(self, name: QualifiedName) -> CreateRelation | None:
        if name in self.tables:
            return CreateTable(table=self.tables[name])
        if name in self.views:
            return CreateOrReplaceView(view=self.views[name])
        if name in self.materialized_views:
            return CreateOrReplaceMaterializedView(materialized_view=self.materialized_views[name])
        else:
            return None

    def has_table(self, name: QualifiedName) -> bool:
        return name in self.tables

    def get_table(self, name: QualifiedName) -> Table | None:
        return self.tables.get(name)

    def create_table(self, table: Table):
        if table.name in self.tables:
            raise ValueError(f'Table {table.name} already exists')

        self.tables[table.name] = table.model_copy(deep=True)

    def drop_table(self, name: QualifiedName):
        if name not in self.tables:
            raise ValueError(f'Table {name} does not exist')

        del self.tables[name]

    def rename_table(self, from_name: QualifiedName, to_name: Identifier):
        self.tables[from_name.with_name(to_name)] = self.tables.pop(from_name)

    def set_primary_key(self, table_name: QualifiedName, primary_key: PrimaryKey | None):
        self.tables[table_name].primary_key = primary_key

    def add_foreign_key(self, table_name: QualifiedName, foreign_key: ForeignKey):
        self.tables[table_name].add_foreign_key(foreign_key)

    def drop_constraint(self, table_name: QualifiedName, constraint_name: Identifier):
        self.tables[table_name].drop_constraint(constraint_name)

    def set_partition_expiration(self, table_name: QualifiedName, expiration_days: float | None):
        self.tables[table_name].partitioning.expiration_days = expiration_days

    def set_require_partition_filter(self, table_name: QualifiedName, require_filter: bool):
        self.tables[table_name].partitioning.require_filter = require_filter

    def set_clustering(self, table_name: QualifiedName, column_names: list[ColumnName] | None):
        self.tables[table_name].clustering = column_names

    def set_description(self, table_name: QualifiedName, description: str | None):
        self.tables[table_name].description = description

    def set_labels(self, table_name: QualifiedName, labels: dict[str, str] | None):
        self.tables[table_name].labels = labels

    def set_tags(self, table_name: QualifiedName, tags: dict[str, str] | None):
        self.tables[table_name].tags = tags

    def set_expiration_timestamp(self, table_name: QualifiedName, expiration_timestamp: datetime | None):
        self.tables[table_name].expiration_timestamp = expiration_timestamp

    def set_default_rounding_mode(self, table_name: QualifiedName, rounding_mode: RoundingMode | None):
        self.tables[table_name].default_rounding_mode = rounding_mode

    def set_max_staleness(self, table_name: QualifiedName, max_staleness: IntervalLiteral | None):
        self.tables[table_name].max_staleness = max_staleness

    def set_enable_change_history(self, table_name: QualifiedName, enabled: bool):
        self.tables[table_name].enable_change_history = enabled

    def set_enable_fine_grained_mutations(self, table_name: QualifiedName, enabled: bool):
        self.tables[table_name].enable_fine_grained_mutations = enabled

    def set_kms_key_name(self, table_name: QualifiedName, key_name: str | None):
        self.tables[table_name].kms_key_name = key_name

    def add_column(self, table_name: QualifiedName, column: Column):
        self.get_table(table_name).columns.append(column.model_copy(deep=True))

    def drop_column(self, table_name: QualifiedName, column_name: ColumnName):
        table = self.get_table(table_name)
        table.columns = [col for col in table.columns if col.name != column_name]

    def rename_column(self, table_name: QualifiedName, from_name: ColumnName, to_name: ColumnName):
        table = self.get_table(table_name)
        table.columns = [col if col.name != from_name else col.with_name(to_name) for col in table.columns]

    def set_column_datatype(self, table_name: QualifiedName, column_name: ColumnName, from_datatype: Datatype, to_datatype: Datatype):
        table = self.get_table(table_name)
        column = table.column_map[column_name]
        column.datatype = to_datatype

    def set_column_nullable(self, table_name: QualifiedName, column_name: ColumnName, nullable: bool):
        table = self.get_table(table_name)
        column = table.column_map[column_name]
        column.nullable = nullable

    def set_column_description(self, table_name: QualifiedName, column_name: ColumnName, description: str | None):
        table = self.get_table(table_name)
        column = table.column_map[column_name]
        column.description = description

    def set_column_rounding_mode(
        self,
        table_name: QualifiedName,
        column_name: ColumnName,
        rounding_mode: RoundingMode | None,
    ):
        table = self.get_table(table_name)
        column = table.column_map[column_name]
        column.rounding_mode = rounding_mode

    def has_view(self, name: QualifiedName) -> bool:
        return name in self.views

    def get_view(self, name: QualifiedName) -> View | None:
        return self.views.get(name)

    def create_or_replace_view(self, view: View):
        self.views[view.name] = view.model_copy(deep=True)

    def drop_view(self, name: QualifiedName):
        if name not in self.views:
            raise ValueError(f'View {name} does not exist')

        del self.views[name]

    def has_materialized_view(self, name: QualifiedName) -> bool:
        return name in self.materialized_views

    def get_materialized_view(self, name: QualifiedName) -> MaterializedView | None:
        return self.materialized_views.get(name)

    def create_or_replace_materialized_view(self, materialized_view: MaterializedView):
        self.materialized_views[materialized_view.name] = materialized_view.model_copy(deep=True)

    def drop_materialized_view(self, name: QualifiedName):
        if name not in self.materialized_views:
            raise ValueError(f'MaterializedView {name} does not exist')

        del self.materialized_views[name]


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
