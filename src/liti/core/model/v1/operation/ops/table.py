from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.table import AddColumn, CreateTable, DropColumn, DropTable, RenameColumn, \
    RenameTable, SetClustering, SetColumnDatatype, SetColumnDescription, SetColumnNullable, SetColumnRoundingMode, \
    SetDefaultRoundingMode, \
    SetDescription, \
    SetLabels, SetTags
from liti.core.model.v1.operation.ops.base import OperationOps


class CreateTableOps(OperationOps):
    op: CreateTable

    def __init__(self, op: CreateTable):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.create_table(self.op.table)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> DropTable:
        return DropTable(table_name=self.op.table.name)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.has_table(self.op.table.name)


class DropTableOps(OperationOps):
    op: DropTable

    def __init__(self, op: DropTable):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.drop_table(self.op.table_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> CreateTable:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return CreateTable(table=sim_table)

    def is_up(self, db_backend: DbBackend) -> bool:
        return not db_backend.has_table(self.op.table_name)


class RenameTableOps(OperationOps):
    op: RenameTable

    def __init__(self, op: RenameTable):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.rename_table(self.op.from_name, self.op.to_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> RenameTable:
        return RenameTable(
            from_name=self.op.from_name.with_table_name(self.op.to_name),
            to_name=self.op.from_name.table_name,
        )

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.has_table(self.op.from_name.with_table_name(self.op.to_name))


class SetClusteringOps(OperationOps):
    op: SetClustering

    def __init__(self, op: SetClustering):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_clustering(self.op.table_name, self.op.columns)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetClustering:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetClustering(table_name=self.op.table_name, columns=sim_table.clustering)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).clustering == self.op.columns


class SetDescriptionOps(OperationOps):
    op: SetDescription

    def __init__(self, op: SetDescription):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_description(self.op.table_name, self.op.description)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetDescription:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetDescription(table_name=self.op.table_name, description=sim_table.description)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).description == self.op.description


class SetLabelsOps(OperationOps):
    op: SetLabels

    def __init__(self, op: SetLabels):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_labels(self.op.table_name, self.op.labels)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetLabels:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetLabels(table_name=self.op.table_name, labels=sim_table.labels)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).labels == self.op.labels


class SetTagsOps(OperationOps):
    op: SetTags

    def __init__(self, op: SetTags):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_tags(self.op.table_name, self.op.tags)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetTags:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetTags(table_name=self.op.table_name, tags=sim_table.tags)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).tags == self.op.tags


class SetDefaultRoundingModeOps(OperationOps):
    op: SetDefaultRoundingMode

    def __init__(self, op: SetDefaultRoundingMode):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_default_rounding_mode(self.op.table_name, self.op.rounding_mode)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetDefaultRoundingMode:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetDefaultRoundingMode(table_name=self.op.table_name, rounding_mode=sim_table.default_rounding_mode)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).default_rounding_mode == self.op.rounding_mode


class AddColumnOps(OperationOps):
    op: AddColumn

    def __init__(self, op: AddColumn):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.add_column(self.op.table_name, self.op.column)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> DropColumn:
        return DropColumn(table_name=self.op.table_name, column_name=self.op.column.name)

    def is_up(self, db_backend: DbBackend) -> bool:
        return self.op.column.name in db_backend.get_table(self.op.table_name).column_map


class DropColumnOps(OperationOps):
    op: DropColumn

    def __init__(self, op: DropColumn):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.drop_column(self.op.table_name, self.op.column_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> AddColumn:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]
        return AddColumn(table_name=self.op.table_name, column=sim_column)

    def is_up(self, db_backend: DbBackend) -> bool:
        return self.op.column_name not in db_backend.get_table(self.op.table_name).column_map


class RenameColumnOps(OperationOps):
    op: RenameColumn

    def __init__(self, op: RenameColumn):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.rename_column(self.op.table_name, self.op.from_name, self.op.to_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> RenameColumn:
        return RenameColumn(
            table_name=self.op.table_name,
            from_name=self.op.to_name,
            to_name=self.op.from_name,
        )

    def is_up(self, db_backend: DbBackend) -> bool:
        return self.op.to_name in db_backend.get_table(self.op.table_name).column_map


class SetColumnDatatypeOps(OperationOps):
    op: SetColumnDatatype

    def __init__(self, op: SetColumnDatatype):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        sim_db = self.simulate(meta_backend.get_applied_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        db_backend.set_column_datatype(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            from_datatype=sim_column.datatype,
            to_datatype=self.op.datatype,
        )

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetColumnDatatype:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnDatatype(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            datatype=sim_column.datatype,
        )

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).column_map[self.op.column_name].datatype == self.op.datatype


class SetColumnNullableOps(OperationOps):
    op: SetColumnNullable

    def __init__(self, op: SetColumnNullable):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_column_nullable(self.op.table_name, self.op.column_name, self.op.nullable)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetColumnNullable:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnNullable(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            nullable=sim_column.nullable,
        )

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).column_map[self.op.column_name].nullable == self.op.nullable


class SetColumnDescriptionOps(OperationOps):
    op: SetColumnDescription

    def __init__(self, op: SetColumnDescription):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_column_description(self.op.table_name, self.op.column_name, self.op.description)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetColumnDescription:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnDescription(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            description=sim_column.description,
        )

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).column_map[self.op.column_name].description == self.op.description


class SetColumnRoundingModeOps(OperationOps):
    op: SetColumnRoundingMode

    def __init__(self, op: SetColumnRoundingMode):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.set_column_rounding_mode(self.op.table_name, self.op.column_name, self.op.rounding_mode)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetColumnRoundingMode:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnRoundingMode(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            rounding_mode=sim_column.rounding_mode,
        )

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).column_map[self.op.column_name].rounding_mode == self.op.rounding_mode
