from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.table import AddColumn, CreateTable, DropColumn, DropTable, RenameColumn, \
    RenameTable, SetClustering, SetColumnDescription, SetColumnRoundingMode, SetDefaultRoundingMode, SetDescription
from liti.core.model.v1.operation.ops.base import OperationOps


class CreateTableOps(OperationOps):
    op: CreateTable

    def __init__(self, op: CreateTable):
        self.op = op

    def up(self, db_backend: DbBackend):
        db_backend.create_table(self.op.table)

    def down(self, db_backend: DbBackend, _meta_backend: MetaBackend) -> DropTable:
        return DropTable(table_name=self.op.table.name)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.has_table(self.op.table.name)


class DropTableOps(OperationOps):
    op: DropTable

    def __init__(self, op: DropTable):
        self.op = op

    def up(self, db_backend: DbBackend):
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

    def up(self, db_backend: DbBackend):
        db_backend.rename_table(self.op.from_name, self.op.to_name)

    def down(self, db_backend: DbBackend, _meta_backend: MetaBackend) -> RenameTable:
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

    def up(self, db_backend: DbBackend):
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

    def up(self, db_backend: DbBackend):
        db_backend.set_column_description(self.op.table_name, self.op.columns)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetDescription:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetDescription(table_name=self.op.table_name, description=sim_table.description)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table_name).description == self.op.description


class SetDefaultRoundingModeOps(OperationOps):
    op: SetDefaultRoundingMode

    def __init__(self, op: SetDefaultRoundingMode):
        self.op = op

    def up(self, db_backend: DbBackend):
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

    def up(self, db_backend: DbBackend):
        db_backend.add_column(self.op.table_name, self.op.column)

    def down(self, db_backend: DbBackend, _meta_backend: MetaBackend) -> DropColumn:
        return DropColumn(table_name=self.op.table_name, column_name=self.op.column.name)

    def is_up(self, db_backend: DbBackend) -> bool:
        return self.op.column.name in db_backend.get_table(self.op.table_name).column_map


class DropColumnOps(OperationOps):
    op: DropColumn

    def __init__(self, op: DropColumn):
        self.op = op

    def up(self, db_backend: DbBackend):
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

    def up(self, db_backend: DbBackend):
        db_backend.rename_column(self.op.table_name, self.op.from_name, self.op.to_name)

    def down(self, db_backend: DbBackend, _meta_backend: MetaBackend) -> RenameColumn:
        return RenameColumn(
            table_name=self.op.table_name,
            from_name=self.op.to_name,
            to_name=self.op.from_name,
        )

    def is_up(self, db_backend: DbBackend) -> bool:
        return self.op.to_name in db_backend.get_table(self.op.table_name).column_map


class SetColumnDescriptionOps(OperationOps):
    op: SetColumnDescription

    def __init__(self, op: SetColumnDescription):
        self.op = op

    def up(self, db_backend: DbBackend):
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

    def up(self, db_backend: DbBackend):
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
