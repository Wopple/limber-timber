from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.table import AddColumn, CreateTable, DropColumn, DropTable, RenameColumn, \
    RenameTable
from liti.core.model.v1.operation.ops.base import OperationOps


class CreateTableOps(OperationOps):
    def __init__(self, op: CreateTable):
        self.op = op

    def up(self, db_backend: DbBackend):
        db_backend.create_table(self.op.table)

    def down(self, db_backend: DbBackend, _meta_backend: MetaBackend) -> DropTable:
        return DropTable(table_name=self.op.table.name)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.has_table(self.op.table.name)


class DropTableOps(OperationOps):
    def __init__(self, op: DropTable):
        self.op = op

    def up(self, db_backend: DbBackend):
        db_backend.drop_table(self.op.table_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> CreateTable:
        # Get the table in the state before it was dropped
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        # Recreate the table in that state
        return CreateTable(table=sim_table)

    def is_up(self, db_backend: DbBackend) -> bool:
        return not db_backend.has_table(self.op.table_name)


class RenameTableOps(OperationOps):
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


class AddColumnOps(OperationOps):
    def __init__(self, op: AddColumn):
        self.op = op

    def up(self, db_backend: DbBackend):
        db_backend.add_column(self.op.table_name, self.op.column)

    def down(self, db_backend: DbBackend, _meta_backend: MetaBackend) -> DropColumn:
        return DropColumn(table_name=self.op.table_name, column_name=self.op.column.name)

    def is_up(self, db_backend: DbBackend) -> bool:
        return self.op.column.name in db_backend.get_table(self.op.table_name).column_map


class DropColumnOps(OperationOps):
    def __init__(self, op: DropColumn):
        self.op = op

    def up(self, db_backend: DbBackend):
        db_backend.drop_column(self.op.table_name, self.op.column_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> AddColumn:
        # Get the column in the state before it was dropped
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        # Recreate the column in that state
        return AddColumn(table_name=self.op.table_name, column=sim_column)

    def is_up(self, db_backend: DbBackend) -> bool:
        return self.op.column_name not in db_backend.get_table(self.op.table_name).column_map


class RenameColumnOps(OperationOps):
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
