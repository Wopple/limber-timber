from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.model.v1.operation.data.table import CreateTable, DropTable
from liti.core.model.v1.operation.ops.base import OperationOps


class CreateTableOps(OperationOps):
    def __init__(self, op: CreateTable):
        self.op = op

    def up(self, db_backend: DbBackend):
        db_backend.create_table(self.op.table)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend):
        db_backend.drop_table(self.op.table.name)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.table.name) == self.op.table


class DropTableOps(OperationOps):
    def __init__(self, op: DropTable):
        self.op = op

    def up(self, db_backend: DbBackend):
        db_backend.drop_table(self.op.name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend):
        from liti.core.runner import MigrateRunner  # circular import

        # Simulate migrations to get the state of the table before it was dropped

        # [:-1] so the simulation does not drop the table we want to create
        sim_ops = meta_backend.get_applied_operations()[:-1]
        sim_db = MemoryDbBackend()
        sim_meta = MemoryMetaBackend()

        MigrateRunner(
            db_backend=sim_db,
            meta_backend=sim_meta,
            target=sim_ops,
        ).run(
            wet_run=True,
            silent=True,
        )

        sim_table = sim_db.get_table(self.op.name)

        # Recreate the table in that state
        db_backend.create_table(sim_table)

    def is_up(self, db_backend: DbBackend) -> bool:
        return db_backend.get_table(self.op.name) is None
