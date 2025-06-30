from liti.core.backend.base import DbBackend
from liti.core.model.operation.data.table import CreateTable
from liti.core.model.operation.ops.base import OperationOps


class CreateTableOps(OperationOps):
    def __init__(self, op: CreateTable):
        self.op = op

    def up(self, backend: DbBackend):
        backend.create_table(self.op.table)

    def down(self, backend: DbBackend):
        backend.drop_table(self.op.table.name)

    def is_up(self, backend: DbBackend) -> bool:
        return self.op.table == backend.get_table(self.op.table.name)
