from typing import ClassVar

from liti.core.backend.base import DbBackend
from liti.core.model.operation.base import Operation
from liti.core.model.schema import Table


class CreateTable(Operation):
    table: Table

    KIND: ClassVar[str] = "create_table"

    def up(self, backend: DbBackend):
        backend.create_table(self.table)

    def down(self, backend: DbBackend):
        backend.drop_table(self.table.name)

    def is_up(self, backend: DbBackend) -> bool:
        return self.table == backend.get_table(self.table.name)
