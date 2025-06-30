from typing import ClassVar

from liti.core.model.operation.data.base import Operation
from liti.core.model.schema import Table


class CreateTable(Operation):
    table: Table

    KIND: ClassVar[str] = 'create_table'
