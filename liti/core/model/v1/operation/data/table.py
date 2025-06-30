from typing import ClassVar

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import Table


class CreateTable(Operation):
    table: Table

    KIND: ClassVar[str] = 'create_table'
