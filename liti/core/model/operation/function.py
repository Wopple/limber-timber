from liti.core.model.operation.base import Operation
from liti.core.model.operation.table import CreateTable


def parse_operation(op_kind: str, op_data: dict) -> Operation:
    match op_kind:
        case "create_table":
            return CreateTable(**op_data)
        case _:
            raise ValueError(f"Unknown operation kind: {op_kind}")
