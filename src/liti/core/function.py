from typing import Any, Iterator

from liti.core.context import Context
from liti.core.model.v1.datatype import Array, Datatype, Struct
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.ops.base import OperationOps
from liti.core.model.v1.schema import ColumnName, FieldPath, Table


def extract_nested(data: Any, iterator: Iterator[Any], get_next: callable) -> Any:
    def extract(data: Any) -> Any:
        try:
            item = next(iterator)
        except StopIteration:
            return data

        return extract(get_next(data, item))

    return extract(data)


def extract_nested_datatype(table: Table, field_path: FieldPath) -> Datatype:
    def get_next(dt: Datatype, segment: str) -> Datatype:
        if isinstance(dt, Array):
            return get_next(dt.inner, segment)
        elif isinstance(dt, Struct):
            if segment in dt.fields:
                return dt.fields[segment]

        raise ValueError(f'Unable to extract datatype from {table} with path {field_path}')

    segments = iter(field_path)
    datatype = table.column_map[ColumnName(next(segments))].datatype
    return extract_nested(datatype, segments, get_next)


def attach_ops(operation: Operation, context: Context) -> OperationOps:
    return OperationOps.get_attachment(operation)(operation, context)
