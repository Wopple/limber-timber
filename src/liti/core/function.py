import json
from pathlib import Path
from typing import Any, Iterator

import yaml

from liti.core.base import LitiModel, STAR
from liti.core.context import Context
from liti.core.model.v1.datatype import Array, Datatype, Struct
from liti.core.model.v1.manifest import Manifest, Template
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


def parse_operation(op_kind: str, op_data: dict) -> Operation:
    return Operation.get_kind(op_kind)(**op_data)


def attach_ops(operation: Operation, context: Context) -> OperationOps:
    return OperationOps.get_attachment(operation)(operation, context)


def parse_json_or_yaml_file(path: Path) -> list | dict:
    with open(path) as f:
        content = f.read()

    suffix = path.suffix.lower()

    if suffix == '.json':
        return json.loads(content)
    elif suffix in ('.yaml', '.yml'):
        return yaml.safe_load(content)
    else:
        raise ValueError(f'Unexpected file extension: "{path}"')


def get_manifest_path(target_dir: Path) -> Path:
    filenames = ('manifest.json', 'manifest.yaml', 'manifest.yml')

    for filename in filenames:
        candidate = target_dir.joinpath(filename)

        if candidate.is_file():
            return candidate

    raise ValueError(f'No manifest found in {target_dir}')


def parse_manifest(path: Path) -> Manifest:
    obj = parse_json_or_yaml_file(path)

    return Manifest(
        version=obj['version'],
        operation_files=[Path(filename) for filename in obj['operation_files']],
        templates=None if 'templates' not in obj else [
            Template(
                operation_types=[LitiModel.by_name(name) for name in template.get('operation_types', [])],
                root_type=LitiModel.by_name(template['root_type']),
                path=template['path'].split('.'),
                value=template['value'],
                full_match=template.get('full_match', STAR),
                local_match=template.get('local_match', STAR),
            )
            for template in obj['templates']
        ],
    )


def parse_operation_file(path: Path) -> list[Operation]:
    obj = parse_json_or_yaml_file(path)
    return [parse_operation(op['kind'], op['data']) for op in obj['operations']]


def parse_operations(operation_files: list[Path], target_dir: Path) -> list[Operation]:
    return [
        operation
        for filename in operation_files
        for operation in parse_operation_file(target_dir.joinpath(filename))
    ]
