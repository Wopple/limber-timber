import json
from pathlib import Path

import yaml

from liti.core.model.operation.data.base import Operation
from liti.core.model.operation.data.table import CreateTable
from liti.core.model.operation.ops.base import OperationOps
from liti.core.model.operation.ops.table import CreateTableOps
from liti.core.model.schema import Table


def parse_operation(op_kind: str, op_data: dict) -> Operation:
    match op_kind:
        case 'create_table':
            return CreateTable(table=Table(**op_data))
        case _:
            raise ValueError(f'Unknown operation kind: {op_kind}')


def attach_ops(operation: Operation) -> OperationOps:
    match operation.KIND:
        case 'create_table':
            return CreateTableOps(operation)
        case _:
            raise ValueError(f'Unhandled operation kind: {operation.KIND}')


def parse_json_or_yaml_file(path: Path) -> list | dict:
    with open(path) as f:
        content = f.read()

    if path.suffix == '.json':
        return json.loads(content)
    elif path.suffix in ('.yaml', '.yml'):
        return yaml.safe_load(content)
    else:
        raise ValueError(f'Unexpected file extension: "{path}"')


def get_manifest_path(target_dir: Path) -> Path:
    filenames = ['manifest.json', 'manifest.yaml', 'manifest.yml']

    for filename in filenames:
        candidate = target_dir.joinpath(filename)
        if candidate.is_file():
            return candidate

    raise ValueError(f'No manifest found in {target_dir}')


def parse_manifest(path: Path) -> list[Path]:
    array = parse_json_or_yaml_file(path) or []

    assert isinstance(array, list), f'Manifest file {path} should contain a list of filenames.'

    return [Path(filename) for filename in array]


def parse_operations_file(path: Path) -> list[Operation]:
    array = parse_json_or_yaml_file(path)
    return [parse_operation(op['kind'], op['data']) for op in array]


def get_target_operations(target_dir: Path) -> list[Operation]:
    manifest_path = get_manifest_path(target_dir)
    operations_filenames = parse_manifest(manifest_path)

    return [
        operation
        for filename in operations_filenames
        for operation in parse_operations_file(target_dir.joinpath(filename))
    ]
