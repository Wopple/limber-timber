import json
from pathlib import Path

import yaml

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import AddColumn, CreateTable, DropColumn, DropTable, RenameColumn, \
    RenameTable
from liti.core.model.v1.operation.ops.base import OperationOps
from liti.core.model.v1.operation.ops.table import AddColumnOps, CreateTableOps, DropColumnOps, DropTableOps, \
    RenameColumnOps, RenameTableOps


def parse_operation(op_kind: str, op_data: dict) -> Operation:
    match op_kind.lower():
        case 'create_table':
            return CreateTable(**op_data)
        case 'drop_table':
            return DropTable(**op_data)
        case 'rename_table':
            return RenameTable(**op_data)
        case 'add_column':
            return AddColumn(**op_data)
        case 'drop_column':
            return DropColumn(**op_data)
        case 'rename_column':
            return RenameColumn(**op_data)
        case _:
            raise ValueError(f'Unknown operation kind: {op_kind}')


def attach_ops(operation: Operation) -> OperationOps:
    if isinstance(operation, CreateTable):
        return CreateTableOps(operation)
    elif isinstance(operation, DropTable):
        return DropTableOps(operation)
    elif isinstance(operation, RenameTable):
        return RenameTableOps(operation)
    elif isinstance(operation, AddColumn):
        return AddColumnOps(operation)
    elif isinstance(operation, DropColumn):
        return DropColumnOps(operation)
    elif isinstance(operation, RenameColumn):
        return RenameColumnOps(operation)
    else:
        raise ValueError(f'Unhandled operation kind: {operation.KIND}')


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


def parse_manifest(path: Path) -> list[Path]:
    obj = parse_json_or_yaml_file(path)
    return [Path(filename) for filename in obj['operation_files']]


def parse_operation_file(path: Path) -> list[Operation]:
    obj = parse_json_or_yaml_file(path)
    return [parse_operation(op['kind'], op['data']) for op in obj["operations"]]


def get_target_operations(target_dir: Path) -> list[Operation]:
    manifest_path = get_manifest_path(target_dir)
    operations_filenames = parse_manifest(manifest_path)

    return [
        operation
        for filename in operations_filenames
        for operation in parse_operation_file(target_dir.joinpath(filename))
    ]
