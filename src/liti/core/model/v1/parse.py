from pathlib import Path

from liti.core.base import LitiModel, STAR
from liti.core.file import parse_json_or_yaml_file
from liti.core.model.v1.manifest import Manifest
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.template import Template


def parse_manifest(path: Path) -> Manifest:
    obj = parse_json_or_yaml_file(path)

    return Manifest(
        version=obj['version'],
        operation_files=[Path(filename) for filename in obj['operation_files']],
    )


def parse_templates(path: Path) -> list[Template]:
    arr = parse_json_or_yaml_file(path)

    return [
        Template(
            operation_kinds=template.get('operation_kinds', []),
            root_type=LitiModel.by_name(template['root_type']) if template.get('root_type') else None,
            path=template['path'].split('.'),
            value=template['value'],
            full_match=template.get('full_match', STAR),
            local_match=template.get('local_match', STAR),
        )
        for template in arr
    ]


def parse_operation(op_kind: str, op_data: dict) -> Operation:
    return Operation.by_kind(op_kind)(**op_data)


def parse_operation_file(path: Path) -> list[Operation]:
    obj = parse_json_or_yaml_file(path)
    return [parse_operation(op['kind'], op['data']) for op in obj['operations']]


def parse_operations(operation_files: list[Path], target_dir: Path) -> list[tuple[Path, list[Operation]]]:
    return [
        (filename, parse_operation_file(target_dir.joinpath(filename)))
        for filename in operation_files
    ]
