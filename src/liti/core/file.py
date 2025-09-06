import json
from pathlib import Path

import yaml


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
