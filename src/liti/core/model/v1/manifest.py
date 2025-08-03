from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from liti.core.base import Star


class Template(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    path: list[str]
    value: Any
    match: dict | Star


class Manifest(BaseModel):
    version: int
    target_dir: Path
    operation_files: list[Path]
    templates: list[Template] | None
