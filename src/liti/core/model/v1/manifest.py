from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from liti.core.base import LitiModel, Star, STAR


class Template(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # the root type from which to start looking for the value to replace
    root_type: type[LitiModel]

    # the path from the root to the field to replace
    path: list[str]

    # the value to replace the field with
    value: Any

    # filter on the whole data structure
    full_match: dict | Star = STAR

    # filter from the root
    local_match: dict | Star = STAR


class Manifest(BaseModel):
    version: int
    target_dir: Path
    operation_files: list[Path]
    templates: list[Template] | None
