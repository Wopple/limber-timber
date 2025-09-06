from pathlib import Path

from pydantic import BaseModel


class Manifest(BaseModel):
    version: int
    operation_files: list[Path]
