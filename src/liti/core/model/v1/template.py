from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from liti.core.base import LitiModel, Star, STAR


class Template(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # only perform replacements on operations within these files, empty mean consider all files
    files: list[str] = Field(default_factory=str)

    # the kinds of operations to perform replacements within, empty means consider all operations
    operation_kinds: list[str] = Field(default_factory=list)

    # the root type from which to start looking for the value to replace
    root_type: type[LitiModel] = None

    # the path from the root to the field to replace
    path: list[str]

    # the value to replace the field with
    value: Any

    # filter on the whole data structure
    full_match: dict | Star = STAR

    # filter from the root
    local_match: dict | Star = STAR
