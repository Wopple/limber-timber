from typing import ClassVar

from pydantic import BaseModel


class Operation(BaseModel):
    KIND: ClassVar[str]
