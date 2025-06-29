from typing import Any

from pydantic import BaseModel

from liti.core.model.schema import ColumnName


class DataType(BaseModel):
    pass


class Bool(DataType):
    pass


class Int(DataType):
    bits: int

    @property
    def bytes(self) -> int:
        return self.bits // 8


class Float(DataType):
    bits: int

    @property
    def bytes(self) -> int:
        return self.bits // 8


class String(DataType):
    characters: int | None = None


class Array(DataType):
    inner: DataType

    def model_post_init(self, _context: Any):
        if isinstance(self.inner, Array):
            raise ValueError("Array<Array> is not allowed")


class Struct(DataType):
    fields: dict[ColumnName, DataType]


BOOL = Bool()
INT64 = Int(bits=64)
FLOAT64 = Float(bits=64)
STRING = String()
