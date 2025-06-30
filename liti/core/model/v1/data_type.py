from typing import Any, Self

from pydantic import BaseModel, model_validator

type FieldName = str


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
            raise ValueError('Nested arrays are not allowed')


class Struct(DataType):
    fields: dict[FieldName, DataType]


BOOL = Bool()
INT64 = Int(bits=64)
FLOAT64 = Float(bits=64)
STRING = String()
