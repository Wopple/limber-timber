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


def parse_data_type(data: DataType | str | list[Any] | dict[str, Any]) -> DataType:
    if isinstance(data, DataType):
        return data
    elif isinstance(data, str):
        match data.upper():
            case 'BOOL' | 'BOOLEAN': return BOOL
            case 'INT64': return INT64
            case 'FLOAT64': return FLOAT64
            case 'STRING': return STRING
    elif isinstance(data, list):
        if len(data) == 1:
            return Array(inner=parse_data_type(data[0]))
        else:
            raise ValueError(f'Array data type must have exactly one inner type: {data}')
    elif isinstance(data, dict):
        return Struct(
            fields={k: parse_data_type(v) for k, v in data.items()}
        )

    raise ValueError(f'Cannot parse data type: {data}')

def serialize_data_type(data: DataType) -> str | list[Any] | dict[str, Any]:
    if isinstance(data, Bool):
        return 'BOOL'
    elif isinstance(data, Int):
        return f'INT{data.bits}'
    elif isinstance(data, Float):
        return f'FLOAT{data.bits}'
    elif isinstance(data, String):
        return 'STRING'
    elif isinstance(data, Array):
        return [serialize_data_type(data.inner)]
    elif isinstance(data, Struct):
        return {k: serialize_data_type(v) for k, v in data.fields.items()}
    
    raise ValueError(f'Cannot serialize data type: {data}')


BOOL = Bool()
INT64 = Int(bits=64)
FLOAT64 = Float(bits=64)
STRING = String()
