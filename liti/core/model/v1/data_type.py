from typing import Any

from pydantic import BaseModel

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
    collate: str | None = None


class Date(DataType):
    pass


class DateTime(DataType):
    pass


class Timestamp(DataType):
    pass


class Array(DataType):
    inner: DataType

    def model_post_init(self, _context: Any):
        if isinstance(self.inner, Array):
            raise ValueError('Nested arrays are not allowed')


class Struct(DataType):
    fields: dict[FieldName, DataType]


def parse_data_type(data: DataType | str | list[Any] | dict[str, Any]) -> DataType:
    # Already parsed
    if isinstance(data, DataType):
        return data
    # Map string value to atomic type
    elif isinstance(data, str):
        match data.upper():
            case 'BOOL' | 'BOOLEAN':
                return BOOL
            case 'INT64':
                return INT64
            case 'FLOAT64':
                return FLOAT64
            case 'STRING':
                return STRING
            case 'DATE':
                return DATE
            case 'DATETIME':
                return DATE_TIME
            case 'TIMESTAMP':
                return TIMESTAMP
    # Arrays are represented as a list with exactly one element representing the inner type
    elif isinstance(data, list):
        if len(data) == 1:
            return Array(inner=parse_data_type(data[0]))
        else:
            raise ValueError(f'Array data type must have exactly one inner type: {data}')
    # Structs are represented as a dict
    elif isinstance(data, dict):
        return Struct(fields={k: parse_data_type(v) for k, v in data.items()})
    else:
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
    elif isinstance(data, Date):
        return 'DATE'
    elif isinstance(data, DateTime):
        return 'DATETIME'
    elif isinstance(data, Timestamp):
        return 'TIMESTAMP'
    elif isinstance(data, Array):
        return [serialize_data_type(data.inner)]
    elif isinstance(data, Struct):
        return {k: serialize_data_type(v) for k, v in data.fields.items()}
    else:
        raise ValueError(f'Cannot serialize data type: {data}')


BOOL = Bool()
INT64 = Int(bits=64)
FLOAT64 = Float(bits=64)
STRING = String()
DATE = Date()
DATE_TIME = DateTime()
TIMESTAMP = Timestamp()
