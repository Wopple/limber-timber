from typing import Any, Literal

from pydantic import field_validator, model_serializer
from pydantic_core.core_schema import FieldSerializationInfo, SerializerFunctionWrapHandler

from liti.core.base import LitiModel

FieldName = str


class Datatype(LitiModel):
    type: str


class Bool(Datatype):
    type: Literal['BOOL'] = 'BOOL'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class Int(Datatype):
    type: Literal['INT'] = 'INT'
    bits: int | None = None

    @property
    def bytes(self) -> int:
        return self.bits // 8

    @model_serializer(mode='wrap')
    def serialize(self, nxt: SerializerFunctionWrapHandler) -> str | dict[str, Any]:
        if self.bits in {8, 16, 32, 64}:
            return f'INT{self.bits}'
        else:
            return nxt(self)


class Float(Datatype):
    type: Literal['FLOAT'] = 'FLOAT'
    bits: int | None = None

    @property
    def bytes(self) -> int:
        return self.bits // 8

    @model_serializer(mode='wrap')
    def serialize(self, nxt: SerializerFunctionWrapHandler) -> str | dict[str, Any]:
        if self.bits in {8, 16, 32, 64}:
            return f'FLOAT{self.bits}'
        else:
            return nxt(self)


class Geography(Datatype):
    type: Literal['GEOGRAPHY'] = 'GEOGRAPHY'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class Numeric(Datatype):
    type: Literal['NUMERIC'] = 'NUMERIC'
    precision: int | None = None
    scale: int | None = None


class BigNumeric(Datatype):
    type: Literal['BIGNUMERIC'] = 'BIGNUMERIC'
    precision: int | None = None
    scale: int | None = None


class String(Datatype):
    type: Literal['STRING'] = 'STRING'
    characters: int | None = None

    @model_serializer(mode='wrap')
    def serialize(self, nxt: SerializerFunctionWrapHandler) -> str | dict[str, Any]:
        if self.characters is None:
            return self.type
        else:
            return nxt(self)


class Bytes(Datatype):
    type: Literal['BYTES'] = 'BYTES'
    bytes: int | None = None

    @model_serializer(mode='wrap')
    def serialize(self, nxt: SerializerFunctionWrapHandler) -> str | dict[str, Any]:
        if self.bytes is None:
            return self.type
        else:
            return nxt(self)


class Json(Datatype):
    type: Literal['JSON'] = 'JSON'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class Date(Datatype):
    type: Literal['DATE'] = 'DATE'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class Time(Datatype):
    type: Literal['TIME'] = 'TIME'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class DateTime(Datatype):
    type: Literal['DATETIME'] = 'DATETIME'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class Timestamp(Datatype):
    type: Literal['TIMESTAMP'] = 'TIMESTAMP'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class Range(Datatype):
    type: Literal['RANGE'] = 'RANGE'
    kind: Literal['DATE', 'DATETIME', 'TIMESTAMP']

    @field_validator('kind', mode='before')
    @classmethod
    def validate_kind(cls, value: str) -> str:
        return value.upper()


class Interval(Datatype):
    type: Literal['INTERVAL'] = 'INTERVAL'

    @model_serializer
    def serialize(self) -> str:
        return self.type


class Array(Datatype):
    type: Literal['ARRAY'] = 'ARRAY'
    inner: Datatype

    @model_serializer
    def serialize(self, info: FieldSerializationInfo) -> dict[str, Any]:
        return {
            'type': self.type,
            'inner': self.inner.model_dump(exclude_none=info.exclude_none),
        }


class Struct(Datatype):
    type: Literal['STRUCT'] = 'STRUCT'
    fields: dict[FieldName, Datatype]

    @model_serializer
    def serialize(self, info: FieldSerializationInfo) -> dict[str, Any]:
        return {
            'type': self.type,
            'fields': {
                name: dt.model_dump(exclude_none=info.exclude_none)
                for name, dt in self.fields.items()
            },
        }


BOOL = Bool()
INT64 = Int(bits=64)
FLOAT64 = Float(bits=64)
GEOGRAPHY = Geography()
STRING = String()
BYTES = Bytes()
JSON = Json()
DATE = Date()
TIME = Time()
DATE_TIME = DateTime()
TIMESTAMP = Timestamp()
INTERVAL = Interval()


def parse_datatype(data: Datatype | str | dict[str, Any]) -> Datatype:
    # Need new instances since templates modify in place

    # Already parsed
    if isinstance(data, Datatype):
        if data is BOOL:
            return Bool()
        elif data is INT64:
            return Int(bits=64)
        elif data is FLOAT64:
            return Float(bits=64)
        elif data is GEOGRAPHY:
            return Geography()
        elif data is STRING:
            return String()
        elif data is BYTES:
            return Bytes()
        elif data is JSON:
            return Json()
        elif data is DATE:
            return Date()
        elif data is TIME:
            return Time()
        elif data is DATE_TIME:
            return DateTime()
        elif data is TIMESTAMP:
            return Timestamp()
        elif data is INTERVAL:
            return Interval()
        else:
            return data
    # Map string value to type
    elif isinstance(data, str):
        data = data.upper()

        if data in ('BOOL', 'BOOLEAN'):
            return Bool()
        elif data == 'INT64':
            return Int(bits=64)
        elif data == 'FLOAT64':
            return Float(bits=64)
        elif data == 'GEOGRAPHY':
            return Geography()
        elif data == 'STRING':
            return String()
        elif data == 'BYTES':
            return Bytes()
        elif data == 'JSON':
            return Json()
        elif data == 'DATE':
            return Date()
        elif data == 'TIME':
            return Time()
        elif data == 'DATETIME':
            return DateTime()
        elif data == 'TIMESTAMP':
            return Timestamp()
        elif data == 'INTERVAL':
            return Interval()
    # Parse object into type
    elif isinstance(data, dict):
        type_ = data['type'].upper()

        if type_ == 'BOOL':
            return Bool()
        elif type_ == 'INT':
            return Int(bits=data.get('bits'))
        elif type_ == 'FLOAT':
            return Float(bits=data.get('bits'))
        elif type_ == 'NUMERIC':
            return Numeric(precision=data.get('precision'), scale=data.get('scale'))
        elif type_ == 'BIGNUMERIC':
            return BigNumeric(precision=data.get('precision'), scale=data.get('scale'))
        elif type_ == 'GEOGRAPHY':
            return Geography()
        elif type_ == 'STRING':
            return String(characters=data.get('characters'))
        elif type_ == 'BYTES':
            return Bytes(bytes=data.get('bytes'))
        elif type_ == 'JSON':
            return Json()
        elif type_ == 'DATE':
            return Date()
        elif type_ == 'TIME':
            return Time()
        elif type_ == 'DATETIME':
            return DateTime()
        elif type_ == 'TIMESTAMP':
            return Timestamp()
        elif type_ == 'INTERVAL':
            return Interval()
        elif type_ == 'RANGE':
            return Range(kind=data['kind'])
        elif type_ == 'ARRAY':
            return Array(inner=parse_datatype(data['inner']))
        elif type_ == 'STRUCT':
            return Struct(fields={k: parse_datatype(v) for k, v in data['fields'].items()})

    raise ValueError(f'Cannot parse data type: {data}')
