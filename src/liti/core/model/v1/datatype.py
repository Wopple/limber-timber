from typing import Any, Literal

from pydantic import field_validator

from liti.core.base import LitiModel

FieldName = str


class Datatype(LitiModel):
    pass


class Bool(Datatype):
    pass


class Int(Datatype):
    bits: int | None = None

    DEFAULT_METHOD = 'int_defaults'
    VALIDATE_METHOD = 'validate_int'

    @property
    def bytes(self) -> int:
        return self.bits // 8


class Float(Datatype):
    bits: int | None = None

    DEFAULT_METHOD = 'float_defaults'
    VALIDATE_METHOD = 'validate_float'

    @property
    def bytes(self) -> int:
        return self.bits // 8


class Geography(Datatype):
    pass


class Numeric(Datatype):
    precision: int | None = None
    scale: int | None = None

    DEFAULT_METHOD = 'numeric_defaults'
    VALIDATE_METHOD = 'validate_numeric'


class BigNumeric(Datatype):
    precision: int | None = None
    scale: int | None = None

    DEFAULT_METHOD = 'big_numeric_defaults'
    VALIDATE_METHOD = 'validate_big_numeric'


class String(Datatype):
    characters: int | None = None


class Json(Datatype):
    pass


class Date(Datatype):
    pass


class Time(Datatype):
    pass


class DateTime(Datatype):
    pass


class Timestamp(Datatype):
    pass


class Range(Datatype):
    kind: Literal['DATE', 'DATETIME', 'TIMESTAMP']

    @field_validator('kind', mode='before')
    @classmethod
    def validate_kind(cls, value: str) -> str:
        return value.upper()


class Interval(Datatype):
    pass


class Array(Datatype):
    inner: Datatype

    VALIDATE_METHOD = 'validate_array'


class Struct(Datatype):
    fields: dict[FieldName, Datatype]


BOOL = Bool()
INT64 = Int(bits=64)
FLOAT64 = Float(bits=64)
GEOGRAPHY = Geography()
STRING = String()
JSON = Json()
DATE = Date()
TIME = Time()
DATE_TIME = DateTime()
TIMESTAMP = Timestamp()
INTERVAL = Interval()


def parse_datatype(data: Datatype | str | dict[str, Any]) -> Datatype:
    # Already parsed
    if isinstance(data, Datatype):
        return data
    # Map string value to type
    elif isinstance(data, str):
        data = data.upper()

        if data in ('BOOL', 'BOOLEAN'):
            return BOOL
        elif data == 'INT64':
            return INT64
        elif data == 'FLOAT64':
            return FLOAT64
        elif data == 'GEOGRAPHY':
            return GEOGRAPHY
        elif data == 'STRING':
            return STRING
        elif data == 'JSON':
            return JSON
        elif data == 'DATE':
            return DATE
        elif data == 'TIME':
            return TIME
        elif data == 'DATETIME':
            return DATE_TIME
        elif data == 'TIMESTAMP':
            return TIMESTAMP
        elif data == 'INTERVAL':
            return INTERVAL
    # Parse parametric type
    elif isinstance(data, dict):
        type_ = data['type'].upper()

        if type_ == 'INT':
            return Int(bits=data['bits'])
        elif type_ == 'FLOAT':
            return Float(bits=data['bits'])
        elif type_ == 'NUMERIC':
            return Numeric(precision=data['precision'], scale=data['scale'])
        elif type_ == 'BIGNUMERIC':
            return BigNumeric(precision=data['precision'], scale=data['scale'])
        elif type_ == 'RANGE':
            return Range(kind=data['kind'])
        elif type_ == 'ARRAY':
            return Array(inner=parse_datatype(data['inner']))
        elif type_ == 'STRUCT':
            return Struct(fields={k: parse_datatype(v) for k, v in data['fields'].items()})

    raise ValueError(f'Cannot parse data type: {data}')


def serialize_datatype(data: Datatype) -> str | list[Any] | dict[str, Any]:
    if isinstance(data, Bool):
        return 'BOOL'
    elif isinstance(data, Int):
        return {
            'type': 'INT',
            'bits': data.bits,
        }
    elif isinstance(data, Float):
        return {
            'type': 'FLOAT',
            'bits': data.bits,
        }
    elif isinstance(data, Geography):
        return 'GEOGRAPHY'
    elif isinstance(data, Numeric):
        return {
            'type': 'NUMERIC',
            'precision': data.precision,
            'scale': data.scale,
        }
    elif isinstance(data, BigNumeric):
        return {
            'type': 'BIGNUMERIC',
            'precision': data.precision,
            'scale': data.scale,
        }
    elif isinstance(data, String):
        return 'STRING'
    elif isinstance(data, Json):
        return 'JSON'
    elif isinstance(data, Date):
        return 'DATE'
    elif isinstance(data, Time):
        return 'TIME'
    elif isinstance(data, DateTime):
        return 'DATETIME'
    elif isinstance(data, Timestamp):
        return 'TIMESTAMP'
    elif isinstance(data, Interval):
        return 'INTERVAL'
    elif isinstance(data, Range):
        return {
            'type': 'RANGE',
            'kind': data.kind,
        }
    elif isinstance(data, Array):
        return {
            'type': 'ARRAY',
            'inner': serialize_datatype(data.inner),
        }
    elif isinstance(data, Struct):
        return {
            'type': 'STRUCT',
            'fields': {k: serialize_datatype(v) for k, v in data.fields.items()},
        }
    else:
        raise ValueError(f'Cannot serialize data type: {data}')
