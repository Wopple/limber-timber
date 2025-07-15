from typing import Any, Literal

from pydantic import field_validator

from liti.core.base import LitiModel

FieldName = str


class DataType(LitiModel):
    pass


class Bool(DataType):
    pass


class Int(DataType):
    bits: int | None = None

    DEFAULT_METHOD = 'int_defaults'
    VALIDATE_METHOD = 'validate_int'

    @property
    def bytes(self) -> int:
        return self.bits // 8


class Float(DataType):
    bits: int | None = None

    DEFAULT_METHOD = 'float_defaults'
    VALIDATE_METHOD = 'validate_float'

    @property
    def bytes(self) -> int:
        return self.bits // 8


class Geography(DataType):
    pass


class Numeric(DataType):
    precision: int | None = None
    scale: int | None = None

    DEFAULT_METHOD = 'numeric_defaults'
    VALIDATE_METHOD = 'validate_numeric'


class BigNumeric(DataType):
    precision: int | None = None
    scale: int | None = None

    DEFAULT_METHOD = 'big_numeric_defaults'
    VALIDATE_METHOD = 'validate_big_numeric'


class String(DataType):
    characters: int | None = None
    collate: str | None = None


class Json(DataType):
    pass


class Date(DataType):
    pass


class Time(DataType):
    pass


class DateTime(DataType):
    pass


class Timestamp(DataType):
    pass


class Range(DataType):
    kind: Literal['DATE', 'DATETIME', 'TIMESTAMP']

    @field_validator('kind', mode='before')
    @classmethod
    def validate_kind(cls, value: str) -> str:
        return value.upper()


class Interval(DataType):
    pass


class Array(DataType):
    inner: DataType

    VALIDATE_METHOD = 'validate_array'


class Struct(DataType):
    fields: dict[FieldName, DataType]


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


def parse_data_type(data: DataType | str | dict[str, Any]) -> DataType:
    # Already parsed
    if isinstance(data, DataType):
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
            return Array(inner=parse_data_type(data['inner']))
        elif type_ == 'STRUCT':
            return Struct(fields={k: parse_data_type(v) for k, v in data['fields'].items()})

    raise ValueError(f'Cannot parse data type: {data}')


def serialize_data_type(data: DataType) -> str | list[Any] | dict[str, Any]:
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
            'inner': serialize_data_type(data.inner),
        }
    elif isinstance(data, Struct):
        return {
            'type': 'STRUCT',
            'fields': {k: serialize_data_type(v) for k, v in data.fields.items()},
        }
    else:
        raise ValueError(f'Cannot serialize data type: {data}')


__all__ = [
    'DataType',
    'Bool',
    'Int',
    'Float',
    'Geography',
    'Numeric',
    'BigNumeric',
    'String',
    'Json',
    'Date',
    'Time',
    'DateTime',
    'Timestamp',
    'Range',
    'Interval',
    'Array',
    'Struct',
    'BOOL',
    'INT64',
    'FLOAT64',
    'GEOGRAPHY',
    'STRING',
    'JSON',
    'DATE',
    'TIME',
    'DATE_TIME',
    'TIMESTAMP',
    'INTERVAL',
    'parse_data_type',
    'serialize_data_type',
]
