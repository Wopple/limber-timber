from typing import Any, Literal

from pydantic import field_validator

from liti.core.base import LitiModel

type FieldName = str


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


def parse_data_type(data: DataType | str | dict[str, Any]) -> DataType:
    # Already parsed
    if isinstance(data, DataType):
        return data
    # Map string value to type
    elif isinstance(data, str):
        match data.upper():
            case 'BOOL' | 'BOOLEAN':
                return BOOL
            case 'INT64':
                return INT64
            case 'FLOAT64':
                return FLOAT64
            case 'GEOGRAPHY':
                return GEOGRAPHY
            case 'STRING':
                return STRING
            case 'JSON':
                return JSON
            case 'DATE':
                return DATE
            case 'TIME':
                return TIME
            case 'DATETIME':
                return DATE_TIME
            case 'TIMESTAMP':
                return TIMESTAMP
            case 'INTERVAL':
                return INTERVAL
    # Parse parametric type
    elif isinstance(data, dict):
        match data['type'].upper():
            case 'INT':
                return Int(bits=data['bits'])
            case 'FLOAT':
                return Float(bits=data['bits'])
            case 'NUMERIC':
                return Numeric(precision=data['precision'], scale=data['scale'])
            case 'BIGNUMERIC':
                return BigNumeric(precision=data['precision'], scale=data['scale'])
            case 'RANGE':
                return Range(kind=data['kind'])
            case 'ARRAY':
                return Array(inner=parse_data_type(data['inner']))
            case 'STRUCT':
                return Struct(fields={k: parse_data_type(v) for k, v in data['fields'].items()})
    else:
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
