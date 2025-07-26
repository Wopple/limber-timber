from pytest import mark

from liti.core.model.v1.datatype import BigNumeric, BOOL, DATE, DATE_TIME, Float, GEOGRAPHY, Int, INTERVAL, JSON, \
    Numeric, Range, STRING, TIME, TIMESTAMP


def test_serialize_bool():
    actual = BOOL.model_dump()
    assert actual == 'BOOL'


@mark.parametrize(
    'bits, expected',
    [
        (None, {"type": "INT", "bits": None}),
        (8, 'INT8'),
        (16, 'INT16'),
        (32, 'INT32'),
        (64, 'INT64'),
        (128, {"type": "INT", "bits": 128}),
    ],
)
def test_serialize_int(bits, expected):
    actual = Int(bits=bits).model_dump()
    assert actual == expected


@mark.parametrize(
    'bits, expected',
    [
        (None, {"type": "FLOAT", "bits": None}),
        (8, 'FLOAT8'),
        (16, 'FLOAT16'),
        (32, 'FLOAT32'),
        (64, 'FLOAT64'),
        (128, {"type": "FLOAT", "bits": 128}),
    ],
)
def test_serialize_float(bits, expected):
    actual = Float(bits=bits).model_dump()
    assert actual == expected


@mark.parametrize(
    'precision, scale, expected',
    [
        (None, None, {"type": "NUMERIC", "precision": None, "scale": None}),
        (1, None, {"type": "NUMERIC", "precision": 1, "scale": None}),
        (None, 0, {"type": "NUMERIC", "precision": None, "scale": 0}),
        (1, 0, {"type": "NUMERIC", "precision": 1, "scale": 0}),
        (38, 9, {"type": "NUMERIC", "precision": 38, "scale": 9}),
    ],
)
def test_serialize_numeric(precision, scale, expected):
    actual = Numeric(precision=precision, scale=scale).model_dump()
    assert actual == expected


@mark.parametrize(
    'precision, scale, expected',
    [
        (None, None, {"type": "BIGNUMERIC", "precision": None, "scale": None}),
        (1, None, {"type": "BIGNUMERIC", "precision": 1, "scale": None}),
        (None, 0, {"type": "BIGNUMERIC", "precision": None, "scale": 0}),
        (1, 0, {"type": "BIGNUMERIC", "precision": 1, "scale": 0}),
        (76, 38, {"type": "BIGNUMERIC", "precision": 76, "scale": 38}),
    ],
)
def test_serialize_big_numeric(precision, scale, expected):
    actual = BigNumeric(precision=precision, scale=scale).model_dump()
    assert actual == expected


def test_serialize_geography():
    actual = GEOGRAPHY.model_dump()
    assert actual == 'GEOGRAPHY'


def test_serialize_string():
    actual = STRING.model_dump()
    assert actual == 'STRING'


def test_serialize_json():
    actual = JSON.model_dump()
    assert actual == 'JSON'


def test_serialize_date():
    actual = DATE.model_dump()
    assert actual == 'DATE'


def test_serialize_time():
    actual = TIME.model_dump()
    assert actual == 'TIME'


def test_serialize_date_time():
    actual = DATE_TIME.model_dump()
    assert actual == 'DATETIME'


def test_serialize_timestamp():
    actual = TIMESTAMP.model_dump()
    assert actual == 'TIMESTAMP'


@mark.parametrize(
    'kind, expected',
    [
        ('DATE', {"type": "RANGE", "kind": 'DATE'}),
        ('DATETIME', {"type": "RANGE", "kind": 'DATETIME'}),
        ('TIMESTAMP', {"type": "RANGE", "kind": 'TIMESTAMP'}),
    ],
)
def test_serialize_range(kind, expected):
    actual = Range(kind=kind).model_dump()
    assert actual == expected


def test_serialize_interval():
    actual = INTERVAL.model_dump()
    assert actual == 'INTERVAL'
