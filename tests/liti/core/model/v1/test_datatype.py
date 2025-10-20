from typing import Any

from pytest import mark

from liti.core.model.v1 import datatype as dt


def test_serialize_bool():
    actual = dt.BOOL.model_dump()
    assert actual == 'BOOL'


@mark.parametrize(
    'bits, expected',
    [
        (None, {'type': 'INT', 'bits': None}),
        (8, 'INT8'),
        (16, 'INT16'),
        (32, 'INT32'),
        (64, 'INT64'),
        (128, {'type': 'INT', 'bits': 128}),
    ],
)
def test_serialize_int(bits, expected):
    actual = dt.Int(bits=bits).model_dump()
    assert actual == expected


@mark.parametrize(
    'bits, expected',
    [
        (None, {'type': 'FLOAT', 'bits': None}),
        (8, 'FLOAT8'),
        (16, 'FLOAT16'),
        (32, 'FLOAT32'),
        (64, 'FLOAT64'),
        (128, {'type': 'FLOAT', 'bits': 128}),
    ],
)
def test_serialize_float(bits, expected):
    actual = dt.Float(bits=bits).model_dump()
    assert actual == expected


@mark.parametrize(
    'precision, scale, expected',
    [
        (None, None, {'type': 'NUMERIC', 'precision': None, 'scale': None}),
        (1, None, {'type': 'NUMERIC', 'precision': 1, 'scale': None}),
        (None, 0, {'type': 'NUMERIC', 'precision': None, 'scale': 0}),
        (1, 0, {'type': 'NUMERIC', 'precision': 1, 'scale': 0}),
        (38, 9, {'type': 'NUMERIC', 'precision': 38, 'scale': 9}),
    ],
)
def test_serialize_numeric(precision, scale, expected):
    actual = dt.Numeric(precision=precision, scale=scale).model_dump()
    assert actual == expected


@mark.parametrize(
    'precision, scale, expected',
    [
        (None, None, {'type': 'BIGNUMERIC', 'precision': None, 'scale': None}),
        (1, None, {'type': 'BIGNUMERIC', 'precision': 1, 'scale': None}),
        (None, 0, {'type': 'BIGNUMERIC', 'precision': None, 'scale': 0}),
        (1, 0, {'type': 'BIGNUMERIC', 'precision': 1, 'scale': 0}),
        (76, 38, {'type': 'BIGNUMERIC', 'precision': 76, 'scale': 38}),
    ],
)
def test_serialize_big_numeric(precision, scale, expected):
    actual = dt.BigNumeric(precision=precision, scale=scale).model_dump()
    assert actual == expected


def test_serialize_geography():
    actual = dt.GEOGRAPHY.model_dump()
    assert actual == 'GEOGRAPHY'


def test_serialize_string():
    actual = dt.STRING.model_dump()
    assert actual == 'STRING'


def test_serialize_json():
    actual = dt.JSON.model_dump()
    assert actual == 'JSON'


def test_serialize_date():
    actual = dt.DATE.model_dump()
    assert actual == 'DATE'


def test_serialize_time():
    actual = dt.TIME.model_dump()
    assert actual == 'TIME'


def test_serialize_date_time():
    actual = dt.DATE_TIME.model_dump()
    assert actual == 'DATETIME'


def test_serialize_timestamp():
    actual = dt.TIMESTAMP.model_dump()
    assert actual == 'TIMESTAMP'


@mark.parametrize(
    'kind, expected',
    [
        ('DATE', {'type': 'RANGE', 'kind': 'DATE'}),
        ('DATETIME', {'type': 'RANGE', 'kind': 'DATETIME'}),
        ('TIMESTAMP', {'type': 'RANGE', 'kind': 'TIMESTAMP'}),
    ],
)
def test_serialize_range(kind, expected):
    actual = dt.Range(kind=kind).model_dump()
    assert actual == expected


def test_serialize_interval():
    actual = dt.INTERVAL.model_dump()
    assert actual == 'INTERVAL'


@mark.parametrize(
    'serialized, expected',
    [
        (dt.BOOL, dt.BOOL),
        (dt.INT64, dt.INT64),
        (dt.FLOAT64, dt.FLOAT64),
        (dt.GEOGRAPHY, dt.GEOGRAPHY),
        (dt.STRING, dt.STRING),
        (dt.BYTES, dt.BYTES),
        (dt.JSON, dt.JSON),
        (dt.DATE, dt.DATE),
        (dt.TIME, dt.TIME),
        (dt.DATE_TIME, dt.DATE_TIME),
        (dt.TIMESTAMP, dt.TIMESTAMP),
        (dt.INTERVAL, dt.INTERVAL),
        ('BOOL', dt.BOOL),
        ('INT64', dt.INT64),
        ('FLOAT64', dt.FLOAT64),
        ('GEOGRAPHY', dt.GEOGRAPHY),
        ('STRING', dt.STRING),
        ('BYTES', dt.BYTES),
        ('JSON', dt.JSON),
        ('DATE', dt.DATE),
        ('TIME', dt.TIME),
        ('DATETIME', dt.DATE_TIME),
        ('TIMESTAMP', dt.TIMESTAMP),
        ('INTERVAL', dt.INTERVAL),
        ({'type': 'BOOL'}, dt.BOOL),
        ({'type': 'INT'}, dt.Int()),
        ({'type': 'INT', 'bits': 64}, dt.Int(bits=64)),
        ({'type': 'FLOAT'}, dt.Float()),
        ({'type': 'FLOAT', 'bits': 64}, dt.Float(bits=64)),
        ({'type': 'NUMERIC'}, dt.Numeric()),
        ({'type': 'NUMERIC', 'precision': 38, 'scale': 9}, dt.Numeric(precision=38, scale=9)),
        ({'type': 'BIGNUMERIC'}, dt.BigNumeric()),
        ({'type': 'BIGNUMERIC', 'precision': 76, 'scale': 38}, dt.BigNumeric(precision=76, scale=38)),
        ({'type': 'GEOGRAPHY'}, dt.GEOGRAPHY),
        ({'type': 'STRING'}, dt.String()),
        ({'type': 'STRING', 'characters': 1}, dt.String(characters=1)),
        ({'type': 'BYTES'}, dt.Bytes()),
        ({'type': 'BYTES', 'bytes': 1}, dt.Bytes(bytes=1)),
        ({'type': 'JSON'}, dt.JSON),
        ({'type': 'DATE'}, dt.DATE),
        ({'type': 'TIME'}, dt.TIME),
        ({'type': 'DATETIME'}, dt.DATE_TIME),
        ({'type': 'TIMESTAMP'}, dt.TIMESTAMP),
        ({'type': 'INTERVAL'}, dt.INTERVAL),
        ({'type': 'RANGE', 'kind': 'DATE'}, dt.Range(kind='DATE')),
        ({'type': 'RANGE', 'kind': 'DATETIME'}, dt.Range(kind='DATETIME')),
        ({'type': 'RANGE', 'kind': 'TIMESTAMP'}, dt.Range(kind='TIMESTAMP')),
        ({'type': 'ARRAY', 'inner': dt.BOOL}, dt.Array(inner=dt.BOOL)),
        ({'type': 'ARRAY', 'inner': 'BOOL'}, dt.Array(inner=dt.BOOL)),
        ({'type': 'ARRAY', 'inner': {'type': 'BOOL'}}, dt.Array(inner=dt.BOOL)),
        ({'type': 'STRUCT', 'fields': {'enabled': dt.BOOL}}, dt.Struct(fields={'enabled': dt.BOOL})),
        ({'type': 'STRUCT', 'fields': {'enabled': 'BOOL'}}, dt.Struct(fields={'enabled': dt.BOOL})),
        ({'type': 'STRUCT', 'fields': {'enabled': {'type': 'BOOL'}}}, dt.Struct(fields={'enabled': dt.BOOL})),
    ],
)
def test_parse_datatype(serialized: str | dict[str, Any], expected: dt.Datatype):
    assert dt.parse_datatype(serialized) == expected


@mark.parametrize(
    'serialized',
    [
        dt.BOOL,
        dt.INT64,
        dt.FLOAT64,
        dt.GEOGRAPHY,
        dt.STRING,
        dt.BYTES,
        dt.JSON,
        dt.DATE,
        dt.TIME,
        dt.DATE_TIME,
        dt.TIMESTAMP,
        dt.INTERVAL,
        'BOOL',
        'INT64',
        'FLOAT64',
        'GEOGRAPHY',
        'STRING',
        'BYTES',
        'JSON',
        'DATE',
        'TIME',
        'DATETIME',
        'TIMESTAMP',
        'INTERVAL',
        {'type': 'BOOL'},
        {'type': 'INT'},
        {'type': 'INT', 'bits': 64},
        {'type': 'FLOAT'},
        {'type': 'FLOAT', 'bits': 64},
        {'type': 'NUMERIC'},
        {'type': 'NUMERIC', 'precision': 38, 'scale': 9},
        {'type': 'BIGNUMERIC'},
        {'type': 'BIGNUMERIC', 'precision': 76, 'scale': 38},
        {'type': 'GEOGRAPHY'},
        {'type': 'STRING'},
        {'type': 'STRING', 'characters': 1},
        {'type': 'BYTES'},
        {'type': 'BYTES', 'bytes': 1},
        {'type': 'JSON'},
        {'type': 'DATE'},
        {'type': 'TIME'},
        {'type': 'DATETIME'},
        {'type': 'TIMESTAMP'},
        {'type': 'INTERVAL'},
        {'type': 'RANGE', 'kind': 'DATE'},
        {'type': 'RANGE', 'kind': 'DATETIME'},
        {'type': 'RANGE', 'kind': 'TIMESTAMP'},
        {'type': 'ARRAY', 'inner': dt.BOOL},
        {'type': 'ARRAY', 'inner': 'BOOL'},
        {'type': 'ARRAY', 'inner': {'type': 'BOOL'}},
        {'type': 'STRUCT', 'fields': {'enabled': dt.BOOL}},
        {'type': 'STRUCT', 'fields': {'enabled': 'BOOL'}},
        {'type': 'STRUCT', 'fields': {'enabled': {'type': 'BOOL'}}},
    ],
)
def test_parse_datatype_new_instance(serialized: str | dict[str, Any]):
    assert dt.parse_datatype(serialized) is not dt.parse_datatype(serialized)
