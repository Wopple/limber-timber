from typing import Literal
from unittest.mock import Mock

from pytest import fixture, mark, raises

from liti.core.backend.bigquery import BigQueryDbBackend
from liti.core.model.v1.data_type import *
from liti.core.model.v1.schema import Partitioning
from util import NoRaise


@fixture
def bq_client():
    return Mock()


@fixture
def db_backend(bq_client):
    return BigQueryDbBackend(bq_client)


def test_int_defaults(db_backend: BigQueryDbBackend):
    node = Int()
    node.set_defaults(db_backend)
    assert node.bits == 64


def test_float_defaults(db_backend: BigQueryDbBackend):
    node = Float()
    node.set_defaults(db_backend)
    assert node.bits == 64


def test_numeric_defaults(db_backend: BigQueryDbBackend):
    node = Numeric()
    node.set_defaults(db_backend)
    assert node.precision == 38
    assert node.scale == 9


def test_big_numeric_defaults(db_backend: BigQueryDbBackend):
    node = BigNumeric()
    node.set_defaults(db_backend)
    assert node.precision == 76
    assert node.scale == 38


@mark.parametrize(
    'bits, raise_ctx',
    [
        (None, raises(ValueError)),
        (8, raises(ValueError)),
        (16, raises(ValueError)),
        (32, raises(ValueError)),
        (64, NoRaise()),
        (128, raises(ValueError)),
    ],
)
def test_validate_int(db_backend: BigQueryDbBackend, bits: int | None, raise_ctx):
    node = Int(bits=bits)

    with raise_ctx:
        node.liti_validate(db_backend)


@mark.parametrize(
    'bits, raise_ctx',
    [
        (None, raises(ValueError)),
        (8, raises(ValueError)),
        (16, raises(ValueError)),
        (32, raises(ValueError)),
        (64, NoRaise()),
        (128, raises(ValueError)),
    ],
)
def test_validate_float(db_backend: BigQueryDbBackend, bits: int | None, raise_ctx):
    node = Float(bits=bits)

    with raise_ctx:
        node.liti_validate(db_backend)


@mark.parametrize(
    'precision, scale, raise_ctx',
    [
        *[
            (precision, scale, NoRaise())
            for scale in [0, 4, 9]
            for precision in [max(scale, 1), (max(scale, 1) + 38) // 2, 38]
        ],
        *[
            (precision, scale, raises(ValueError))
            for scale in [0, 4, 9]
            for precision in [0, (0 + scale - 1) // 2, scale - 1]
        ],
        *[
            (precision, scale, raises(ValueError))
            for scale in [0, 4, 9]
            for precision in [scale + 30, (scale + 30 + 39) // 2, 39]
        ],
        *[
            (precision, scale, raises(ValueError))
            for scale in [-1, 10]
            for precision in [1, 19, 38]
        ],
    ],
)
def test_validate_numeric(db_backend: BigQueryDbBackend, precision: int, scale: int, raise_ctx):
    node = Numeric(precision=precision, scale=scale)

    with raise_ctx:
        node.liti_validate(db_backend)


@mark.parametrize(
    'precision, scale, raise_ctx',
    [
        *[
            (precision, scale, NoRaise())
            for scale in [0, 19, 38]
            for precision in [max(scale, 1), (max(scale, 1) + 76) // 2, 76]
        ],
        *[
            (precision, scale, raises(ValueError))
            for scale in [0, 19, 38]
            for precision in [0, (0 + scale - 1) // 2, scale - 1]
        ],
        *[
            (precision, scale, raises(ValueError))
            for scale in [0, 19, 38]
            for precision in [scale + 39, (scale + 39 + 77) // 2, 77]
        ],
        *[
            (precision, scale, raises(ValueError))
            for scale in [-1, 39]
            for precision in [1, 39, 77]
        ],
    ],
)
def test_validate_big_numeric(db_backend: BigQueryDbBackend, precision: int, scale: int, raise_ctx):
    node = BigNumeric(precision=precision, scale=scale)

    with raise_ctx:
        node.liti_validate(db_backend)


@mark.parametrize(
    'inner, raise_ctx',
    [
        (BOOL, NoRaise()),
        (INT64, NoRaise()),
        (FLOAT64, NoRaise()),
        (GEOGRAPHY, NoRaise()),
        (Numeric(), NoRaise()),
        (BigNumeric(), NoRaise()),
        (STRING, NoRaise()),
        (JSON, NoRaise()),
        (DATE, NoRaise()),
        (TIME, NoRaise()),
        (DATE_TIME, NoRaise()),
        (TIMESTAMP, NoRaise()),
        (Range(kind='DATE'), NoRaise()),
        (Range(kind='DATETIME'), NoRaise()),
        (Range(kind='TIMESTAMP'), NoRaise()),
        (INTERVAL, NoRaise()),
        (Array(inner=BOOL), raises(ValueError)),
        (Struct(fields={'field': BOOL}), NoRaise()),
    ],
)
def test_validate_array(db_backend: BigQueryDbBackend, inner: DataType, raise_ctx):
    node = Array(inner=inner)

    with raise_ctx:
        node.liti_validate(db_backend)


@mark.parametrize(
    'kind, time_unit, int_start, int_end, int_step, raise_ctx',
    [
        *[
            ('TIME', time_unit, None, None, None, NoRaise())
            for time_unit in ['YEAR', 'MONTH', 'DAY', 'HOUR']
        ],
        ('TIME', None, None, None, None, raises(ValueError)),
        ('TIME', 'YEAR', 0, None, None, raises(ValueError)),
        ('TIME', 'YEAR', None, 0, None, raises(ValueError)),
        ('TIME', 'YEAR', None, None, 0, raises(ValueError)),
        *[
            ('INT', None, None, None, None, NoRaise())
            for int_start in range(3)
            for int_end in range(int_start + 1, int_start + 4)
            for int_step in range(max(int_start, int_start))
        ],
        ('INT', None, None, 1, 1, raises(ValueError)),
        ('INT', None, 0, None, 1, raises(ValueError)),
        ('INT', None, 0, 1, None, raises(ValueError)),
        ('INT', 'YEAR', 0, 1, 1, raises(ValueError)),
        ('INT', 'MONTH', 0, 1, 1, raises(ValueError)),
        ('INT', 'DAY', 0, 1, 1, raises(ValueError)),
        ('INT', 'HOUR', 0, 1, 1, raises(ValueError)),
    ],
)
def test_validate_partitioning(
    db_backend: BigQueryDbBackend,
    kind: Literal['TIME', 'INT'],
    time_unit: Literal['YEAR', 'MONTH', 'DAY', 'HOUR'] | None,
    int_start: int | None,
    int_end: int | None,
    int_step: int | None,
    raise_ctx,
):
    node = Partitioning(
        kind=kind,
        time_unit=time_unit,
        int_start=int_start,
        int_end=int_end,
        int_step=int_step,
    )

    with raise_ctx:
        node.liti_validate(db_backend)
