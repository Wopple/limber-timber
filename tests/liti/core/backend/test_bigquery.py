from datetime import datetime, timezone
from typing import Literal
from unittest.mock import Mock

from pytest import fixture, mark, raises

from liti import bigquery as bq
from liti.core.backend.bigquery import BigQueryDbBackend, can_coerce, column_to_sql, datatype_to_sql, \
    extract_dataset_ref, interval_literal_to_sql, NULLABLE, REPEATED, REQUIRED, to_bq_table, to_column, \
    to_dataset_ref, to_datatype, to_datatype_array, to_field_type, to_fields, to_liti_table, to_max_length, to_mode, \
    to_precision, to_range_element_type, to_scale, to_schema_field, to_table_ref
from liti.core.model.v1.datatype import Array, BigNumeric, BOOL, Datatype, DATE, DATE_TIME, Float, FLOAT64, GEOGRAPHY, \
    Int, INT64, INTERVAL, JSON, Numeric, Range, STRING, String, Struct, TIME, TIMESTAMP
from liti.core.model.v1.schema import BigLake, Column, ColumnName, DatabaseName, ForeignKey, ForeignReference, \
    Identifier, IntervalLiteral, Partitioning, PrimaryKey, RoundingModeLiteral, SchemaName, Table, TableName
from tests.liti.util import NoRaise


@fixture
def bq_client() -> Mock:
    return Mock()


@fixture
def db_backend(bq_client) -> BigQueryDbBackend:
    return BigQueryDbBackend(bq_client, raise_unsupported=set())


def test_to_dataset_ref():
    actual = to_dataset_ref(DatabaseName('test_project'), SchemaName('test_dataset'))
    assert actual == bq.DatasetReference('test_project', 'test_dataset')


@mark.parametrize(
    'name, expected',
    [
        [
            TableName('test_project.test_dataset.test_table'),
            bq.DatasetReference('test_project', 'test_dataset'),
        ],
        [
            bq.TableListItem({
                'tableReference': {
                    'projectId': 'test_project',
                    'datasetId': 'test_dataset',
                    'tableId': 'test_table',
                }
            }),
            bq.DatasetReference('test_project', 'test_dataset'),
        ],
    ],
)
def test_extract_dataset_ref(name, expected):
    actual = extract_dataset_ref(name)
    assert actual == expected


@mark.parametrize(
    'name, expected',
    [
        [
            TableName('test_project.test_dataset.test_table'),
            bq.TableReference(bq.DatasetReference('test_project', 'test_dataset'), 'test_table'),
        ],
        [
            bq.TableListItem({
                'tableReference': {
                    'projectId': 'test_project',
                    'datasetId': 'test_dataset',
                    'tableId': 'test_table',
                }
            }),
            bq.TableReference(bq.DatasetReference('test_project', 'test_dataset'), 'test_table'),
        ],
    ],
)
def test_to_table_ref(name, expected):
    actual = to_table_ref(name)
    assert actual == expected


@mark.parametrize(
    'datatype, expected',
    [
        [BOOL, 'BOOL'],
        [INT64, 'INT64'],
        [Int(bits=64), 'INT64'],
        [FLOAT64, 'FLOAT64'],
        [Float(bits=64), 'FLOAT64'],
        [GEOGRAPHY, 'GEOGRAPHY'],
        [Numeric(precision=1, scale=1), 'NUMERIC'],
        [BigNumeric(precision=1, scale=1), 'BIGNUMERIC'],
        [STRING, 'STRING'],
        [String(characters=1), 'STRING'],
        [JSON, 'JSON'],
        [DATE, 'DATE'],
        [TIME, 'TIME'],
        [DATE_TIME, 'DATETIME'],
        [TIMESTAMP, 'TIMESTAMP'],
        [Range(kind='DATE'), 'RANGE'],
        [Range(kind='DATETIME'), 'RANGE'],
        [Range(kind='TIMESTAMP'), 'RANGE'],
        [INTERVAL, 'INTERVAL'],
        [Array(inner=BOOL), 'BOOL'],
        [Struct(fields={'field': BOOL}), 'RECORD'],
    ],
)
def test_to_field_type(datatype, expected):
    actual = to_field_type(datatype)
    assert actual == expected


@mark.parametrize(
    'column, expected',
    [
        [Column(name=ColumnName('test_name'), datatype=BOOL, nullable=False), REQUIRED],
        [Column(name=ColumnName('test_name'), datatype=BOOL, nullable=True), NULLABLE],
        [Column(name=ColumnName('test_name'), datatype=Array(inner=BOOL), nullable=False), REPEATED],
        [Column(name=ColumnName('test_name'), datatype=Array(inner=BOOL), nullable=True), REPEATED],
    ],
)
def test_to_mode(column, expected):
    actual = to_mode(column)
    assert actual == expected


@mark.parametrize(
    'datatype, expected',
    [
        [
            Struct(fields={
                'field1': BOOL,
                'field2': INT64,
            }),
            (
                bq.SchemaField('field1', 'BOOL'),
                bq.SchemaField('field2', 'INT64'),
            ),
        ],
        [
            Array(inner=Struct(fields={
                'field1': BOOL,
                'field2': INT64,
                'field3': Array(inner=Struct(fields={
                    'inner_field1': BOOL,
                    'inner_field2': INT64,
                })),
            })),
            (
                bq.SchemaField('field1', 'BOOL'),
                bq.SchemaField('field2', 'INT64'),
                bq.SchemaField(
                    name='field3',
                    field_type='RECORD',
                    mode=REPEATED,
                    fields=(
                        bq.SchemaField('inner_field1', 'BOOL'),
                        bq.SchemaField('inner_field2', 'INT64'),
                    )
                ),
            ),
        ],
    ],
)
def test_to_fields(datatype, expected):
    actual = to_fields(datatype)
    assert actual == expected


@mark.parametrize(
    'datatype, expected',
    [
        [Numeric(precision=2, scale=1), 2],
        [BigNumeric(precision=2, scale=1), 2],
        [Array(inner=Numeric(precision=2, scale=1)), 2],
        [Array(inner=BigNumeric(precision=2, scale=1)), 2],
    ],
)
def test_to_precision(datatype, expected):
    actual = to_precision(datatype)
    assert actual == expected


@mark.parametrize(
    'datatype, expected',
    [
        [Numeric(precision=2, scale=1), 1],
        [BigNumeric(precision=2, scale=1), 1],
        [Array(inner=Numeric(precision=2, scale=1)), 1],
        [Array(inner=BigNumeric(precision=2, scale=1)), 1],
    ],
)
def test_to_scale(datatype, expected):
    actual = to_scale(datatype)
    assert actual == expected


@mark.parametrize(
    'datatype, expected',
    [
        [STRING, None],
        [String(characters=None), None],
        [String(characters=1), 1],
        [Array(inner=STRING), None],
        [Array(inner=String(characters=None)), None],
        [Array(inner=String(characters=1)), 1],
    ],
)
def test_to_max_length(datatype, expected):
    actual = to_max_length(datatype)
    assert actual == expected


@mark.parametrize(
    'datatype, expected',
    [
        [Range(kind='DATE'), 'DATE'],
        [Range(kind='DATETIME'), 'DATETIME'],
        [Range(kind='TIMESTAMP'), 'TIMESTAMP'],
        [Array(inner=Range(kind='DATE')), 'DATE'],
        [Array(inner=Range(kind='DATETIME')), 'DATETIME'],
        [Array(inner=Range(kind='TIMESTAMP')), 'TIMESTAMP'],
    ],
)
def test_to_range_element_type(datatype, expected):
    actual = to_range_element_type(datatype)
    assert actual == expected


@mark.parametrize(
    'column, expected',
    [
        [
            Column(
                name='col_bool',
                datatype=BOOL,
                nullable=False,
            ),
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                mode=REQUIRED,
            ),
        ],
        [
            Column(
                name='col_date',
                datatype=DATE,
                default_expression='CURRENT_DATE',
                nullable=True,
            ),
            bq.SchemaField(
                name='col_date',
                field_type='DATE',
                default_value_expression='CURRENT_DATE',
            ),
        ],
        [
            Column(
                name='col_bool',
                datatype=BOOL,
                nullable=True,
                description='Test description',
            ),
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                description='Test description',
            ),
        ],
        [
            Column(
                name='col_struct',
                datatype=Struct(fields={
                    'field1': BOOL,
                    'field2': INT64,
                }),
                nullable=True,
            ),
            bq.SchemaField(
                name='col_struct',
                field_type='RECORD',
                fields=(bq.SchemaField('field1', 'BOOL'), bq.SchemaField('field2', 'INT64')),
            ),
        ],
        [
            Column(
                name='col_numeric',
                datatype=Numeric(precision=2, scale=1),
                nullable=True,
            ),
            bq.SchemaField(
                name='col_numeric',
                field_type='NUMERIC',
                precision=2,
                scale=1,
            ),
        ],
        [
            Column(
                name='col_bignumeric',
                datatype=BigNumeric(precision=2, scale=1),
                nullable=True,
            ),
            bq.SchemaField(
                name='col_bignumeric',
                field_type='BIGNUMERIC',
                precision=2,
                scale=1,
            ),
        ],
        [
            Column(
                name='col_string',
                datatype=String(characters=1),
                nullable=True,
            ),
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
                max_length=1,
            ),
        ],
        [
            Column(
                name='col_range',
                datatype=Range(kind='DATE'),
                nullable=True,
            ),
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATE',
            ),
        ],
        [
            Column(
                name='col_range',
                datatype=Range(kind='DATETIME'),
                nullable=True,
            ),
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATETIME',
            ),
        ],
        [
            Column(
                name='col_range',
                datatype=Range(kind='TIMESTAMP'),
                nullable=True,
            ),
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='TIMESTAMP',
            ),
        ],
        [
            Column(
                name='col_float',
                datatype=FLOAT64,
                nullable=True,
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
            ),
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
            ),
        ],
        [
            Column(
                name='col_float',
                datatype=FLOAT64,
                nullable=True,
                rounding_mode='ROUND_HALF_EVEN',
            ),
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_EVEN',
            ),
        ],
    ],
)
def test_to_schema_field(column, expected):
    actual = to_schema_field(column)
    assert actual == expected


def test_to_bq_table():
    table = Table(
        name=TableName('test_project.test_dataset.test_table'),
        columns=[Column(name=ColumnName('col_date'), datatype=DATE)],
        primary_key=PrimaryKey(column_names=[ColumnName('col_date')]),
        foreign_keys=[ForeignKey(
            name=Identifier('fk_test'),
            foreign_table_name=TableName('test_project.test_dataset.fk_test_table'),
            references=[ForeignReference(
                local_column_name=ColumnName('col_date'),
                foreign_column_name=ColumnName('fk_col_date'),
            )],
        )],
        partitioning=Partitioning(
            kind='TIME',
            column=ColumnName('col_date'),
            time_unit='DAY',
        ),
        clustering=[ColumnName('col_date')],
        friendly_name='test_friendly',
        description='test_description',
        labels={'l1': 'v1'},
        tags={'t1': 'v1'},
        expiration_timestamp=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        max_staleness=IntervalLiteral(hour=1),
        enable_change_history=True,
        enable_fine_grained_mutations=True,
        kms_key_name='test_key',
        storage_uri='test/uri',
        file_format='PARQUET',
        table_format='ICEBERG',
    )

    actual = to_bq_table(table)

    assert actual.project == 'test_project'
    assert actual.dataset_id == 'test_dataset'
    assert actual.table_id == 'test_table'
    assert actual.schema == [bq.SchemaField('col_date', 'DATE', mode=REQUIRED)]
    assert actual.table_constraints == bq.TableConstraints(
        primary_key=bq.PrimaryKey(['col_date']),
        foreign_keys=[
            bq.ForeignKey(
                name='fk_test',
                referenced_table=bq.TableReference(bq.DatasetReference('test_project', 'test_dataset'), 'fk_test_table'),
                column_references=[bq.ColumnReference('col_date', 'fk_col_date')],
            )
        ],
    )
    assert actual.time_partitioning == bq.TimePartitioning(
        type_='DAY',
        field='col_date',
        require_partition_filter=False,
    )
    assert actual.clustering_fields == ['col_date']
    assert actual.friendly_name == 'test_friendly'
    assert actual.description == 'test_description'
    assert actual.labels == {'l1': 'v1'}
    assert actual.resource_tags == {'t1': 'v1'}
    assert actual.expires == datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert actual.max_staleness == '0-0 0 1:0:0.000000'


@mark.parametrize(
    'datatype, expected',
    [
        [BOOL, 'BOOL'],
        [INT64, 'INT64'],
        [Int(bits=64), 'INT64'],
        [FLOAT64, 'FLOAT64'],
        [Float(bits=64), 'FLOAT64'],
        [GEOGRAPHY, 'GEOGRAPHY'],
        [Numeric(precision=2, scale=1), 'NUMERIC(2, 1)'],
        [BigNumeric(precision=4, scale=3), 'BIGNUMERIC(4, 3)'],
        [STRING, 'STRING'],
        [String(characters=1), 'STRING(1)'],
        [JSON, 'JSON'],
        [DATE, 'DATE'],
        [TIME, 'TIME'],
        [DATE_TIME, 'DATETIME'],
        [TIMESTAMP, 'TIMESTAMP'],
        [Range(kind='DATE'), 'RANGE<DATE>'],
        [Range(kind='DATETIME'), 'RANGE<DATETIME>'],
        [Range(kind='TIMESTAMP'), 'RANGE<TIMESTAMP>'],
        [INTERVAL, 'INTERVAL'],
        [Array(inner=BOOL), 'ARRAY<BOOL>'],
        [Struct(fields={'field': BOOL}), 'STRUCT<field BOOL>'],
    ],
)
def test_datatype_to_sql(datatype, expected):
    actual = datatype_to_sql(datatype)
    assert actual == expected


@mark.parametrize(
    'interval, expected',
    [
        [IntervalLiteral(year=1), 'INTERVAL \'1-0 0 0:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(month=1), 'INTERVAL \'0-1 0 0:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(day=1), 'INTERVAL \'0-0 1 0:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(hour=1), 'INTERVAL \'0-0 0 1:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(minute=1), 'INTERVAL \'0-0 0 0:1:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(second=1), 'INTERVAL \'0-0 0 0:0:1.000000\' YEAR TO SECOND'],
        [IntervalLiteral(microsecond=1), 'INTERVAL \'0-0 0 0:0:0.000001\' YEAR TO SECOND'],
        [IntervalLiteral(year=1, month=2), 'INTERVAL \'1-2 0 0:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(year=1, day=2), 'INTERVAL \'1-0 2 0:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(year=1, hour=2), 'INTERVAL \'1-0 0 2:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(year=1, minute=2), 'INTERVAL \'1-0 0 0:2:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(year=1, second=2), 'INTERVAL \'1-0 0 0:0:2.000000\' YEAR TO SECOND'],
        [IntervalLiteral(year=1, microsecond=2), 'INTERVAL \'1-0 0 0:0:0.000002\' YEAR TO SECOND'],
        [IntervalLiteral(month=1, day=2), 'INTERVAL \'0-1 2 0:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(month=1, hour=2), 'INTERVAL \'0-1 0 2:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(month=1, minute=2), 'INTERVAL \'0-1 0 0:2:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(month=1, second=2), 'INTERVAL \'0-1 0 0:0:2.000000\' YEAR TO SECOND'],
        [IntervalLiteral(month=1, microsecond=2), 'INTERVAL \'0-1 0 0:0:0.000002\' YEAR TO SECOND'],
        [IntervalLiteral(day=1, hour=2), 'INTERVAL \'0-0 1 2:0:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(day=1, minute=2), 'INTERVAL \'0-0 1 0:2:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(day=1, second=2), 'INTERVAL \'0-0 1 0:0:2.000000\' YEAR TO SECOND'],
        [IntervalLiteral(day=1, microsecond=2), 'INTERVAL \'0-0 1 0:0:0.000002\' YEAR TO SECOND'],
        [IntervalLiteral(hour=1, minute=2), 'INTERVAL \'0-0 0 1:2:0.000000\' YEAR TO SECOND'],
        [IntervalLiteral(hour=1, second=2), 'INTERVAL \'0-0 0 1:0:2.000000\' YEAR TO SECOND'],
        [IntervalLiteral(hour=1, microsecond=2), 'INTERVAL \'0-0 0 1:0:0.000002\' YEAR TO SECOND'],
        [IntervalLiteral(minute=1, second=2), 'INTERVAL \'0-0 0 0:1:2.000000\' YEAR TO SECOND'],
        [IntervalLiteral(minute=1, microsecond=2), 'INTERVAL \'0-0 0 0:1:0.000002\' YEAR TO SECOND'],
        [IntervalLiteral(second=1, microsecond=2), 'INTERVAL \'0-0 0 0:0:1.000002\' YEAR TO SECOND'],
    ],
)
def test_interval_literal_to_sql(interval, expected):
    actual = interval_literal_to_sql(interval)
    assert actual == expected


@mark.parametrize(
    'column, expected',
    [
        [
            Column(
                name='col_bool',
                datatype=BOOL,
                nullable=False,
            ),
            '`col_bool` BOOL NOT NULL',
        ],
        [
            Column(
                name='col_date',
                datatype=DATE,
                default_expression='CURRENT_DATE',
                nullable=True,
            ),
            '`col_date` DATE DEFAULT CURRENT_DATE',
        ],
        [
            Column(
                name='col_bool',
                datatype=BOOL,
                nullable=True,
                description='Test description',
            ),
            '`col_bool` BOOL OPTIONS(description = \'Test description\')',
        ],
        [
            Column(
                name='col_struct',
                datatype=Struct(fields={
                    '`field1`': BOOL,
                    '`field2`': INT64,
                }),
                nullable=True,
            ),
            '`col_struct` STRUCT<`field1` BOOL, `field2` INT64>',
        ],
        [
            Column(
                name='col_numeric',
                datatype=Numeric(precision=2, scale=1),
                nullable=True,
            ),
            '`col_numeric` NUMERIC(2, 1)',
        ],
        [
            Column(
                name='col_bignumeric',
                datatype=BigNumeric(precision=2, scale=1),
                nullable=True,
            ),
            '`col_bignumeric` BIGNUMERIC(2, 1)',
        ],
        [
            Column(
                name='col_string',
                datatype=String(characters=1),
                nullable=True,
            ),
            '`col_string` STRING(1)',
        ],
        [
            Column(
                name='col_range',
                datatype=Range(kind='DATE'),
                nullable=True,
            ),
            '`col_range` RANGE<DATE>',
        ],
        [
            Column(
                name='col_range',
                datatype=Range(kind='DATETIME'),
                nullable=True,
            ),
            '`col_range` RANGE<DATETIME>',
        ],
        [
            Column(
                name='col_range',
                datatype=Range(kind='TIMESTAMP'),
                nullable=True,
            ),
            '`col_range` RANGE<TIMESTAMP>',
        ],
        [
            Column(
                name='col_float',
                datatype=FLOAT64,
                nullable=True,
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
            ),
            '`col_float` FLOAT64 OPTIONS(rounding_mode = \'ROUND_HALF_AWAY_FROM_ZERO\')',
        ],
        [
            Column(
                name='col_float',
                datatype=FLOAT64,
                nullable=True,
                rounding_mode='ROUND_HALF_EVEN',
            ),
            '`col_float` FLOAT64 OPTIONS(rounding_mode = \'ROUND_HALF_EVEN\')',
        ],
    ],
)
def test_column_to_sql(column, expected):
    actual = column_to_sql(column)
    assert actual == expected


@mark.parametrize(
    'column, expected',
    [
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                mode=REQUIRED,
            ),
            BOOL,
        ],
        [
            bq.SchemaField(
                name='col_date',
                field_type='DATE',
                default_value_expression='CURRENT_DATE',
            ),
            DATE,
        ],
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                description='Test description',
            ),
            BOOL,
        ],
        [
            bq.SchemaField(
                name='col_struct',
                field_type='RECORD',
                fields=(bq.SchemaField('field1', 'BOOL'), bq.SchemaField('field2', 'INT64')),
            ),
            Struct(fields={
                'field1': BOOL,
                'field2': INT64,
            }),
        ],
        [
            bq.SchemaField(
                name='col_numeric',
                field_type='NUMERIC',
                precision=2,
                scale=1,
            ),
            Numeric(precision=2, scale=1),
        ],
        [
            bq.SchemaField(
                name='col_bignumeric',
                field_type='BIGNUMERIC',
                precision=2,
                scale=1,
            ),
            BigNumeric(precision=2, scale=1),
        ],
        [
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
            ),
            STRING,
        ],
        [
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
                max_length=1,
            ),
            String(characters=1),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATE',
            ),
            Range(kind='DATE'),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATETIME',
            ),
            Range(kind='DATETIME'),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='TIMESTAMP',
            ),
            Range(kind='TIMESTAMP'),
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
            ),
            FLOAT64,
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_EVEN',
            ),
            FLOAT64,
        ],
    ],
)
def test_to_datatype(column, expected):
    actual = to_datatype(column)
    assert actual == expected


@mark.parametrize(
    'column, expected',
    [
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                mode=REQUIRED,
            ),
            BOOL,
        ],
        [
            bq.SchemaField(
                name='col_date',
                field_type='DATE',
                default_value_expression='CURRENT_DATE',
            ),
            DATE,
        ],
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                description='Test description',
            ),
            BOOL,
        ],
        [
            bq.SchemaField(
                name='col_struct',
                field_type='RECORD',
                fields=(bq.SchemaField('field1', 'BOOL'), bq.SchemaField('field2', 'INT64')),
            ),
            Struct(fields={
                'field1': BOOL,
                'field2': INT64,
            }),
        ],
        [
            bq.SchemaField(
                name='col_numeric',
                field_type='NUMERIC',
                precision=2,
                scale=1,
            ),
            Numeric(precision=2, scale=1),
        ],
        [
            bq.SchemaField(
                name='col_bignumeric',
                field_type='BIGNUMERIC',
                precision=2,
                scale=1,
            ),
            BigNumeric(precision=2, scale=1),
        ],
        [
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
            ),
            STRING,
        ],
        [
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
                max_length=1,
            ),
            String(characters=1),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATE',
            ),
            Range(kind='DATE'),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATETIME',
            ),
            Range(kind='DATETIME'),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='TIMESTAMP',
            ),
            Range(kind='TIMESTAMP'),
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
            ),
            FLOAT64,
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_EVEN',
            ),
            FLOAT64,
        ],
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                mode=REPEATED,
            ),
            Array(inner=BOOL),
        ],
        [
            bq.SchemaField(
                name='col_date',
                field_type='DATE',
                default_value_expression='CURRENT_DATE',
                mode=REPEATED,
            ),
            Array(inner=DATE),
        ],
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                description='Test description',
                mode=REPEATED,
            ),
            Array(inner=BOOL),
        ],
        [
            bq.SchemaField(
                name='col_struct',
                field_type='RECORD',
                fields=(bq.SchemaField('field1', 'BOOL'), bq.SchemaField('field2', 'INT64')),
                mode=REPEATED,
            ),
            Array(inner=Struct(fields={
                'field1': BOOL,
                'field2': INT64,
            })),
        ],
        [
            bq.SchemaField(
                name='col_numeric',
                field_type='NUMERIC',
                precision=2,
                scale=1,
                mode=REPEATED,
            ),
            Array(inner=Numeric(precision=2, scale=1)),
        ],
        [
            bq.SchemaField(
                name='col_bignumeric',
                field_type='BIGNUMERIC',
                precision=2,
                scale=1,
                mode=REPEATED,
            ),
            Array(inner=BigNumeric(precision=2, scale=1)),
        ],
        [
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
                mode=REPEATED,
            ),
            Array(inner=STRING),
        ],
        [
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
                max_length=1,
                mode=REPEATED,
            ),
            Array(inner=String(characters=1)),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATE',
                mode=REPEATED,
            ),
            Array(inner=Range(kind='DATE')),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATETIME',
                mode=REPEATED,
            ),
            Array(inner=Range(kind='DATETIME')),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='TIMESTAMP',
                mode=REPEATED,
            ),
            Array(inner=Range(kind='TIMESTAMP')),
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
                mode=REPEATED,
            ),
            Array(inner=FLOAT64),
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_EVEN',
                mode=REPEATED,
            ),
            Array(inner=FLOAT64),
        ],
    ],
)
def test_to_datatype_array(column, expected):
    actual = to_datatype_array(column)
    assert actual == expected


@mark.parametrize(
    'schema_field, expected',
    [
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                mode=REQUIRED,
            ),
            Column(
                name='col_bool',
                datatype=BOOL,
                nullable=False,
            ),
        ],
        [
            bq.SchemaField(
                name='col_date',
                field_type='DATE',
                default_value_expression='CURRENT_DATE',
            ),
            Column(
                name='col_date',
                datatype=DATE,
                default_expression='CURRENT_DATE',
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_bool',
                field_type='BOOL',
                description='Test description',
            ),
            Column(
                name='col_bool',
                datatype=BOOL,
                nullable=True,
                description='Test description',
            ),
        ],
        [
            bq.SchemaField(
                name='col_struct',
                field_type='RECORD',
                fields=(bq.SchemaField('field1', 'BOOL'), bq.SchemaField('field2', 'INT64')),
            ),
            Column(
                name='col_struct',
                datatype=Struct(fields={
                    'field1': BOOL,
                    'field2': INT64,
                }),
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_numeric',
                field_type='NUMERIC',
                precision=2,
                scale=1,
            ),
            Column(
                name='col_numeric',
                datatype=Numeric(precision=2, scale=1),
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_bignumeric',
                field_type='BIGNUMERIC',
                precision=2,
                scale=1,
            ),
            Column(
                name='col_bignumeric',
                datatype=BigNumeric(precision=2, scale=1),
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_string',
                field_type='STRING',
                max_length=1,
            ),
            Column(
                name='col_string',
                datatype=String(characters=1),
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATE',
            ),
            Column(
                name='col_range',
                datatype=Range(kind='DATE'),
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='DATETIME',
            ),
            Column(
                name='col_range',
                datatype=Range(kind='DATETIME'),
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_range',
                field_type='RANGE',
                range_element_type='TIMESTAMP',
            ),
            Column(
                name='col_range',
                datatype=Range(kind='TIMESTAMP'),
                nullable=True,
            ),
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
            ),
            Column(
                name='col_float',
                datatype=FLOAT64,
                nullable=True,
                rounding_mode='ROUND_HALF_AWAY_FROM_ZERO',
            ),
        ],
        [
            bq.SchemaField(
                name='col_float',
                field_type='FLOAT64',
                rounding_mode='ROUND_HALF_EVEN',
            ),
            Column(
                name='col_float',
                datatype=FLOAT64,
                nullable=True,
                rounding_mode='ROUND_HALF_EVEN',
            ),
        ],
    ],
)
def test_to_column(schema_field, expected):
    actual = to_column(schema_field)
    assert actual == expected


def test_to_liti_table():
    table = bq.Table('test_project.test_dataset.test_table', [bq.SchemaField('col_date', 'DATE', mode=REQUIRED)])
    table.table_constraints = bq.TableConstraints(
        primary_key=bq.PrimaryKey(['col_date']),
        foreign_keys=[
            bq.ForeignKey(
                name='fk_test',
                referenced_table=bq.TableReference(bq.DatasetReference('test_project', 'test_dataset'), 'fk_test_table'),
                column_references=[bq.ColumnReference('col_date', 'fk_col_date')],
            )
        ],
    )
    table.time_partitioning = bq.TimePartitioning(
        type_='DAY',
        field='col_date',
    )
    table.clustering_fields = ['col_date']
    table.friendly_name = 'test_friendly'
    table.description = 'test_description'
    table.labels = {'l1': 'v1'}
    table.resource_tags = {'t1': 'v1'}
    table.expires = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    expected = Table(
        name=TableName('test_project.test_dataset.test_table'),
        columns=[Column(name=ColumnName('col_date'), datatype=DATE)],
        primary_key=PrimaryKey(column_names=[ColumnName('col_date')]),
        foreign_keys=[ForeignKey(
            name=Identifier('fk_test'),
            foreign_table_name=TableName('test_project.test_dataset.fk_test_table'),
            references=[ForeignReference(
                local_column_name=ColumnName('col_date'),
                foreign_column_name=ColumnName('fk_col_date'),
            )],
            enforced=False,
        )],
        partitioning=Partitioning(
            kind='TIME',
            column=ColumnName('col_date'),
            time_unit='DAY',
        ),
        clustering=[ColumnName('col_date')],
        friendly_name='test_friendly',
        description='test_description',
        labels={'l1': 'v1'},
        tags={'t1': 'v1'},
        expiration_timestamp=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )

    actual = to_liti_table(table)
    assert actual == expected


@mark.parametrize(
    'from_dt, to_dt, expected',
    [
        [INT64, INT64, False],
        [INT64, FLOAT64, True],
        [INT64, Numeric(precision=1, scale=0), True],
        [INT64, BigNumeric(precision=1, scale=0), True],
        [INT64, STRING, False],
        [FLOAT64, INT64, False],
        [FLOAT64, FLOAT64, False],
        [FLOAT64, Numeric(precision=1, scale=0), False],
        [FLOAT64, BigNumeric(precision=1, scale=0), False],
        [FLOAT64, STRING, False],
        [Numeric(precision=1, scale=0), INT64, False],
        [Numeric(precision=1, scale=0), FLOAT64, True],
        [Numeric(precision=1, scale=0), Numeric(precision=1, scale=0), False],
        [Numeric(precision=1, scale=0), Numeric(precision=2, scale=0), True],
        [Numeric(precision=1, scale=0), Numeric(precision=1, scale=1), True],
        [Numeric(precision=1, scale=0), BigNumeric(precision=1, scale=0), True],
        [Numeric(precision=1, scale=0), BigNumeric(precision=2, scale=0), True],
        [Numeric(precision=1, scale=0), BigNumeric(precision=1, scale=1), True],
        [Numeric(precision=1, scale=0), STRING, False],
        [BigNumeric(precision=1, scale=0), INT64, False],
        [BigNumeric(precision=1, scale=0), FLOAT64, True],
        [BigNumeric(precision=1, scale=0), Numeric(precision=1, scale=0), False],
        [BigNumeric(precision=1, scale=0), Numeric(precision=2, scale=0), False],
        [BigNumeric(precision=1, scale=0), Numeric(precision=1, scale=1), False],
        [BigNumeric(precision=1, scale=0), BigNumeric(precision=1, scale=0), False],
        [BigNumeric(precision=1, scale=0), BigNumeric(precision=2, scale=0), True],
        [BigNumeric(precision=1, scale=0), BigNumeric(precision=1, scale=1), True],
        [BigNumeric(precision=1, scale=0), STRING, False],
        [STRING, INT64, False],
        [STRING, FLOAT64, False],
        [STRING, Numeric(precision=1, scale=0), False],
        [STRING, BigNumeric(precision=1, scale=0), False],
        [STRING, STRING, False],
        [STRING, String(characters=1), False],
        [STRING, String(characters=2), False],
        [String(characters=1), INT64, False],
        [String(characters=1), FLOAT64, False],
        [String(characters=1), Numeric(precision=1, scale=0), False],
        [String(characters=1), BigNumeric(precision=1, scale=0), False],
        [String(characters=1), STRING, True],
        [String(characters=1), String(characters=1), False],
        [String(characters=1), String(characters=2), True],
    ],
)
def test_can_coerce(from_dt, to_dt, expected):
    actual = can_coerce(from_dt, to_dt)
    assert actual == expected


def test_create_table(db_backend: BigQueryDbBackend, bq_client: Mock):
    table = Table(
        name=TableName('test_project.test_dataset.test_table'),
        columns=[Column(name=ColumnName('col_date'), datatype=DATE)],
        primary_key=PrimaryKey(column_names=[ColumnName('col_date')]),
        foreign_keys=[ForeignKey(
            name=Identifier('fk_test'),
            foreign_table_name=TableName('test_project.test_dataset.fk_test_table'),
            references=[ForeignReference(
                local_column_name=ColumnName('col_date'),
                foreign_column_name=ColumnName('fk_col_date'),
            )],
        )],
        partitioning=Partitioning(
            kind='TIME',
            column=ColumnName('col_date'),
            time_unit='DAY',
            expiration_days=1.5,
            require_filter=True,
        ),
        clustering=[ColumnName('col_date')],
        friendly_name='test_friendly',
        description='Test description',
        labels={'l1': 'v1', 'l2': 'v2'},
        tags={'t1': 'v1', 't2': 'v2'},
        expiration_timestamp=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        default_rounding_mode=RoundingModeLiteral('ROUND_HALF_AWAY_FROM_ZERO'),
        max_staleness=IntervalLiteral(hour=1),
        enable_change_history=True,
        enable_fine_grained_mutations=True,
        kms_key_name='test_key',
        big_lake=BigLake(
            connection_id='test_connection',
            storage_uri='test/uri',
        ),
    )

    db_backend.create_table(table)

    bq_client.query_and_wait.assert_called_once_with(
        f'CREATE TABLE `test_project.test_dataset.test_table` (\n'
        f'    `col_date` DATE NOT NULL,\n'
        f'    PRIMARY KEY (`col_date`) NOT ENFORCED,\n'
        f'    CONSTRAINT `fk_test` FOREIGN KEY (`col_date`) REFERENCES `test_project.test_dataset.fk_test_table` (`fk_col_date`) NOT ENFORCED\n'
        f')\n'
        f'PARTITION BY `col_date`\n'
        f'CLUSTER BY `col_date`\n'
        f'WITH CONNECTION `test_connection`\n'
        f'OPTIONS(\n'
        f'    partition_expiration_days = 1.5,\n'
        f'    require_partition_filter = TRUE,\n'
        f'    friendly_name = \'test_friendly\',\n'
        f'    description = \'Test description\',\n'
        f'    labels = [(\'l1\', \'v1\'), (\'l2\', \'v2\')],\n'
        f'    tags = [(\'t1\', \'v1\'), (\'t2\', \'v2\')],\n'
        f'    expiration_timestamp = TIMESTAMP \'2025-01-01 00:00:00 UTC\',\n'
        f'    default_rounding_mode = \'ROUND_HALF_AWAY_FROM_ZERO\',\n'
        f'    max_staleness = INTERVAL \'0-0 0 1:0:0.000000\' YEAR TO SECOND,\n'
        f'    enable_change_history = TRUE,\n'
        f'    enable_fine_grained_mutations = TRUE,\n'
        f'    kms_key_name = \'test_key\',\n'
        f'    storage_uri = \'test/uri\',\n'
        f'    file_format = PARQUET,\n'
        f'    table_format = ICEBERG\n'
        f')\n'
    )


@mark.parametrize(
    'column_names, expected',
    [
        [
            None,
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'DROP PRIMARY KEY\n',
        ],
        [
            ['col_date', 'col_int'],
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'ADD PRIMARY KEY (`col_date`, `col_int`) NOT ENFORCED\n',
        ],
    ],
)
def test_set_primary_key(db_backend: BigQueryDbBackend, bq_client: Mock, column_names, expected):
    table_name = TableName('test_project.test_dataset.test_table')

    if column_names:
        primary_key = PrimaryKey(column_names=[ColumnName('col_date'), ColumnName('col_int')])
    else:
        primary_key = None

    db_backend.set_primary_key(table_name, primary_key)
    bq_client.query_and_wait.assert_called_once_with(expected)


def test_add_foreign_key(db_backend: BigQueryDbBackend, bq_client: Mock):
    table_name = TableName('test_project.test_dataset.test_table')

    foreign_key = ForeignKey(
        foreign_table_name=TableName('test_project.test_dataset.fk_test_table'),
        references=[
            ForeignReference(
                local_column_name=ColumnName('col_date'),
                foreign_column_name=ColumnName('fk_col_date'),
            ),
            ForeignReference(
                local_column_name=ColumnName('col_int'),
                foreign_column_name=ColumnName('fk_col_int'),
            ),
        ],
    )

    db_backend.add_foreign_key(table_name, foreign_key)

    bq_client.query_and_wait.assert_called_once_with(
        f'ALTER TABLE `test_project.test_dataset.test_table`\n'
        f'ADD CONSTRAINT `fk__col_date_col_int__test_project_test_dataset_fk_test_table__fk_col_date_fk_col_int`'
        f' FOREIGN KEY (`col_date`, `col_int`)'
        f' REFERENCES `test_project.test_dataset.fk_test_table` (`fk_col_date`, `fk_col_int`)'
        f' NOT ENFORCED\n'
    )


def test_drop_constraint(db_backend: BigQueryDbBackend, bq_client: Mock):
    table_name = TableName('test_project.test_dataset.test_table')
    constraint_name = Identifier('fk__test')
    db_backend.drop_constraint(table_name, constraint_name)

    bq_client.query_and_wait.assert_called_once_with(
        f'ALTER TABLE `test_project.test_dataset.test_table`\n'
        f'DROP CONSTRAINT `fk__test`\n'
    )


@mark.parametrize(
    'description, expected',
    [
        [
            None,
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(description = NULL)\n',
        ],
        [
            'Test description',
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(description = \'Test description\')\n',
        ],
    ],
)
def test_set_description(db_backend: BigQueryDbBackend, bq_client: Mock, description, expected):
    table_name = TableName('test_project.test_dataset.test_table')
    db_backend.set_description(table_name, description)
    bq_client.query_and_wait.assert_called_once_with(expected)


@mark.parametrize(
    'labels, expected',
    [
        [
            None,
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(labels = NULL)\n',
        ],
        [
            {'l1': 'v1', 'l2': 'v2'},
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(labels = [(\'l1\', \'v1\'), (\'l2\', \'v2\')])\n',
        ],
    ],
)
def test_set_labels(db_backend: BigQueryDbBackend, bq_client: Mock, labels, expected):
    table_name = TableName('test_project.test_dataset.test_table')
    db_backend.set_labels(table_name, labels)
    bq_client.query_and_wait.assert_called_once_with(expected)


@mark.parametrize(
    'tags, expected',
    [
        [
            None,
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(tags = NULL)\n',
        ],
        [
            {'t1': 'v1', 't2': 'v2'},
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(tags = [(\'t1\', \'v1\'), (\'t2\', \'v2\')])\n',
        ],
    ],
)
def test_set_tags(db_backend: BigQueryDbBackend, bq_client: Mock, tags, expected):
    table_name = TableName('test_project.test_dataset.test_table')
    db_backend.set_tags(table_name, tags)
    bq_client.query_and_wait.assert_called_once_with(expected)


@mark.parametrize(
    'default_rounding_mode, expected',
    [
        [
            None,
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(default_rounding_mode = NULL)\n',
        ],
        [
            RoundingModeLiteral('ROUND_HALF_AWAY_FROM_ZERO'),
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(default_rounding_mode = \'ROUND_HALF_AWAY_FROM_ZERO\')\n',
        ],
        [
            RoundingModeLiteral('ROUND_HALF_EVEN'),
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'SET OPTIONS(default_rounding_mode = \'ROUND_HALF_EVEN\')\n',
        ],
    ],
)
def test_set_default_rounding_mode(db_backend: BigQueryDbBackend, bq_client: Mock, default_rounding_mode, expected):
    table_name = TableName('test_project.test_dataset.test_table')
    db_backend.set_default_rounding_mode(table_name, default_rounding_mode)
    bq_client.query_and_wait.assert_called_once_with(expected)


def test_add_column(db_backend: BigQueryDbBackend, bq_client: Mock):
    table_name = TableName('test_project.test_dataset.test_table')
    column = Column(name=ColumnName('col_date'), datatype=DATE)
    db_backend.add_column(table_name, column)

    bq_client.query_and_wait.assert_called_once_with(
        f'ALTER TABLE `test_project.test_dataset.test_table`\n'
        f'ADD COLUMN `col_date` DATE NOT NULL\n'
    )


def test_rename_column(db_backend: BigQueryDbBackend, bq_client: Mock):
    table_name = TableName('test_project.test_dataset.test_table')
    column_name = ColumnName('col_date')
    column_rename = ColumnName('col_date_renamed')
    db_backend.rename_column(table_name, column_name, column_rename)

    bq_client.query_and_wait.assert_called_once_with(
        f'ALTER TABLE `test_project.test_dataset.test_table`\n'
        f'RENAME COLUMN `col_date` TO `col_date_renamed`\n'
    )


def test_set_column_datatype(db_backend: BigQueryDbBackend, bq_client: Mock):
    table_name = TableName('test_project.test_dataset.test_table')
    column_name = ColumnName('col_num')
    from_dt = INT64
    to_dt = FLOAT64
    db_backend.set_column_datatype(table_name, column_name, from_dt, to_dt)

    bq_client.query_and_wait.assert_called_once_with(
        f'ALTER TABLE `test_project.test_dataset.test_table`\n'
        f'ALTER COLUMN `col_num`\n'
        f'SET DATA TYPE FLOAT64\n'
    )


def test_set_column_nullable(db_backend: BigQueryDbBackend, bq_client: Mock):
    table_name = TableName('test_project.test_dataset.test_table')
    column_name = ColumnName('col_num')
    nullable = True
    db_backend.set_column_nullable(table_name, column_name, nullable)

    bq_client.query_and_wait.assert_called_once_with(
        f'ALTER TABLE `test_project.test_dataset.test_table`\n'
        f'ALTER COLUMN `col_num`\n'
        f'DROP NOT NULL\n'
    )


@mark.parametrize(
    'description, expected',
    [
        [
            None,
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'ALTER COLUMN `col_num`\n'
            f'SET OPTIONS(description = NULL)\n',
        ],
        [
            'Test description',
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'ALTER COLUMN `col_num`\n'
            f'SET OPTIONS(description = \'Test description\')\n',
        ],
    ],
)
def test_set_column_description(db_backend: BigQueryDbBackend, bq_client: Mock, description, expected):
    table_name = TableName('test_project.test_dataset.test_table')
    column_name = ColumnName('col_num')
    db_backend.set_column_description(table_name, column_name, description)
    bq_client.query_and_wait.assert_called_once_with(expected)


@mark.parametrize(
    'rounding_mode, expected',
    [
        [
            'ROUND_HALF_AWAY_FROM_ZERO',
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'ALTER COLUMN `col_num`\n'
            f'SET OPTIONS(rounding_mode = \'ROUND_HALF_AWAY_FROM_ZERO\')\n',
        ],
        [
            'ROUND_HALF_EVEN',
            f'ALTER TABLE `test_project.test_dataset.test_table`\n'
            f'ALTER COLUMN `col_num`\n'
            f'SET OPTIONS(rounding_mode = \'ROUND_HALF_EVEN\')\n',
        ],
    ],
)
def test_set_column_rounding_mode(db_backend: BigQueryDbBackend, bq_client: Mock, rounding_mode, expected):
    table_name = TableName('test_project.test_dataset.test_table')
    column_name = ColumnName('col_num')
    db_backend.set_column_rounding_mode(table_name, column_name, rounding_mode)
    bq_client.query_and_wait.assert_called_once_with(expected)


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
        [None, raises(ValueError)],
        [8, raises(ValueError)],
        [16, raises(ValueError)],
        [32, raises(ValueError)],
        [64, NoRaise()],
        [128, raises(ValueError)],
    ],
)
def test_validate_int(db_backend: BigQueryDbBackend, bits: int | None, raise_ctx):
    node = Int(bits=bits)

    with raise_ctx:
        node.liti_validate(db_backend)


@mark.parametrize(
    'bits, raise_ctx',
    [
        [None, raises(ValueError)],
        [8, raises(ValueError)],
        [16, raises(ValueError)],
        [32, raises(ValueError)],
        [64, NoRaise()],
        [128, raises(ValueError)],
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
            [precision, scale, NoRaise()]
            for scale in [0, 4, 9]
            for precision in [max(scale, 1), (max(scale, 1) + 38) // 2, 38]
        ],
        *[
            [precision, scale, raises(ValueError)]
            for scale in [0, 4, 9]
            for precision in [0, (0 + scale - 1) // 2, scale - 1]
        ],
        *[
            [precision, scale, raises(ValueError)]
            for scale in [0, 4, 9]
            for precision in [scale + 30, (scale + 30 + 39) // 2, 39]
        ],
        *[
            [precision, scale, raises(ValueError)]
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
            [precision, scale, NoRaise()]
            for scale in [0, 19, 38]
            for precision in [max(scale, 1), (max(scale, 1) + 76) // 2, 76]
        ],
        *[
            [precision, scale, raises(ValueError)]
            for scale in [0, 19, 38]
            for precision in [0, (0 + scale - 1) // 2, scale - 1]
        ],
        *[
            [precision, scale, raises(ValueError)]
            for scale in [0, 19, 38]
            for precision in [scale + 39, (scale + 39 + 77) // 2, 77]
        ],
        *[
            [precision, scale, raises(ValueError)]
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
        [BOOL, NoRaise()],
        [INT64, NoRaise()],
        [FLOAT64, NoRaise()],
        [GEOGRAPHY, NoRaise()],
        [Numeric(), NoRaise()],
        [BigNumeric(), NoRaise()],
        [STRING, NoRaise()],
        [JSON, NoRaise()],
        [DATE, NoRaise()],
        [TIME, NoRaise()],
        [DATE_TIME, NoRaise()],
        [TIMESTAMP, NoRaise()],
        [Range(kind='DATE'), NoRaise()],
        [Range(kind='DATETIME'), NoRaise()],
        [Range(kind='TIMESTAMP'), NoRaise()],
        [INTERVAL, NoRaise()],
        [Array(inner=BOOL), raises(ValueError)],
        [Struct(fields={'field': BOOL}), NoRaise()],
    ],
)
def test_validate_array(db_backend: BigQueryDbBackend, inner: Datatype, raise_ctx):
    node = Array(inner=inner)

    with raise_ctx:
        node.liti_validate(db_backend)


@mark.parametrize(
    'kind, time_unit, int_start, int_end, int_step, raise_ctx',
    [
        *[
            ['TIME', time_unit, None, None, None, NoRaise()]
            for time_unit in ['YEAR', 'MONTH', 'DAY', 'HOUR']
        ],
        ['TIME', None, None, None, None, raises(ValueError)],
        ['TIME', 'YEAR', 0, None, None, raises(ValueError)],
        ['TIME', 'YEAR', None, 0, None, raises(ValueError)],
        ['TIME', 'YEAR', None, None, 0, raises(ValueError)],
        *[
            ['INT', None, None, None, None, NoRaise()]
            for int_start in range(3)
            for int_end in range(int_start + 1, int_start + 4)
            for int_step in range(max(int_start, int_start))
        ],
        ['INT', None, None, 1, 1, raises(ValueError)],
        ['INT', None, 0, None, 1, raises(ValueError)],
        ['INT', None, 0, 1, None, raises(ValueError)],
        ['INT', 'YEAR', 0, 1, 1, raises(ValueError)],
        ['INT', 'MONTH', 0, 1, 1, raises(ValueError)],
        ['INT', 'DAY', 0, 1, 1, raises(ValueError)],
        ['INT', 'HOUR', 0, 1, 1, raises(ValueError)],
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
