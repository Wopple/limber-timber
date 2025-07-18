from datetime import datetime

from pytest import fixture

from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.model.v1.datatype import Array, BigNumeric, BOOL, DATE, DATE_TIME, FLOAT64, GEOGRAPHY, INT64, JSON, \
    Numeric, Range, STRING, Struct, TIME, TIMESTAMP
from liti.core.model.v1.schema import Column, ColumnName, IntervalLiteral, Partitioning, RoundingModeLiteral, Table, \
    TableName
from liti.core.runner import MigrateRunner


@fixture
def db_backend() -> MemoryDbBackend:
    return MemoryDbBackend()


@fixture
def meta_backend() -> MemoryMetaBackend:
    return MemoryMetaBackend()


def test_all_datatypes(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.datatypes_table')

    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_all_datatypes',
    )

    up_runner.run(wet_run=True)
    datatypes_table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2

    assert datatypes_table == Table(
        name=table_name,
        columns=[
            Column(name=ColumnName('col_bool'), datatype=BOOL),
            Column(name=ColumnName('col_int_64'), datatype=INT64),
            Column(name=ColumnName('col_float_64'), datatype=FLOAT64),
            Column(name=ColumnName('col_int_64_bits'), datatype=INT64),
            Column(name=ColumnName('col_float_64_bits'), datatype=FLOAT64),
            Column(name=ColumnName('col_geography'), datatype=GEOGRAPHY),
            Column(name=ColumnName('col_numeric'), datatype=Numeric(precision=38, scale=9)),
            Column(name=ColumnName('col_big_numeric'), datatype=BigNumeric(precision=76, scale=38)),
            Column(name=ColumnName('col_string'), datatype=STRING),
            Column(name=ColumnName('col_json'), datatype=JSON),
            Column(name=ColumnName('col_date'), datatype=DATE),
            Column(name=ColumnName('col_time'), datatype=TIME),
            Column(name=ColumnName('col_date_time'), datatype=DATE_TIME),
            Column(name=ColumnName('col_timestamp'), datatype=TIMESTAMP),
            Column(name=ColumnName('col_range_date'), datatype=Range(kind='DATE')),
            Column(name=ColumnName('col_range_datetime'), datatype=Range(kind='DATETIME')),
            Column(name=ColumnName('col_range_timestamp'), datatype=Range(kind='TIMESTAMP')),
            Column(name=ColumnName('col_array'), datatype=Array(inner=Struct(fields={'field_bool': BOOL}))),
            Column(
                name=ColumnName('col_struct'),
                datatype=Struct(fields={'field_bool': BOOL, 'field_array': Array(inner=BOOL)}),
            ),
        ],
    )

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name) is None


def test_create_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.create_table')

    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_create_table',
    )

    up_runner.run(wet_run=True)
    create_table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert create_table is not None
    assert create_table.clustering == [ColumnName('col2'), ColumnName('col1')]
    assert create_table.friendly_name == 'my_friendly_name'
    assert create_table.description == 'My description'
    assert create_table.labels == {'key_1': 'value_1', 'key_2': 'value_2'}
    assert create_table.tags == {'key_3': 'value_3', 'key_4': 'value_4'}
    assert create_table.expiration_timestamp == datetime(2000, 1, 2, 3, 4, 5)
    assert create_table.enable_change_history is True
    assert create_table.enable_fine_grained_mutations is True
    assert create_table.kms_key_name == 'my/kms/key/name'
    assert create_table.storage_uri == 'https://my.storage/uri'
    assert create_table.file_format == 'PARQUET'
    assert create_table.table_format == 'ICEBERG'

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name) is None


def test_drop_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_drop_table',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.tables) == 0
    assert len(meta_backend.get_applied_operations()) == 2

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)
    revert_table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1

    assert revert_table == Table(
        name=table_name,
        columns=[
            Column(name='col_bool', datatype=BOOL),
        ],
    )


def test_all_max_staleness(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_all_max_staleness',
    )

    up_runner.run(wet_run=True)
    all_pos_table = db_backend.get_table(TableName('my_project.my_dataset.all_pos_table'))
    all_neg_table = db_backend.get_table(TableName('my_project.my_dataset.all_neg_table'))
    only_year_table = db_backend.get_table(TableName('my_project.my_dataset.only_year_table'))
    only_month_table = db_backend.get_table(TableName('my_project.my_dataset.only_month_table'))
    only_day_table = db_backend.get_table(TableName('my_project.my_dataset.only_day_table'))
    only_hour_table = db_backend.get_table(TableName('my_project.my_dataset.only_hour_table'))
    only_minute_table = db_backend.get_table(TableName('my_project.my_dataset.only_minute_table'))
    only_second_table = db_backend.get_table(TableName('my_project.my_dataset.only_second_table'))
    only_microsecond_table = db_backend.get_table(TableName('my_project.my_dataset.only_microsecond_table'))

    assert len(db_backend.tables) == 10
    assert len(meta_backend.get_applied_operations()) == 10

    assert all_pos_table.max_staleness == IntervalLiteral(
        year=1,
        month=2,
        day=3,
        hour=4,
        minute=5,
        second=6,
        microsecond=7,
        sign='+',
    )

    assert all_neg_table.max_staleness == IntervalLiteral(
        year=1,
        month=2,
        day=3,
        hour=4,
        minute=5,
        second=6,
        microsecond=7,
        sign='-',
    )

    assert only_year_table.max_staleness == IntervalLiteral(year=1)
    assert only_month_table.max_staleness == IntervalLiteral(month=1)
    assert only_day_table.max_staleness == IntervalLiteral(day=1)
    assert only_hour_table.max_staleness == IntervalLiteral(hour=1)
    assert only_minute_table.max_staleness == IntervalLiteral(minute=1)
    assert only_second_table.max_staleness == IntervalLiteral(second=1)
    assert only_microsecond_table.max_staleness == IntervalLiteral(microsecond=1)

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.all_pos_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.all_neg_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.only_year_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.only_month_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.only_day_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.only_hour_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.only_minute_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.only_second_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.only_microsecond_table')) is None


def test_all_partitioning(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_all_partitioning',
    )

    up_runner.run(wet_run=True)
    time_all_table = db_backend.get_table(TableName('my_project.my_dataset.time_all_table'))
    year_ingest_table = db_backend.get_table(TableName('my_project.my_dataset.year_ingest_table'))
    month_ingest_table = db_backend.get_table(TableName('my_project.my_dataset.month_ingest_table'))
    day_ingest_table = db_backend.get_table(TableName('my_project.my_dataset.day_ingest_table'))
    hour_ingest_table = db_backend.get_table(TableName('my_project.my_dataset.hour_ingest_table'))
    int_all_table = db_backend.get_table(TableName('my_project.my_dataset.int_all_table'))

    assert len(db_backend.tables) == 7
    assert len(meta_backend.get_applied_operations()) == 7

    assert time_all_table.partitioning == Partitioning(
        kind='TIME',
        column=ColumnName('col_date'),
        time_unit='YEAR',
        expiration_days=1,
        require_filter=True,
    )

    assert year_ingest_table.partitioning == Partitioning(
        kind='TIME',
        time_unit='YEAR',
    )

    assert month_ingest_table.partitioning == Partitioning(
        kind='TIME',
        time_unit='MONTH',
    )

    assert day_ingest_table.partitioning == Partitioning(
        kind='TIME',
        time_unit='DAY',
    )

    assert hour_ingest_table.partitioning == Partitioning(
        kind='TIME',
        time_unit='HOUR',
    )

    assert int_all_table.partitioning == Partitioning(
        kind='INT',
        column=ColumnName('col_int'),
        int_start=0,
        int_end=8,
        int_step=2,
        require_filter=True,
    )

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.time_all_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.year_ingest_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.month_ingest_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.day_ingest_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.hour_ingest_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.int_all_table')) is None


def test_all_rounding_mode(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_all_rounding_mode',
    )

    up_runner.run(wet_run=True)
    round_half_away_from_zero_table = db_backend.get_table(TableName('my_project.my_dataset.round_half_away_from_zero_table'))
    round_half_even_table = db_backend.get_table(TableName('my_project.my_dataset.round_half_even_table'))

    assert len(db_backend.tables) == 3
    assert len(meta_backend.get_applied_operations()) == 3
    assert round_half_away_from_zero_table.default_rounding_mode == RoundingModeLiteral('ROUND_HALF_AWAY_FROM_ZERO')
    assert round_half_even_table.default_rounding_mode == RoundingModeLiteral('ROUND_HALF_EVEN')

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.round_half_away_from_zero_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.round_half_even_table')) is None


def test_rename_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_rename_table',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName('my_project.my_dataset.revert_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.renamed_table')) is not None

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.revert_table')) is not None
    assert db_backend.get_table(TableName('my_project.my_dataset.renamed_table')) is None


def test_set_clustering(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.clustering_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_clustering',
    )

    set_runner.run(wet_run=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 3

    assert db_backend.get_table(table_name).clustering == [
        ColumnName('col_int'),
        ColumnName('col_bool'),
    ]

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_clustering',
    )

    unset_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(table_name).clustering == [
        ColumnName('col_bool'),
        ColumnName('col_int'),
    ]

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name) is None


def test_set_description(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_description',
    )

    set_runner.run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(table_name).description == 'bar'

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_description',
    )

    unset_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).description == 'foo'

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).description is None


def test_set_labels(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_labels',
    )

    set_runner.run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3

    assert db_backend.get_table(table_name).labels == {
        'l2': 'v2',
        'l3': 'v3',
    }

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_labels',
    )

    unset_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(table_name).labels == {
        'l1': 'v1',
        'l2': 'v2',
    }

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).labels is None


def test_set_tags(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_tags',
    )

    set_runner.run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3

    assert db_backend.get_table(table_name).tags == {
        't2': 'v2',
        't3': 'v3',
    }

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_tags',
    )

    unset_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(table_name).tags == {
        't1': 'v1',
        't2': 'v2',
    }

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).tags is None


def test_set_default_rounding_mode(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_default_rounding_mode',
    )

    set_runner.run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert table.default_rounding_mode == RoundingModeLiteral('ROUND_HALF_EVEN')

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_default_rounding_mode',
    )

    unset_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert table.default_rounding_mode == RoundingModeLiteral('ROUND_HALF_AWAY_FROM_ZERO')

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.default_rounding_mode == RoundingModeLiteral()


def test_add_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_add_column',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.get_table(table_name).columns) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('add_col') in db_backend.get_table(table_name).column_map

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('add_col') not in db_backend.get_table(table_name).column_map


def test_drop_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_drop_column',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.get_table(table_name).columns) == 0
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col_bool') not in db_backend.get_table(table_name).column_map

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col_bool') in db_backend.get_table(table_name).column_map


def test_rename_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_rename_column',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col_bool') not in db_backend.get_table(table_name).column_map
    assert ColumnName('col_renamed') in db_backend.get_table(table_name).column_map

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col_bool') in db_backend.get_table(table_name).column_map
    assert ColumnName('col_renamed') not in db_backend.get_table(table_name).column_map


def test_set_column_datatype(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.datatype_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_column_datatype',
    )

    set_runner.run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 4
    assert table.column_map[ColumnName('col_int')].datatype == Numeric(precision=1, scale=1)
    assert table.column_map[ColumnName('col_numeric')].datatype == FLOAT64
    assert table.column_map[ColumnName('col_bignumeric')].datatype == BigNumeric(precision=12, scale=8)

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_column_datatype',
    )

    unset_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.column_map[ColumnName('col_int')].datatype == INT64
    assert table.column_map[ColumnName('col_numeric')].datatype == Numeric(precision=4, scale=2)
    assert table.column_map[ColumnName('col_bignumeric')].datatype == BigNumeric(precision=8, scale=4)


def test_add_column_field(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.column_field_table')

    add_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_add_column_field',
    )

    add_runner.run(wet_run=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert col_struct.fields['field_string'] == STRING
    assert col_struct.fields['field_array_struct'].inner.fields['field_array_string'].inner == STRING

    unadd_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unadd_column_field',
    )

    unadd_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert 'field_string' not in col_struct.fields
    assert 'field_array_string' not in col_struct.fields['field_array_struct'].inner.fields


def test_drop_column_field(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.column_field_table')

    drop_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_drop_column_field',
    )

    drop_runner.run(wet_run=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 5
    assert 'field_string' not in col_struct.fields
    assert 'field_array_string' not in col_struct.fields['field_array_struct'].inner.fields

    add_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_add_column_field',
    )

    add_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert col_struct.fields['field_string'] == STRING
    assert col_struct.fields['field_array_struct'].inner.fields['field_array_string'].inner == STRING


def test_set_column_nullable(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_column_nullable',
    )

    set_runner.run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert table.column_map[ColumnName('col_bool')].nullable == False

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_column_nullable',
    )

    unset_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert table.column_map[ColumnName('col_bool')].nullable == True

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.column_map[ColumnName('col_bool')].nullable == False


def test_set_column_description(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    table_name = TableName('my_project.my_dataset.revert_table')

    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_set_column_description',
    )

    set_runner.run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert table.column_map[ColumnName('col_bool')].description == 'bar'

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_unset_column_description',
    )

    unset_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert table.column_map[ColumnName('col_bool')].description == 'foo'

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='tests/res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.column_map[ColumnName('col_bool')].description is None
