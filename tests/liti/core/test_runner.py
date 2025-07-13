from datetime import datetime
from pytest import fixture

from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.model.v1.datatype import Array, BigNumeric, BOOL, DATE, DATE_TIME, FLOAT64, GEOGRAPHY, INT64, JSON, \
    Numeric, Range, \
    STRING, \
    Struct, \
    TIME, TIMESTAMP
from liti.core.model.v1.schema import Column, ColumnName, IntervalLiteral, Partitioning, Table, TableName
from liti.core.runner import MigrateRunner

@fixture
def db_backend() -> MemoryDbBackend:
    return MemoryDbBackend()


@fixture
def meta_backend() -> MemoryMetaBackend:
    return MemoryMetaBackend()


def test_all_data_types(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_all_data_types',
    )

    up_runner.run(wet_run=True)
    data_types_table = db_backend.get_table(TableName('my_project.my_dataset.data_types_table'))

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2

    assert data_types_table == Table(
        name=TableName('my_project.my_dataset.data_types_table'),
        columns=[
            Column(name=ColumnName('col_bool'), data_type=BOOL),
            Column(name=ColumnName('col_int_64'), data_type=INT64),
            Column(name=ColumnName('col_float_64'), data_type=FLOAT64),
            Column(name=ColumnName('col_int_64_bits'), data_type=INT64),
            Column(name=ColumnName('col_float_64_bits'), data_type=FLOAT64),
            Column(name=ColumnName('col_geography'), data_type=GEOGRAPHY),
            Column(name=ColumnName('col_numeric'), data_type=Numeric(precision=38, scale=9)),
            Column(name=ColumnName('col_big_numeric'), data_type=BigNumeric(precision=76, scale=38)),
            Column(name=ColumnName('col_string'), data_type=STRING),
            Column(name=ColumnName('col_json'), data_type=JSON),
            Column(name=ColumnName('col_date'), data_type=DATE),
            Column(name=ColumnName('col_time'), data_type=TIME),
            Column(name=ColumnName('col_date_time'), data_type=DATE_TIME),
            Column(name=ColumnName('col_timestamp'), data_type=TIMESTAMP),
            Column(name=ColumnName('col_range_date'), data_type=Range(kind='DATE')),
            Column(name=ColumnName('col_range_datetime'), data_type=Range(kind='DATETIME')),
            Column(name=ColumnName('col_range_timestamp'), data_type=Range(kind='TIMESTAMP')),
            Column(name=ColumnName('col_array'), data_type=Array(inner=Struct(fields={'field_bool': BOOL}))),
            Column(
                name=ColumnName('col_struct'),
                data_type=Struct(fields={'field_bool': BOOL, 'field_array': Array(inner=BOOL)}),
            ),
        ],
    )

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.data_types_table')) is None


def test_create_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_create_table',
    )

    up_runner.run(wet_run=True)
    create_table = db_backend.get_table(TableName('my_project.my_dataset.create_table'))

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
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.create_table')) is None


def test_drop_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_drop_table',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.tables) == 0
    assert len(meta_backend.get_applied_operations()) == 2

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)
    revert_table = db_backend.get_table(TableName('my_project.my_dataset.revert_table'))

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1

    assert revert_table == Table(
        name=TableName('my_project.my_dataset.revert_table'),
        columns=[
            Column(name='col_bool', data_type=BOOL),
        ],
    )


def test_all_max_staleness(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_all_max_staleness',
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
        target='res/target_revert',
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
        target='res/target_all_partitioning',
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
        target='res/target_revert',
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
        target='res/target_all_rounding_mode',
    )

    up_runner.run(wet_run=True)
    round_half_away_from_zero_table = db_backend.get_table(TableName('my_project.my_dataset.round_half_away_from_zero_table'))
    round_half_even_table = db_backend.get_table(TableName('my_project.my_dataset.round_half_even_table'))

    assert len(db_backend.tables) == 3
    assert len(meta_backend.get_applied_operations()) == 3

    assert round_half_away_from_zero_table.default_rounding_mode == 'ROUND_HALF_AWAY_FROM_ZERO'
    assert round_half_even_table.default_rounding_mode == 'ROUND_HALF_EVEN'

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
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
        target='res/target_rename_table',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName('my_project.my_dataset.revert_table')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.renamed_table')) is not None

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.revert_table')) is not None
    assert db_backend.get_table(TableName('my_project.my_dataset.renamed_table')) is None


def test_set_clustering(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    set_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_set_clustering',
    )

    set_runner.run(wet_run=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 3

    assert db_backend.get_table(TableName('my_project.my_dataset.clustering_table')).clustering == [
        ColumnName('col_int'),
        ColumnName('col_bool'),
    ]

    unset_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_unset_clustering',
    )

    unset_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(TableName('my_project.my_dataset.clustering_table')).clustering == [
        ColumnName('col_bool'),
        ColumnName('col_int'),
    ]

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.clustering_table')) is None


def test_add_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_add_column',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.revert_table')).columns) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('add_col') in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.revert_table')).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('add_col') not in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map


def test_drop_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_drop_column',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.revert_table')).columns) == 0
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col_bool') not in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.revert_table')).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col_bool') in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map


def test_rename_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend):
    up_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_rename_column',
    )

    up_runner.run(wet_run=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.revert_table')).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col_bool') not in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map
    assert ColumnName('col_renamed') in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map

    down_runner = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    down_runner.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.revert_table')).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col_bool') in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map
    assert ColumnName('col_renamed') not in db_backend.get_table(TableName('my_project.my_dataset.revert_table')).column_map
