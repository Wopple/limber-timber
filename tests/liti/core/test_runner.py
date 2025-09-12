from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from pytest import fixture, mark

from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.context import Context
from liti.core.model.v1.datatype import Array, BigNumeric, BOOL, DATE, DATE_TIME, FLOAT64, GEOGRAPHY, INT64, JSON, \
    Numeric, Range, STRING, Struct, TIME, TIMESTAMP
from liti.core.model.v1.operation.data.column import AddColumn
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.parse import parse_templates
from liti.core.model.v1.schema import Column, ColumnName, ForeignKey, ForeignReference, IntervalLiteral, Partitioning, \
    PrimaryKey, RoundingMode, Table, QualifiedName
from liti.core.model.v1.template import Template
from liti.core.runner import apply_templates, MigrateRunner, sort_operations

MakeRunner = Callable[[str], MigrateRunner]
TemplateMakeRunner = Callable[[str, list[Path]], MigrateRunner]


@fixture
def db_backend() -> MemoryDbBackend:
    return MemoryDbBackend()


@fixture
def meta_backend() -> MemoryMetaBackend:
    return MemoryMetaBackend()


@fixture
def make_runner(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend) -> MakeRunner | TemplateMakeRunner:
    return lambda filename, template_files=None: MigrateRunner(context=Context(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target_dir=Path(f'tests/res/{filename}'),
        template_files=template_files,
    ))


def test_all_datatypes(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.datatypes_table')

    make_runner('target_all_datatypes').run(wet_run=True)
    datatypes_table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2

    assert datatypes_table == Table(
        name=table_name,
        columns=[
            Column('col_bool', BOOL),
            Column('col_int_64', INT64),
            Column('col_float_64', FLOAT64),
            Column('col_int_64_bits', INT64),
            Column('col_float_64_bits', FLOAT64),
            Column('col_geography', GEOGRAPHY),
            Column('col_numeric', Numeric(precision=38, scale=9)),
            Column('col_big_numeric', BigNumeric(precision=76, scale=38)),
            Column('col_string', STRING),
            Column('col_json', JSON),
            Column('col_date', DATE),
            Column('col_time', TIME),
            Column('col_date_time', DATE_TIME),
            Column('col_timestamp', TIMESTAMP),
            Column('col_range_date', Range(kind='DATE')),
            Column('col_range_datetime', Range(kind='DATETIME')),
            Column('col_range_timestamp', Range(kind='TIMESTAMP')),
            Column('col_array', Array(inner=Struct(fields={'field_bool': BOOL}))),
            Column('col_struct', Struct(fields={
                'field_bool': BOOL,
                'field_array': Array(inner=BOOL),
            })),
        ],
    )

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name) is None


def test_create_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.create_table')

    make_runner('target_create_table').run(wet_run=True)
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
    assert create_table.big_lake.connection_id == 'my_connection'
    assert create_table.big_lake.storage_uri == 'https://my.storage/uri'
    assert create_table.big_lake.file_format == 'PARQUET'
    assert create_table.big_lake.table_format == 'ICEBERG'

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name) is None


def test_drop_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_drop_table').run(wet_run=True)

    assert len(db_backend.tables) == 0
    assert len(meta_backend.get_applied_operations()) == 2

    make_runner('target_revert').run(wet_run=True, allow_down=True)
    revert_table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1

    assert revert_table == Table(
        name=table_name,
        columns=[Column('col_bool', BOOL)],
    )


def test_all_max_staleness(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    make_runner('target_all_max_staleness').run(wet_run=True)
    all_pos_table = db_backend.get_table(QualifiedName('my_project.my_dataset.all_pos_table'))
    all_neg_table = db_backend.get_table(QualifiedName('my_project.my_dataset.all_neg_table'))
    only_year_table = db_backend.get_table(QualifiedName('my_project.my_dataset.only_year_table'))
    only_month_table = db_backend.get_table(QualifiedName('my_project.my_dataset.only_month_table'))
    only_day_table = db_backend.get_table(QualifiedName('my_project.my_dataset.only_day_table'))
    only_hour_table = db_backend.get_table(QualifiedName('my_project.my_dataset.only_hour_table'))
    only_minute_table = db_backend.get_table(QualifiedName('my_project.my_dataset.only_minute_table'))
    only_second_table = db_backend.get_table(QualifiedName('my_project.my_dataset.only_second_table'))
    only_microsecond_table = db_backend.get_table(QualifiedName('my_project.my_dataset.only_microsecond_table'))

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

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.all_pos_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.all_neg_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.only_year_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.only_month_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.only_day_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.only_hour_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.only_minute_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.only_second_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.only_microsecond_table')) is None


def test_all_partitioning(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    make_runner('target_all_partitioning').run(wet_run=True)
    time_all_table = db_backend.get_table(QualifiedName('my_project.my_dataset.time_all_table'))
    year_ingest_table = db_backend.get_table(QualifiedName('my_project.my_dataset.year_ingest_table'))
    month_ingest_table = db_backend.get_table(QualifiedName('my_project.my_dataset.month_ingest_table'))
    day_ingest_table = db_backend.get_table(QualifiedName('my_project.my_dataset.day_ingest_table'))
    hour_ingest_table = db_backend.get_table(QualifiedName('my_project.my_dataset.hour_ingest_table'))
    int_all_table = db_backend.get_table(QualifiedName('my_project.my_dataset.int_all_table'))

    assert len(db_backend.tables) == 7
    assert len(meta_backend.get_applied_operations()) == 7

    assert time_all_table.partitioning == Partitioning(
        kind='TIME',
        column=ColumnName('col_date'),
        time_unit='YEAR',
        expiration=timedelta(days=1),
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

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.time_all_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.year_ingest_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.month_ingest_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.day_ingest_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.hour_ingest_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.int_all_table')) is None


def test_all_rounding_mode(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    make_runner('target_all_rounding_mode').run(wet_run=True)
    round_half_away_from_zero_table = db_backend.get_table(QualifiedName('my_project.my_dataset.round_half_away_from_zero_table'))
    round_half_even_table = db_backend.get_table(QualifiedName('my_project.my_dataset.round_half_even_table'))

    assert len(db_backend.tables) == 3
    assert len(meta_backend.get_applied_operations()) == 3
    assert round_half_away_from_zero_table.default_rounding_mode == RoundingMode('ROUND_HALF_AWAY_FROM_ZERO')
    assert round_half_even_table.default_rounding_mode == RoundingMode('ROUND_HALF_EVEN')

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.round_half_away_from_zero_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.round_half_even_table')) is None


def test_rename_table(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    make_runner('target_rename_table').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.revert_table')) is None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.renamed_table')) is not None

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.revert_table')) is not None
    assert db_backend.get_table(QualifiedName('my_project.my_dataset.renamed_table')) is None


def test_set_primary_key(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_set_primary_key').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(table_name).primary_key is None

    make_runner('target_unset_primary_key').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(table_name).primary_key == PrimaryKey(
        column_names = [ColumnName('col_bool')],
        enforced=True,
    )

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).primary_key is None


def test_set_partition_expiration(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.partitioning_table')

    make_runner('target_set_partition_expiration').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).partitioning.expiration == timedelta(days=30)

    make_runner('target_clear_partition_expiration').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(table_name).partitioning.expiration is None

    make_runner('target_unset_partition_expiration').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).partitioning.expiration == timedelta(days=90)


def test_set_require_partition_filter(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.partitioning_table')

    make_runner('target_set_require_partition_filter').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).partitioning.require_filter is False

    make_runner('target_unset_require_partition_filter').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).partitioning.require_filter is True


def test_set_clustering(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.clustering_table')

    make_runner('target_set_clustering').run(wet_run=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 3

    assert db_backend.get_table(table_name).clustering == [
        ColumnName('col_int'),
        ColumnName('col_bool'),
    ]

    make_runner('target_unset_clustering').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(table_name).clustering == [
        ColumnName('col_bool'),
        ColumnName('col_int'),
    ]

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name) is None


def test_set_description(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_set_description').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(table_name).description == 'bar'

    make_runner('target_unset_description').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).description == 'foo'

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).description is None


def test_set_labels(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_set_labels').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3

    assert db_backend.get_table(table_name).labels == {
        'l2': 'v2',
        'l3': 'v3',
    }

    make_runner('target_unset_labels').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(table_name).labels == {
        'l1': 'v1',
        'l2': 'v2',
    }

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).labels is None


def test_set_tags(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_set_tags').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3

    assert db_backend.get_table(table_name).tags == {
        't2': 'v2',
        't3': 'v3',
    }

    make_runner('target_unset_tags').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2

    assert db_backend.get_table(table_name).tags == {
        't1': 'v1',
        't2': 'v2',
    }

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).tags is None


def test_set_expiration_timestamp(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.expiration_table')

    make_runner('target_set_expiration_timestamp').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).expiration_timestamp == datetime(2027, 1, 1, tzinfo=timezone.utc)

    make_runner('target_clear_expiration_timestamp').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(table_name).expiration_timestamp is None

    make_runner('target_unset_expiration_timestamp').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).expiration_timestamp == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_set_default_rounding_mode(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_set_default_rounding_mode').run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert table.default_rounding_mode == RoundingMode('ROUND_HALF_EVEN')

    make_runner('target_unset_default_rounding_mode').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert table.default_rounding_mode == RoundingMode('ROUND_HALF_AWAY_FROM_ZERO')

    make_runner('target_revert').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.default_rounding_mode is None


def test_set_max_staleness(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.staleness_table')

    make_runner('target_set_max_staleness').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).max_staleness == IntervalLiteral(minute=10)

    make_runner('target_clear_max_staleness').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(table_name).max_staleness is None

    make_runner('target_unset_max_staleness').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).max_staleness == IntervalLiteral(minute=30)


def test_set_enable_change_history(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.change_history_table')

    make_runner('target_set_enable_change_history').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).enable_change_history is False

    make_runner('target_unset_enable_change_history').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).enable_change_history is True


def test_set_enable_fine_grained_mutations(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.fine_grained_mutations_table')

    make_runner('target_set_enable_fine_grained_mutations').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).enable_fine_grained_mutations is False

    make_runner('target_unset_enable_fine_grained_mutations').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).enable_fine_grained_mutations is True


def test_set_kms_key_name(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.kms_key_name_table')

    make_runner('target_set_kms_key_name').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(table_name).kms_key_name == 'my/new/key/name'

    make_runner('target_clear_kms_key_name').run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(table_name).kms_key_name is None

    make_runner('target_unset_kms_key_name').run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(table_name).kms_key_name == 'my/kms/key/name'


def test_add_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_add_column').run(wet_run=True)

    assert len(db_backend.get_table(table_name).columns) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('add_col') in db_backend.get_table(table_name).column_map

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('add_col') not in db_backend.get_table(table_name).column_map


def test_drop_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_drop_column').run(wet_run=True)

    assert len(db_backend.get_table(table_name).columns) == 0
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col_bool') not in db_backend.get_table(table_name).column_map

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col_bool') in db_backend.get_table(table_name).column_map


def test_rename_column(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_rename_column').run(wet_run=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col_bool') not in db_backend.get_table(table_name).column_map
    assert ColumnName('col_renamed') in db_backend.get_table(table_name).column_map

    make_runner('target_revert').run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(table_name).columns) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col_bool') in db_backend.get_table(table_name).column_map
    assert ColumnName('col_renamed') not in db_backend.get_table(table_name).column_map


def test_set_column_datatype(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.datatype_table')

    make_runner('target_set_column_datatype').run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 4
    assert table.column_map[ColumnName('col_int')].datatype == Numeric(precision=1, scale=1)
    assert table.column_map[ColumnName('col_numeric')].datatype == FLOAT64
    assert table.column_map[ColumnName('col_bignumeric')].datatype == BigNumeric(precision=12, scale=8)

    make_runner('target_unset_column_datatype').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.column_map[ColumnName('col_int')].datatype == INT64
    assert table.column_map[ColumnName('col_numeric')].datatype == Numeric(precision=4, scale=2)
    assert table.column_map[ColumnName('col_bignumeric')].datatype == BigNumeric(precision=8, scale=4)


def test_add_column_field(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.column_field_table')

    make_runner('target_add_column_field').run(wet_run=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert col_struct.fields['field_string'] == STRING
    assert col_struct.fields['field_array_struct'].inner.fields['field_array_string'].inner == STRING

    make_runner('target_unadd_column_field').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert 'field_string' not in col_struct.fields
    assert 'field_array_string' not in col_struct.fields['field_array_struct'].inner.fields


def test_drop_column_field(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.column_field_table')

    make_runner('target_drop_column_field').run(wet_run=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 5
    assert 'field_string' not in col_struct.fields
    assert 'field_array_string' not in col_struct.fields['field_array_struct'].inner.fields

    make_runner('target_add_column_field').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)
    col_struct = table.column_map[ColumnName('col_struct')].datatype

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert col_struct.fields['field_string'] == STRING
    assert col_struct.fields['field_array_struct'].inner.fields['field_array_string'].inner == STRING


def test_set_column_nullable(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_set_column_nullable').run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert table.column_map[ColumnName('col_bool')].nullable == False

    make_runner('target_unset_column_nullable').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert table.column_map[ColumnName('col_bool')].nullable == True

    make_runner('target_revert').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.column_map[ColumnName('col_bool')].nullable == False


def test_set_column_description(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.revert_table')

    make_runner('target_set_column_description').run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert table.column_map[ColumnName('col_bool')].description == 'bar'

    make_runner('target_unset_column_description').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert table.column_map[ColumnName('col_bool')].description == 'foo'

    make_runner('target_revert').run(wet_run=True, allow_down=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert table.column_map[ColumnName('col_bool')].description is None


def test_create_view(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.view_table')

    make_runner('target_create_view').run(wet_run=True)
    view = db_backend.get_view(table_name)

    assert len(db_backend.views) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert view.select_sql == 'SELECT 1 AS foo\n'
    assert view.formatted_select_sql == 'SELECT 1 AS foo\n'
    assert view.columns == [Column('foo', INT64)]
    assert view.friendly_name == 'my_friendly_name'
    assert view.description == 'My description'
    assert view.labels == {'key_1': 'value_1', 'key_2': 'value_2'}
    assert view.tags == {'key_3': 'value_3', 'key_4': 'value_4'}

    assert view.expiration_timestamp == datetime(
        year=2000,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
    )

    assert view.privacy_policy == {'p1': 123, 'p2': 'baz'}

    make_runner('target_replace_view').run(wet_run=True)
    view = db_backend.get_view(table_name)

    assert len(db_backend.views) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert view.select_sql == 'SELECT 2 AS bar\n'
    assert view.formatted_select_sql == 'SELECT 2 AS bar\n'
    assert view.columns == [Column('bar', INT64)]

    make_runner('target_create_view').run(wet_run=True, allow_down=True)
    view = db_backend.get_view(table_name)

    assert len(db_backend.views) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert view.select_sql == 'SELECT 1 AS foo\n'
    assert view.formatted_select_sql == 'SELECT 1 AS foo\n'
    assert view.columns == [Column('foo', INT64)]


def test_drop_view(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.view_table')

    make_runner('target_drop_view').run(wet_run=True)
    view = db_backend.get_view(table_name)

    assert len(db_backend.views) == 0
    assert len(meta_backend.get_applied_operations()) == 2
    assert view is None

    make_runner('target_create_view').run(wet_run=True, allow_down=True)
    view = db_backend.get_view(table_name)

    assert len(db_backend.views) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert view.select_sql == 'SELECT 1 AS foo\n'
    assert view.formatted_select_sql == 'SELECT 1 AS foo\n'
    assert view.columns == [Column('foo', INT64)]
    assert view.friendly_name == 'my_friendly_name'
    assert view.description == 'My description'
    assert view.labels == {'key_1': 'value_1', 'key_2': 'value_2'}
    assert view.tags == {'key_3': 'value_3', 'key_4': 'value_4'}

    assert view.expiration_timestamp == datetime(
        year=2000,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
    )

    assert view.privacy_policy == {'p1': 123, 'p2': 'baz'}


def test_create_materialized_view(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.materialized_view_table')

    make_runner('target_create_materialized_view').run(wet_run=True)
    materialized_view = db_backend.get_materialized_view(table_name)

    assert len(db_backend.materialized_views) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert materialized_view.select_sql == 'SELECT NOT col_bool AS not_col_bool, col_date FROM my_project.my_dataset.source_table\n'
    assert materialized_view.formatted_select_sql == 'SELECT NOT col_bool AS not_col_bool, col_date FROM my_project.my_dataset.source_table\n'

    assert materialized_view.partitioning == Partitioning(
        kind='TIME',
        column=ColumnName('col_date'),
        column_datatype=DATE,
    )

    assert materialized_view.clustering == [ColumnName('not_col_bool')]
    assert materialized_view.friendly_name == 'my_friendly_name'
    assert materialized_view.description == 'My description'
    assert materialized_view.labels == {'key_1': 'value_1', 'key_2': 'value_2'}
    assert materialized_view.tags == {'key_3': 'value_3', 'key_4': 'value_4'}

    assert materialized_view.expiration_timestamp == datetime(
        year=2000,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
    )

    assert materialized_view.allow_non_incremental_definition == True
    assert materialized_view.enable_refresh == True
    assert materialized_view.refresh_interval == timedelta(hours=1)

    make_runner('target_replace_materialized_view').run(wet_run=True)
    materialized_view = db_backend.get_materialized_view(table_name)

    assert len(db_backend.materialized_views) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert materialized_view.select_sql == 'SELECT col_bool, col_date FROM my_project.my_dataset.source_table\n'
    assert materialized_view.formatted_select_sql == 'SELECT col_bool, col_date FROM my_project.my_dataset.source_table\n'
    assert materialized_view.partitioning is None
    assert materialized_view.clustering is None
    assert materialized_view.friendly_name is None
    assert materialized_view.description is None
    assert materialized_view.labels is None
    assert materialized_view.tags is None
    assert materialized_view.expiration_timestamp is None
    assert materialized_view.allow_non_incremental_definition == False
    assert materialized_view.enable_refresh == False
    assert materialized_view.refresh_interval == timedelta(hours=2)

    make_runner('target_create_materialized_view').run(wet_run=True, allow_down=True)
    materialized_view = db_backend.get_materialized_view(table_name)

    assert len(db_backend.materialized_views) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert materialized_view.select_sql == 'SELECT NOT col_bool AS not_col_bool, col_date FROM my_project.my_dataset.source_table\n'
    assert materialized_view.formatted_select_sql == 'SELECT NOT col_bool AS not_col_bool, col_date FROM my_project.my_dataset.source_table\n'

    assert materialized_view.partitioning == Partitioning(
        kind='TIME',
        column=ColumnName('col_date'),
        column_datatype=DATE,
    )

    assert materialized_view.clustering == [ColumnName('not_col_bool')]
    assert materialized_view.friendly_name == 'my_friendly_name'
    assert materialized_view.description == 'My description'
    assert materialized_view.labels == {'key_1': 'value_1', 'key_2': 'value_2'}
    assert materialized_view.tags == {'key_3': 'value_3', 'key_4': 'value_4'}

    assert materialized_view.expiration_timestamp == datetime(
        year=2000,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
    )

    assert materialized_view.allow_non_incremental_definition == True
    assert materialized_view.enable_refresh == True
    assert materialized_view.refresh_interval == timedelta(hours=1)


def test_drop_materialized_view(db_backend: MemoryDbBackend, meta_backend: MemoryMetaBackend, make_runner: MakeRunner):
    table_name = QualifiedName('my_project.my_dataset.materialized_view_table')

    make_runner('target_drop_materialized_view').run(wet_run=True)
    materialized_view = db_backend.get_materialized_view(table_name)

    assert len(db_backend.materialized_views) == 0
    assert len(meta_backend.get_applied_operations()) == 3
    assert materialized_view is None

    make_runner('target_create_materialized_view').run(wet_run=True, allow_down=True)
    materialized_view = db_backend.get_materialized_view(table_name)

    assert len(db_backend.materialized_views) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert materialized_view.select_sql == 'SELECT NOT col_bool AS not_col_bool, col_date FROM my_project.my_dataset.source_table\n'
    assert materialized_view.formatted_select_sql == 'SELECT NOT col_bool AS not_col_bool, col_date FROM my_project.my_dataset.source_table\n'

    assert materialized_view.partitioning == Partitioning(
        kind='TIME',
        column=ColumnName('col_date'),
        column_datatype=DATE,
    )

    assert materialized_view.clustering == [ColumnName('not_col_bool')]
    assert materialized_view.friendly_name == 'my_friendly_name'
    assert materialized_view.description == 'My description'
    assert materialized_view.labels == {'key_1': 'value_1', 'key_2': 'value_2'}
    assert materialized_view.tags == {'key_3': 'value_3', 'key_4': 'value_4'}

    assert materialized_view.expiration_timestamp == datetime(
        year=2000,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
    )

    assert materialized_view.allow_non_incremental_definition == True
    assert materialized_view.enable_refresh == True
    assert materialized_view.refresh_interval == timedelta(hours=1)


def test_template_database_and_schema(
    db_backend: MemoryDbBackend,
    meta_backend: MemoryMetaBackend,
    make_runner: TemplateMakeRunner,
):
    table_name = QualifiedName('new_project.new_dataset.template_table')
    template_files = [Path('tests/res/templates/database_and_schema.yaml')]

    make_runner('target_template_database_and_schema', template_files).run(wet_run=True)
    table = db_backend.get_table(table_name)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col_bool') in table.column_map
    assert ColumnName('add_col') in table.column_map


def test_apply_templates():
    table_name = QualifiedName('test_project.test_dataset.test_table')
    foreign_table_name = QualifiedName('test_project.test_dataset.test_foreign_table')
    add_table_name = QualifiedName('test_project.test_dataset.test_table')

    operations = [
        CreateTable(table=Table(
            name=table_name,
            columns=[Column('col_bool', BOOL)],
            foreign_keys=[ForeignKey(
                foreign_table_name=foreign_table_name,
                references=[ForeignReference(
                    local_column_name=ColumnName('col_bool'),
                    foreign_column_name=ColumnName('foreign_col_bool'),
                )],
            )],
        )),
        AddColumn(
            table_name=add_table_name,
            column=Column('col_int', INT64),
        ),
    ]

    templates = [
        Template(
            root_type=QualifiedName,
            path=['database'],
            value='new_project',
        ),
        Template(
            root_type=QualifiedName,
            path=['schema'],
            value='new_dataset',
        ),
    ]

    apply_templates(operations, templates)

    assert table_name.database.string == 'new_project'
    assert table_name.schema.string == 'new_dataset'
    assert foreign_table_name.database.string == 'new_project'
    assert foreign_table_name.schema.string == 'new_dataset'
    assert add_table_name.database.string == 'new_project'
    assert add_table_name.schema.string == 'new_dataset'


@mark.parametrize(
    'graph, expected',
    [
        [
            {
                'a': [],
                'b': [],
                'c': [],
            },
            ['a', 'b', 'c'],
        ],
        [
            {
                'a': ['b', 'c'],
                'b': [],
                'c': [],
            },
            ['b', 'c', 'a'],
        ],
        [
            {
                'a': ['b'],
                'b': ['c'],
                'c': ['d'],
                'd': ['e'],
                'e': [],
            },
            ['e', 'd', 'c', 'b', 'a'],
        ],
        [
            {
                'a': ['b', 'c', 'd', 'e'],
                'b': ['c', 'd', 'e'],
                'c': ['d', 'e'],
                'd': ['e'],
                'e': [],
            },
            ['e', 'd', 'c', 'b', 'a'],
        ],
    ],
)
def test_sort_operations(graph, expected):
    def to_table_name(name: str) -> QualifiedName:
        return QualifiedName(f'my_project.my_dataset.{name}')

    def to_table(local_table: str, foreign_tables: list[str]) -> Table:
        return Table(
            name=to_table_name(local_table),
            columns=[Column('col_bool', BOOL)],
            foreign_keys=[
                ForeignKey(
                    foreign_table_name=to_table_name(name),
                    references=[ForeignReference(
                        local_column_name=ColumnName('col_bool'),
                        foreign_column_name=ColumnName('col_bool'),
                    )],
                )
                for name in foreign_tables
            ],
        )

    operations = [CreateTable(table=to_table(l, fs)) for l, fs in graph.items()]
    actual = sort_operations(operations)
    assert [op.table.name for op in actual] == [to_table_name(name) for name in expected]
