from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.model.v1.datatype import Array, BigNumeric, BOOL, DATE, DATE_TIME, FLOAT64, GEOGRAPHY, INT64, JSON, \
    Numeric, Range, \
    STRING, \
    Struct, \
    TIME, TIMESTAMP
from liti.core.model.v1.schema import Column, ColumnName, Table, TableName
from liti.core.runner import MigrateRunner


def test_all_data_types():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner_1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_all_data_types',
    )

    runner_1.run(wet_run=True)
    actual_table = db_backend.get_table(TableName('my_project.my_dataset.my_table_1'))

    assert len(db_backend.tables) == 1

    assert actual_table == Table(
        name=TableName('my_project.my_dataset.my_table_1'),
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


def test_create_table():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner_1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_create_table',
    )

    runner_1.run(wet_run=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_2')) is not None

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_2')) is None


def test_drop_table():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner_1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_drop_table',
    )

    runner_1.run(wet_run=True)

    assert len(db_backend.tables) == 0
    assert len(meta_backend.get_applied_operations()) == 2

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)
    my_table_1 = db_backend.get_table(TableName('my_project.my_dataset.my_table_1'))

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert my_table_1 == Table(
        name=TableName('my_project.my_dataset.my_table_1'),
        columns=[
            Column(name='col1', data_type=INT64),
            Column(name='col2', data_type=FLOAT64, nullable=True),
            Column(name='col3', data_type=STRING),
        ],
    )


def test_rename_table():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner_1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_rename_table',
    )

    runner_1.run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_1')) is None
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_2')) is not None

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 1
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_1')) is not None
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_2')) is None


def test_add_column():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner_1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_add_column',
    )

    runner_1.run(wet_run=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 4
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col4') in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 3
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col4') not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map


def test_drop_column():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner_1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_drop_column',
    )

    runner_1.run(wet_run=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col3') not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 3
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col3') in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map


def test_rename_column():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner_1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_rename_column',
    )

    runner_1.run(wet_run=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 3
    assert len(meta_backend.get_applied_operations()) == 2
    assert ColumnName('col3') not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map
    assert ColumnName('col4') in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 3
    assert len(meta_backend.get_applied_operations()) == 1
    assert ColumnName('col3') in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map
    assert ColumnName('col4') not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map
