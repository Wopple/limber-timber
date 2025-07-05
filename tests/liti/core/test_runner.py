from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.model.v1.data_type import FLOAT64, INT64, STRING
from liti.core.model.v1.schema import Column, Table, TableName
from liti.core.runner import MigrateRunner


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
    assert 'col4' in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 3
    assert len(meta_backend.get_applied_operations()) == 1
    assert 'col4' not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map


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
    assert 'col3' not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 3
    assert len(meta_backend.get_applied_operations()) == 1
    assert 'col3' in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map


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
    assert 'col3' not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map
    assert 'col4' in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map

    runner_2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target_revert',
    )

    runner_2.run(wet_run=True, allow_down=True)

    assert len(db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).columns) == 3
    assert len(meta_backend.get_applied_operations()) == 1
    assert 'col3' in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map
    assert 'col4' not in db_backend.get_table(TableName('my_project.my_dataset.my_table_1')).column_map
