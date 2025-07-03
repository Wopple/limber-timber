from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.model.v1.data_type import BOOL, FLOAT64, INT64, STRING
from liti.core.model.v1.schema import Column, Table, TableName
from liti.core.runner import MigrateRunner


def test_down_migration():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target1',
    )

    runner1.run(wet_run=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_2')).columns[1].data_type == BOOL
    assert meta_backend.get_applied_operations()[1].table.columns[1].data_type == BOOL

    runner2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target2',
    )

    runner2.run(wet_run=True, allow_down=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table_2')).columns[1].data_type == INT64
    assert meta_backend.get_applied_operations()[1].table.columns[1].data_type == INT64


def test_drop_table():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner1 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target3',
    )

    runner1.run(wet_run=True)

    assert len(db_backend.tables) == 1
    assert len(meta_backend.get_applied_operations()) == 3
    assert db_backend.get_table(TableName('my_project.my_dataset.my_table')) is None

    runner2 = MigrateRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target4',
    )

    runner2.run(wet_run=True, allow_down=True)
    my_table = db_backend.get_table(TableName('my_project.my_dataset.my_table'))

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert my_table == Table(
        name=TableName('my_project.my_dataset.my_table'),
        columns=[
            Column(name='col1', data_type=INT64),
            Column(name='col2', data_type=FLOAT64, nullable=True),
            Column(name='col3', data_type=STRING),
        ],
    )
