from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.model.v1.data_type import BOOL, INT64
from liti.core.model.v1.schema import TableName
from liti.core.runner import MigrationsRunner


def test_down_migration():
    db_backend = MemoryDbBackend()
    meta_backend = MemoryMetaBackend()

    runner1 = MigrationsRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target1',
    )

    runner1.run(dry_run=False)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName("my_project.my_dataset.my_table_2")).columns[1].data_type == BOOL
    assert meta_backend.get_applied_operations()[1].table.columns[1].data_type == BOOL

    runner2 = MigrationsRunner(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target='res/target2',
    )

    runner2.run(dry_run=False, allow_down=True)

    assert len(db_backend.tables) == 2
    assert len(meta_backend.get_applied_operations()) == 2
    assert db_backend.get_table(TableName("my_project.my_dataset.my_table_2")).columns[1].data_type == INT64
    assert meta_backend.get_applied_operations()[1].table.columns[1].data_type == INT64
