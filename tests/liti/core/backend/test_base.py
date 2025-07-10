from liti.core.backend.memory import MemoryMetaBackend
from liti.core.model.v1.datatype import INT64
from liti.core.model.v1.operation.data.table import CreateTable, DropTable
from liti.core.model.v1.schema import Column, Table, TableName


def table_name(num: int) -> TableName:
    return TableName(name=f'my_project.my_dataset.my_table_{num}')


def column(num: int) -> Column:
    return Column(name=f'col_{num}', data_type=INT64)


def create_table(num: int) -> CreateTable:
    return CreateTable(table=Table(name=table_name(num), columns=[column(1)]))


def drop_table(num: int) -> DropTable:
    return DropTable(table_name=table_name(num))


def test_get_migration_plan_same():
    current = [
        create_table(1),
        drop_table(1),
    ]

    target = [
        create_table(1),
        drop_table(1),
    ]

    meta_backend = MemoryMetaBackend(current)
    actual = meta_backend.get_migration_plan(target)

    assert actual['down'] == []
    assert actual['up'] == []


def test_get_migration_plan_behind():
    current = [
        create_table(1),
        drop_table(1),
    ]

    target = [
        create_table(1),
        drop_table(1),
        create_table(3),
        drop_table(3),
    ]

    meta_backend = MemoryMetaBackend(current)
    actual = meta_backend.get_migration_plan(target)

    assert actual['down'] == []

    assert actual['up'] == [
        create_table(3),
        drop_table(3),
    ]


def test_get_migration_plan_ahead():
    current = [
        create_table(1),
        drop_table(1),
        create_table(2),
        create_table(3),
    ]

    target = [
        create_table(1),
        drop_table(1),
    ]

    meta_backend = MemoryMetaBackend(current)
    actual = meta_backend.get_migration_plan(target)

    assert actual['down'] == [
        create_table(3),
        create_table(2),
    ]

    assert actual['up'] == []


def test_get_migration_mismatch():
    current = [
        create_table(1),
        drop_table(1),
        create_table(2),
        create_table(3),
    ]

    target = [
        create_table(1),
        drop_table(1),
        create_table(3),
        drop_table(3),
    ]

    meta_backend = MemoryMetaBackend(current)
    actual = meta_backend.get_migration_plan(target)

    assert actual['down'] == [
        create_table(3),
        create_table(2),
    ]

    assert actual['up'] == [
        create_table(3),
        drop_table(3),
    ]
