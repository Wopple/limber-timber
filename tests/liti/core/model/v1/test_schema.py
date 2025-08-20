from pytest import mark

from liti.core.model.v1.schema import ColumnName, ForeignKey, ForeignReference, QualifiedName


@mark.parametrize(
    'input_name, expected',
    [
        [None, 'fk__local_1_local_2__test_database_test_schema_test_table__foreign_1_foreign_2'],
        ['fk$1', 'fk__local_1_local_2__test_database_test_schema_test_table__foreign_1_foreign_2'],
        ['fk_invalid_name$1', 'fk__local_1_local_2__test_database_test_schema_test_table__foreign_1_foreign_2'],
        ['fk_valid_name', 'fk_valid_name'],
    ],
)
def test_foreign_key_validation(input_name: str | None, expected: str):
    foreign_key = ForeignKey(
        name=input_name,
        foreign_table_name=QualifiedName('test_database.test_schema.test_table'),
        references=[
            ForeignReference(
                local_column_name=ColumnName('local_1'),
                foreign_column_name=ColumnName('foreign_1'),
            ),
            ForeignReference(
                local_column_name=ColumnName('local_2'),
                foreign_column_name=ColumnName('foreign_2'),
            ),
        ],
    )

    assert foreign_key.name == expected
