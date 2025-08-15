from pytest import fixture, mark

from liti.core.function import extract_nested_datatype
from liti.core.model.v1.datatype import Array, BOOL, Datatype, FLOAT64, INT64, STRING, Struct
from liti.core.model.v1.schema import Column, FieldPath, Table, TableName


@fixture
def nested_table() -> Table:
    return Table(name=TableName('project.dataset.table'), columns=[
        Column('struct_1', Struct(fields={
            'field_1': Struct(fields={
                'sub_field_1': INT64,
                'sub_field_2': FLOAT64,
                'sub_field_3': Array(inner=Struct(fields={'inner_field_1': BOOL})),
            }),
            'field_2': STRING,
        }))
    ])


@mark.parametrize(
    'field_path, expected',
    [
        ('struct_1.field_1.sub_field_1', INT64),
        ('struct_1.field_1.sub_field_2', FLOAT64),
        ('struct_1.field_1.sub_field_3.inner_field_1', BOOL),
        ('struct_1.field_1.sub_field_3', Array(inner=Struct(fields={'inner_field_1': BOOL}))),
        ('struct_1.field_1', Struct(fields={
            'sub_field_1': INT64,
            'sub_field_2': FLOAT64,
            'sub_field_3': Array(inner=Struct(fields={'inner_field_1': BOOL})),
        })),
        ('struct_1.field_2', STRING),
        ('struct_1', Struct(fields={
            'field_1': Struct(fields={
                'sub_field_1': INT64,
                'sub_field_2': FLOAT64,
                'sub_field_3': Array(inner=Struct(fields={'inner_field_1': BOOL})),
            }),
            'field_2': STRING,
        })),
    ]
)
def test_extract_nested_datatype(nested_table: Table, field_path: str, expected: Datatype):
    assert extract_nested_datatype(nested_table, FieldPath(field_path)) == expected
