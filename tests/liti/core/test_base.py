from typing import Any

from pydantic import Field
from pytest import mark, raises

from liti.core.base import LitiModel, STAR
from liti.core.model.v1.datatype import BOOL
from liti.core.model.v1.schema import Column, ColumnName, ForeignKey, ForeignReference, Table, QualifiedName


class ListNestedModel(LitiModel):
    f_bool: bool = False
    f_int: int = 0


class NestedModel(LitiModel):
    f_bool: bool = False
    f_int: int = 0
    f_list_nested: list[ListNestedModel] = Field(default_factory=list)


class Model(LitiModel):
    f_none: bool | None = False
    f_bool: bool = False
    f_int: int = 0
    f_float: float = 0.1
    f_string: str = ''
    f_tuple: tuple[int, ...] = ()
    f_list: list[int] = Field(default_factory=list)
    f_set: set[int] = Field(default_factory=set)
    f_nested: NestedModel = Field(default_factory=NestedModel)
    f_nested_sibling: NestedModel = Field(default_factory=NestedModel)


def test_get_roots():
    table_name = QualifiedName('test_project.test_dataset.test_table')
    foreign_table_name = QualifiedName('test_project.test_dataset.test_foreign_table')

    table = Table(
        name=table_name,
        columns=[Column('col_bool', BOOL)],
        foreign_keys=[ForeignKey(
            foreign_table_name=foreign_table_name,
            references=[ForeignReference(
                local_column_name=ColumnName('col_bool'),
                foreign_column_name=ColumnName('foreign_col_bool'),
            )],
        )],
    )

    roots = table.get_roots(QualifiedName, STAR)
    actual_table_name_1, actual_match_1 = next(roots)
    actual_table_name_2, actual_match_2 = next(roots)

    with raises(StopIteration):
        next(roots)

    assert actual_table_name_1 is table_name
    assert actual_match_1 is STAR
    assert actual_table_name_2 is foreign_table_name
    assert actual_match_2 is STAR


@mark.parametrize(
    'field, match, set_value, expected',
    [
        # Star tests
        ['f_none', STAR, None, None],
        ['f_bool', STAR, False, False],
        ['f_bool', STAR, True, True],
        ['f_int', STAR, 0, 0],
        ['f_int', STAR, 1, 1],
        ['f_int', STAR, -1, -1],
        ['f_float', STAR, 0.0, 0.0],
        ['f_float', STAR, 0.5, 0.5],
        ['f_float', STAR, -0.5, -0.5],
        ['f_string', STAR, '', ''],
        ['f_string', STAR, 'a', 'a'],
        ['f_string', STAR, '1', '1'],
        ['f_string', STAR, 'foo', 'foo'],
        ['f_tuple', STAR, (), ()],
        ['f_tuple', STAR, (0, 1), (0, 1)],
        ['f_list', STAR, [], []],
        ['f_list', STAR, [0, 1], [0, 1]],
        ['f_set', STAR, set(), set()],
        ['f_set', STAR, {0, 1}, {0, 1}],

        # match tests
        ['f_none', {'f_none': False}, None, None],
        ['f_none', {'f_none': None}, None, False],
        ['f_bool', {'f_bool': False}, True, True],
        ['f_bool', {'f_bool': True}, True, False],
        ['f_int', {'f_int': 0}, 1, 1],
        ['f_int', {'f_int': 1}, 1, 0],
        ['f_float', {'f_float': 0.1}, 0.5, 0.5],
        ['f_float', {'f_float': 0.0999999999999}, 0.5, 0.5],
        ['f_float', {'f_float': 0.5}, 0.5, 0.1],
        ['f_string', {'f_string': ''}, 'a', 'a'],
        ['f_string', {'f_string': 'a'}, 'a', ''],
        ['f_tuple', {'f_tuple': ()}, (0, 1), (0, 1)],
        ['f_tuple', {'f_tuple': (0, 1)}, (0, 1), ()],
        ['f_list', {'f_list': []}, [0, 1], [0, 1]],
        ['f_list', {'f_list': [0, 1]}, [0, 1], []],
        ['f_set', {'f_set': set()}, {0, 1}, {0, 1}],
        ['f_set', {'f_set': {0, 1}}, {0, 1}, set()],

        # sibling tests
        ['f_none', {'f_set': set()}, None, None],
        ['f_none', {'f_set': {0, 1}}, None, False],
        ['f_bool', {'f_none': False}, True, True],
        ['f_bool', {'f_none': None}, True, False],
        ['f_int', {'f_bool': False}, 1, 1],
        ['f_int', {'f_bool': True}, 1, 0],
        ['f_float', {'f_int': 0}, 0.5, 0.5],
        ['f_float', {'f_int': 1}, 0.5, 0.1],
        ['f_string', {'f_float': 0.1}, 'a', 'a'],
        ['f_string', {'f_float': 0.5}, 'a', ''],
        ['f_tuple', {'f_string': ''}, (0, 1), (0, 1)],
        ['f_tuple', {'f_string': 'a'}, (0, 1), ()],
        ['f_list', {'f_tuple': ()}, [0, 1], [0, 1]],
        ['f_list', {'f_tuple': (0, 1)}, [0, 1], []],
        ['f_set', {'f_list': []}, {0, 1}, {0, 1}],
        ['f_set', {'f_list': [0, 1]}, {0, 1}, set()],

        # multi tests
        ['f_none', {'f_none': False, 'f_set': set()}, None, None],
        ['f_none', {'f_none': True, 'f_set': set()}, None, False],
        ['f_none', {'f_none': False, 'f_set': {0, 1}}, None, False],
        ['f_bool', {'f_bool': False, 'f_none': False}, True, True],
        ['f_bool', {'f_bool': True, 'f_none': False}, True, False],
        ['f_bool', {'f_bool': False, 'f_none': None}, True, False],
        ['f_int', {'f_int': 0, 'f_bool': False}, 1, 1],
        ['f_int', {'f_int': 1, 'f_bool': False}, 1, 0],
        ['f_int', {'f_int': 0, 'f_bool': True}, 1, 0],
        ['f_float', {'f_float': 0.1, 'f_int': 0}, 0.5, 0.5],
        ['f_float', {'f_float': 0.5, 'f_int': 0}, 0.5, 0.1],
        ['f_float', {'f_float': 0.1, 'f_int': 1}, 0.5, 0.1],
        ['f_string', {'f_string': '', 'f_float': 0.1}, 'a', 'a'],
        ['f_string', {'f_string': 'a', 'f_float': 0.1}, 'a', ''],
        ['f_string', {'f_string': '', 'f_float': 0.5}, 'a', ''],
        ['f_tuple', {'f_tuple': (), 'f_string': ''}, (0, 1), (0, 1)],
        ['f_tuple', {'f_tuple': (0, 1), 'f_string': ''}, (0, 1), ()],
        ['f_tuple', {'f_tuple': (), 'f_string': 'a'}, (0, 1), ()],
        ['f_list', {'f_list': [], 'f_tuple': ()}, [0, 1], [0, 1]],
        ['f_list', {'f_list': [0, 1], 'f_tuple': ()}, [0, 1], []],
        ['f_list', {'f_list': [], 'f_tuple': (0, 1)}, [0, 1], []],
        ['f_set', {'f_set': set(), 'f_list': []}, {0, 1}, {0, 1}],
        ['f_set', {'f_set': {0, 1}, 'f_list': []}, {0, 1}, set()],
        ['f_set', {'f_set': set(), 'f_list': [0, 1]}, {0, 1}, set()],
    ],
)
def test_get_update_fns_basic(field: str, match: Any, set_value: Any, expected: Any):
    model = Model()

    for fn in model.get_update_fns([field], [match]):
        fn(set_value)

    assert model.model_dump()[field] == expected


@mark.parametrize(
    'path, match, set_value, expected',
    [
        # Star tests
        [['f_nested', 'f_bool'], STAR, True, True],
        [['f_nested', 'f_int'], STAR, 1, 1],

        # match tests
        [['f_nested', 'f_bool'], {'f_nested': {'f_bool': False}}, True, True],
        [['f_nested', 'f_bool'], {'f_nested': {'f_bool': True}}, True, False],
        [['f_nested', 'f_int'], {'f_nested': {'f_int': 0}}, 1, 1],
        [['f_nested', 'f_int'], {'f_nested': {'f_int': 1}}, 1, 0],

        # sibling tests
        [['f_nested', 'f_bool'], {'f_nested': {'f_int': 0}}, True, True],
        [['f_nested', 'f_bool'], {'f_nested': {'f_int': 1}}, True, False],
        [['f_nested', 'f_int'], {'f_nested': {'f_bool': False}}, 1, 1],
        [['f_nested', 'f_int'], {'f_nested': {'f_bool': True}}, 1, 0],

        # multi tests
        [['f_nested', 'f_bool'], {'f_bool': False, 'f_nested': {'f_bool': False, 'f_int': 0}}, True, True],
        [['f_nested', 'f_bool'], {'f_bool': True, 'f_nested': {'f_bool': False, 'f_int': 0}}, True, False],
        [['f_nested', 'f_bool'], {'f_bool': False, 'f_nested': {'f_bool': True, 'f_int': 0}}, True, False],
        [['f_nested', 'f_bool'], {'f_bool': False, 'f_nested': {'f_bool': False, 'f_int': 1}}, True, False],
    ],
)
def test_get_update_fns_nested(path: list[str], match: Any, set_value: Any, expected: Any):
    model = Model()

    for fn in model.get_update_fns(path, [match]):
        fn(set_value)

    assert model.model_dump()[path[0]][path[1]] == expected


@mark.parametrize(
    'path, models, match, set_value, expected',
    [
        # Star tests
        [
            ['f_nested', 'f_list_nested', 'f_bool'],
            [ListNestedModel(f_bool=v) for v in [False, True]],
            STAR,
            True,
            [True, True],
        ],
        [
            ['f_nested', 'f_list_nested', 'f_int'],
            [ListNestedModel(f_int=v) for v in [0, 1, -1]],
            STAR,
            1,
            [1, 1, 1],
        ],

        # match tests
        [
            ['f_nested', 'f_list_nested', 'f_bool'],
            [ListNestedModel(f_bool=v) for v in [False, True]],
            {'f_nested': {'f_list_nested': {'f_bool': False}}},
            True,
            [True, True],
        ],
        [
            ['f_nested', 'f_list_nested', 'f_bool'],
            [ListNestedModel(f_bool=v) for v in [False, True]],
            {'f_nested': {'f_list_nested': {'f_bool': True}}},
            True,
            [False, True],
        ],
        [
            ['f_nested', 'f_list_nested', 'f_int'],
            [ListNestedModel(f_int=v) for v in [0, 1, -1]],
            {'f_nested': {'f_list_nested': {'f_int': 0}}},
            1,
            [1, 1, -1],
        ],
        [
            ['f_nested', 'f_list_nested', 'f_int'],
            [ListNestedModel(f_int=v) for v in [0, 1, -1]],
            {'f_nested': {'f_list_nested': {'f_int': 1}}},
            1,
            [0, 1, -1],
        ],
    ],
)
def test_get_update_fns_list_nested(
    path: list[str],
    models: list[ListNestedModel],
    match: Any,
    set_value: Any,
    expected: Any,
):
    model = Model(f_nested=NestedModel(f_list_nested=models))

    for fn in model.get_update_fns(path, [match]):
        fn(set_value)

    assert [m[path[2]] for m in model.model_dump()[path[0]][path[1]]] == expected
