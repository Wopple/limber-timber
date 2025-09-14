from typing import Any, ClassVar, Literal

from liti.core.base import LitiModel
from liti.core.reflect import recursive_subclasses

EntityKind = Literal['SCHEMA', 'TABLE', 'VIEW', 'MATERIALIZED_VIEW']


class Operation(LitiModel):
    KIND: ClassVar[str]

    @classmethod
    def by_kind(cls, kind: str) -> type['Operation']:
        # ensure Operation subclasses are imported first
        # noinspection PyUnresolvedReferences
        import liti.core.model.v1.operation.data.subclasses

        return {
            subclass.KIND: subclass
            for subclass in recursive_subclasses(Operation)
        }[kind]

    def to_op_data(self, format: Literal['json', 'yaml']) -> dict[str, Any]:
        data = self.model_dump(
            mode='json' if format == 'json' else 'python',
            exclude_none=True,
        )

        return {
            'kind': self.KIND,
            'data': data,
        }

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return set()
