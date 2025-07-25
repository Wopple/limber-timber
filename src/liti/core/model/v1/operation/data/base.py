from typing import Any, ClassVar, Literal

from liti.core.base import LitiModel


class Operation(LitiModel):
    KIND: ClassVar[str]

    @classmethod
    def get_kind(cls, kind: str) -> type:
        return {
            subclass.KIND: subclass
            for subclass in Operation.__subclasses__()
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
