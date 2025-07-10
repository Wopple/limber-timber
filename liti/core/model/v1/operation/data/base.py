from typing import Any, ClassVar, Literal

from liti.core.base import LitiModel


class Operation(LitiModel):
    KIND: ClassVar[str]

    def to_op_data(self, format: Literal['json', 'yaml']) -> dict[str, Any]:
        data = self.model_dump(mode='json' if format == 'json' else 'python')

        return {
            'kind': self.KIND,
            'data': data,
        }
