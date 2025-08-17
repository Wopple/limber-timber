from typing import ClassVar

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import QualifiedName, View


class CreateOrReplaceView(Operation):
    """ Semantics: CREATE OR REPLACE """

    view: View

    KIND: ClassVar[str] = 'create_or_replace_view'


class DropView(Operation):
    """ Semantics: DROP """

    view_name: QualifiedName

    KIND: ClassVar[str] = 'drop_view'
