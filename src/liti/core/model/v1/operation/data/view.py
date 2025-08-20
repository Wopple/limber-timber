from typing import ClassVar

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import MaterializedView, QualifiedName, View


class CreateOrReplaceView(Operation):
    """ Semantics: CREATE OR REPLACE """

    view: View

    KIND: ClassVar[str] = 'create_or_replace_view'


class DropView(Operation):
    """ Semantics: DROP """

    view_name: QualifiedName

    KIND: ClassVar[str] = 'drop_view'


class CreateOrReplaceMaterializedView(Operation):
    """ Semantics: CREATE OR REPLACE """

    materialized_view: MaterializedView

    KIND: ClassVar[str] = 'create_or_replace_materialized_view'


class DropMaterializedView(Operation):
    """ Semantics: DROP """

    materialized_view_name: QualifiedName

    KIND: ClassVar[str] = 'drop_materialized_view'
