from typing import ClassVar

from liti.core.model.v1.operation.data.base import EntityKind, Operation
from liti.core.model.v1.schema import MaterializedView, QualifiedName, View


class CreateView(Operation):
    """ Semantics: `CREATE OR REPLACE VIEW` """

    view: View

    KIND: ClassVar[str] = 'create_view'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'VIEW'}


class DropView(Operation):
    """ Semantics: `DROP VIEW` """

    view_name: QualifiedName

    KIND: ClassVar[str] = 'drop_view'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'VIEW'}


class CreateMaterializedView(Operation):
    """ Semantics: `CREATE OR REPLACE MATERIALIZED VIEW` """

    materialized_view: MaterializedView

    KIND: ClassVar[str] = 'create_materialized_view'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'MATERIALIZED_VIEW'}


class DropMaterializedView(Operation):
    """ Semantics: `DROP MATERIALIZED VIEW` """

    materialized_view_name: QualifiedName

    KIND: ClassVar[str] = 'drop_materialized_view'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'MATERIALIZED_VIEW'}
