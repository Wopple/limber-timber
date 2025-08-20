from liti.core.context import Context
from liti.core.model.v1.operation.data.view import CreateOrReplaceMaterializedView, CreateOrReplaceView, \
    DropMaterializedView, DropView
from liti.core.model.v1.operation.ops.base import OperationOps


class CreateOrReplaceViewOps(OperationOps):
    op: CreateOrReplaceView

    def __init__(self, op: CreateOrReplaceView, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.create_or_replace_view(self.op.view)

    def down(self) -> CreateOrReplaceView | DropView:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_view = sim_db.get_view(self.op.view.name)

        if sim_view:
            return CreateOrReplaceView(view=sim_view)
        else:
            return DropView(view_name=self.op.view.name)

    def is_up(self) -> bool:
        return False  # CREATE OR REPLACE can safely assume not applied


class DropViewOps(OperationOps):
    op: DropView

    def __init__(self, op: DropView, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.drop_view(self.op.view_name)

    def down(self) -> CreateOrReplaceView:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_view = sim_db.get_view(self.op.view_name)
        return CreateOrReplaceView(view=sim_view)

    def is_up(self) -> bool:
        return not self.context.db_backend.has_view(self.op.view_name)


class CreateOrReplaceMaterializedViewOps(OperationOps):
    op: CreateOrReplaceMaterializedView

    def __init__(self, op: CreateOrReplaceMaterializedView, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.create_or_replace_view(self.op.materialized_view)

    def down(self) -> CreateOrReplaceMaterializedView | DropMaterializedView:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_materialized_view = sim_db.get_materialized_view(self.op.materialized_view.name)

        if sim_materialized_view:
            return CreateOrReplaceMaterializedView(materialized_view=sim_materialized_view)
        else:
            return DropMaterializedView(materialized_view_name=self.op.materialized_view.name)

    def is_up(self) -> bool:
        return False  # CREATE OR REPLACE can safely assume not applied


class DropMaterializedViewOps(OperationOps):
    op: DropMaterializedView

    def __init__(self, op: DropMaterializedView, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.drop_materialized_view(self.op.materialized_view_name)

    def down(self) -> CreateOrReplaceMaterializedView:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_materialized_view = sim_db.get_materialized_view(self.op.materialized_view_name)
        return CreateOrReplaceMaterializedView(materialized_view=sim_materialized_view)

    def is_up(self) -> bool:
        return not self.context.db_backend.has_materialized_view(self.op.materialized_view_name)
