from pathlib import Path

from devtools import pprint
from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.function import attach_ops, get_target_operations
from liti.core.model.v1.operation.data.base import Operation


class MigrateRunner:
    def __init__(
        self,
        db_backend: DbBackend,
        meta_backend: MetaBackend,
        target: str | list[Operation],  # either target dir or a list of the operations
    ):
        self.db_backend = db_backend
        self.meta_backend = meta_backend
        self.target = target

    def run(
        self,
        wet_run: bool = False,
        allow_down: bool = False,
        silent: bool = False,
    ):
        target_operations: list[Operation] = self.get_target_operations()

        if wet_run:
            self.meta_backend.initialize()

        migration_plan = self.meta_backend.get_migration_plan(target_operations)

        if not allow_down and migration_plan['down']:
            raise RuntimeError('Down migrations required but not allowed. Use --allow-down')

        if not silent:
            print('Down')

        for down_op in migration_plan['down']:
            down_ops = attach_ops(down_op)

            if down_ops.is_up(self.db_backend):
                if not silent:
                    pprint(down_op)

                if wet_run:
                    down_ops.down(self.db_backend, self.meta_backend)

            if wet_run:
                self.meta_backend.unapply_operation(down_op)

        if not silent:
            print('Up')

        for up_op in migration_plan['up']:
            up_ops = attach_ops(up_op)

            if not up_ops.is_up(self.db_backend):
                if not silent:
                    pprint(up_op)

                if wet_run:
                    up_ops.up(self.db_backend)

            if wet_run:
                self.meta_backend.apply_operation(up_op)

        if not silent:
            print('Done')

    def get_target_operations(self) -> list[Operation]:
        if isinstance(self.target, str):
            return get_target_operations(Path(self.target))
        else:
            return self.target
