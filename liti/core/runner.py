from pathlib import Path
from pprint import pprint

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.function import attach_ops, get_target_operations
from liti.core.model.v1.operation.data.base import Operation


class MigrationsRunner:
    def __init__(
        self,
        db_backend: DbBackend,
        meta_backend: MetaBackend,
        target: str | list[Operation],  # str variant is a path to the target dir
    ):
        self.db_backend = db_backend
        self.meta_backend = meta_backend
        self.target = target

    def run(
        self,
        dry_run: bool = True,
        allow_down: bool = False,
    ):
        target_operations: list[Operation] = self.get_target_operations()
        migration_plan = self.meta_backend.get_migration_plan(target_operations)

        if not allow_down and migration_plan['down']:
            raise RuntimeError('Down not allowed, but down migrations required. Run with --allow-down')

        for down_op in migration_plan['down']:
            down_ops = attach_ops(down_op)

            if down_ops.is_up(self.db_backend):
                print('Down')
                pprint(down_op.model_dump(), width=100)

                if not dry_run:
                    down_ops.down(self.db_backend)

            if not dry_run:
                self.meta_backend.unapply_operation(down_op)

        for up_op in migration_plan['up']:
            up_ops = attach_ops(up_op)

            if not up_ops.is_up(self.db_backend):
                print('Up')
                pprint(up_op.model_dump(), width=100)

                if not dry_run:
                    up_ops.up(self.db_backend)

            if not dry_run:
                self.meta_backend.apply_operation(up_op)

    def get_target_operations(self) -> list[Operation]:
        if isinstance(self.target, str):
            return get_target_operations(Path(self.target))
        else:
            return self.target
