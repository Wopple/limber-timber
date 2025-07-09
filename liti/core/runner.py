import logging
from pathlib import Path

from devtools import pformat

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.function import attach_ops, get_target_operations
from liti.core.logger import NoOpLogger
from liti.core.model.v1.operation.data.base import Operation

log = logging.getLogger(__name__)


class MigrateRunner:
    def __init__(
        self,
        db_backend: DbBackend,
        meta_backend: MetaBackend,
        target: str | list[Operation],  # either path to target dir or a list of the operations
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
        """
        :param wet_run: [False] True to run the migrations, False to simulate them
        :param allow_down: [False] True to allow down migrations, False will raise if down migrations are required
        :param silent: [False] True to suppress logging
        """

        logger = NoOpLogger() if silent else log
        target_operations: list[Operation] = self.get_target_operations()

        for op in target_operations:
            op.set_defaults(self.db_backend)
            op.liti_validate(self.db_backend)

        if wet_run:
            self.meta_backend.initialize()

        migration_plan = self.meta_backend.get_migration_plan(target_operations)

        if not allow_down and migration_plan['down']:
            raise RuntimeError('Down migrations required but not allowed. Use --down')

        def apply_operations(operations: list[Operation], up: bool):
            for op in operations:
                if up:
                    up_op = op
                else:
                    # Down migrations apply the inverse operation
                    up_op = attach_ops(op).down(self.db_backend, self.meta_backend)

                up_ops = attach_ops(up_op)

                if not up_ops.is_up(self.db_backend):
                    logger.info(pformat(up_op))

                    if wet_run:
                        up_ops.up(self.db_backend)

                if wet_run:
                    if up:
                        self.meta_backend.apply_operation(op)
                    else:
                        self.meta_backend.unapply_operation(op)

        logger.info('Down')
        apply_operations(migration_plan['down'], False)
        logger.info('Up')
        apply_operations(migration_plan['up'], True)
        logger.info('Done')

    def get_target_operations(self) -> list[Operation]:
        if isinstance(self.target, str):
            return get_target_operations(Path(self.target))
        else:
            return self.target
