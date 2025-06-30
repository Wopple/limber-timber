from pathlib import Path

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.function import get_target_operations
from liti.core.model.operation.data.base import Operation


class MigrationsRunner:
    def __init__(self, db_backend: DbBackend, meta_backend: MetaBackend):
        self.db_backend = db_backend
        self.meta_backend = meta_backend

    def run(
        self,
        target_dir: str,
        dry_run: bool = True,
        allow_down: bool = False,
    ):
        # get the target operations
        # get the migration plan
        # raise if not allow down and there are down operations
        # if dry run
        # T
        # print the operations
        # F
        # unapply the down operations
        # apply the up operations

        target_operations: list[Operation] = get_target_operations(Path(target_dir))
