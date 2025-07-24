from pathlib import Path

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.sql import ExecuteSql
from liti.core.model.v1.operation.ops.base import OperationOps


class ExecuteSqlOps(OperationOps):
    op: ExecuteSql

    def __init__(self, op: ExecuteSql):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        if target_dir is not None:
            path = target_dir / self.op.is_up
        else:
            path = self.op.is_up

        with open(path) as f:
            sql = f.read()

        db_backend.execute_sql(sql)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> ExecuteSql:
        return ExecuteSql(
            up=self.op.down,
            down=self.op.up,
            is_up=self.op.is_down,
            is_down=self.op.is_up,
        )

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        if target_dir is not None:
            path = target_dir / self.op.is_up
        else:
            path = self.op.is_up

        if isinstance(self.op.is_up, str):
            with open(path) as f:
                sql = f.read()

            return db_backend.execute_bool_value_query(sql)
        elif isinstance(self.op.is_up, bool):
            return self.op.is_up
        else:
            raise ValueError(f'is_up must be a string or boolean: {self.op.is_up}')
