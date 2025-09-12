from liti.core.model.v1.operation.data.sql import ExecuteSql
from liti.core.model.v1.operation.ops.base import OperationOps


class ExecuteSqlOps(OperationOps):
    op: ExecuteSql

    def __init__(self, op: ExecuteSql):
        self.op = op

    def up(self):
        if self.context.target_dir is not None:
            path = self.context.target_dir / self.op.up
        else:
            path = self.op.up

        with open(path) as f:
            sql = f.read()

        if self.op.entity_names:
            sql = sql.format(**self.op.entity_names)

        self.context.db_backend.execute_sql(sql)

    def down(self) -> ExecuteSql:
        return ExecuteSql(
            up=self.op.down,
            down=self.op.up,
            is_up=self.op.is_down,
            is_down=self.op.is_up,
        )

    def is_up(self) -> bool:
        if self.context.target_dir is not None:
            path = self.context.target_dir / self.op.is_up
        else:
            path = self.op.is_up

        if isinstance(self.op.is_up, str):
            with open(path) as f:
                sql = f.read()

            if self.op.entity_names:
                sql = sql.format(**self.op.entity_names)

            return self.context.db_backend.execute_bool_value_query(sql)
        elif isinstance(self.op.is_up, bool):
            return self.op.is_up
        else:
            raise ValueError(f'is_up must be a string or boolean: {self.op.is_up}')
