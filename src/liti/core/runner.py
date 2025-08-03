import json
import logging
from pathlib import Path
from typing import Literal

import yaml
from devtools import pformat

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.function import attach_ops, get_manifest_path, parse_manifest, parse_operations
from liti.core.logger import NoOpLogger
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.schema import DatabaseName, Identifier, SchemaName, TableName

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

        if self.target_dir:
            manifest = parse_manifest(get_manifest_path(self.target_dir))
            target_operations = parse_operations(manifest)

            if manifest.templates:
                for op in target_operations:
                    for template in manifest.templates:
                        for fn in op.get_update_fns(template.path, template.match):
                            fn(template.value)
        elif isinstance(self.target, list):
            target_operations: list[Operation] = self.target
        else:
            raise ValueError('Unable to determine target operations')

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

                if not up_ops.is_up(self.db_backend, self.target_dir):
                    if up:
                        logger.info(f'\033[32m{pformat(up_op)}\033[0m')  # Green
                    else:
                        logger.info(f'\033[31m{pformat(up_op)}\033[0m')  # Red

                    if wet_run:
                        up_ops.up(self.db_backend, self.meta_backend, self.target_dir)

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

    @property
    def target_dir(self) -> Path | None:
        if isinstance(self.target, str):
            return Path(self.target)
        else:
            return None


def sort_operations(operations: list[Operation]) -> list[Operation]:
    """ Sorts the operations into a valid application order """
    create_tables: dict[TableName, CreateTable] = {}
    others: list[Operation] = []

    for op in operations:
        if isinstance(op, CreateTable):
            create_tables[op.table.name] = op
        else:
            others.append(op)

    sorted_ops = {}

    while create_tables:
        satisfied_ops: dict[TableName, CreateTable] = {}

        for op in create_tables.values():
            if all(fk.foreign_table_name in sorted_ops for fk in op.table.foreign_keys):
                satisfied_ops[op.table.name] = op

        if not satisfied_ops:
            raise RuntimeError('Unsatisfied or circular foreign key references found')

        sorted_ops.update(satisfied_ops)

        for table_name in satisfied_ops:
            del create_tables[table_name]

    return list(sorted_ops.values()) + others


class ScanRunner:
    def __init__(self, db_backend: DbBackend):
        self.db_backend = db_backend

    def run(
        self,
        database: DatabaseName,
        schema: SchemaName,
        table: Identifier | None = None,
        format: Literal['json', 'yaml'] = 'yaml',
    ):
        """
        :param database: database to scan
        :param schema: schema to scan
        :param table: [None] None scans the whole schema, otherwise scans only the provided table
        :param format: ['yaml'] the format to use when printing the operations
        """

        database.liti_validate(self.db_backend)
        schema.liti_validate(self.db_backend)

        if table:
            table.liti_validate(self.db_backend)

            table_name = TableName(database=database, schema_name=schema, table_name=table)
            create_table = self.db_backend.scan_table(table_name)

            if create_table is None:
                raise RuntimeError(f'Table not found: {table_name}')

            operations = [create_table]
        else:
            operations = sort_operations(self.db_backend.scan_schema(database, schema))

        op_data = [op.to_op_data(format=format) for op in operations]

        file_data = {
            'version': 1,
            'operations': op_data,
        }

        if format == 'json':
            print(json.dumps(file_data, indent=4, sort_keys=False))
        elif format == 'yaml':
            print(yaml.dump(file_data, indent=2, sort_keys=False))
        else:
            raise ValueError(f'Unsupported format: {format}')
