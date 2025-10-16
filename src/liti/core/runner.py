import json
import logging
from pathlib import Path
from typing import Literal

import yaml
from devtools import pformat

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.context import Context
from liti.core.file import get_manifest_path
from liti.core.function import attach_ops
from liti.core.logger import NoOpLogger
from liti.core.model.v1.manifest import Manifest
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateSchema, CreateTable
from liti.core.model.v1.parse import parse_manifest, parse_operations, parse_templates
from liti.core.model.v1.schema import DatabaseName, Identifier, QualifiedName, SchemaName
from liti.core.model.v1.template import Template
from liti.core.observe import set_defaults, validate_model

log = logging.getLogger(__name__)


def apply_templates(file_operations: list[tuple[Path, list[Operation]]], templates: list[Template]):
    # first collect all the update functions
    update_fns = [
        # setting default values to work around late binding pass-by-reference closures
        lambda fn=update_fn, v=template.value: fn(v)
        for filename, ops in file_operations
        for template in templates
        if not template.files or filename in {Path(f) for f in template.files}
        for op in ops
        if not template.operation_kinds or type(op) in [Operation.by_kind(k) for k in template.operation_kinds]
        for root, root_match in (
            op.get_roots(template.root_type, template.full_match)
            if template.root_type is not None
            else [(op, template.full_match)]
        )
        for update_fn in root.get_update_fns(template.path, [template.local_match, root_match])
    ]

    # Then apply them so templates do not read the replacements of other templates.
    # If multiple templates update the same field, the last one wins.
    for fn in update_fns:
        fn()


class MigrateRunner:
    def __init__(self, context: Context):
        self.context = context

    @property
    def db_backend(self) -> DbBackend:
        return self.context.db_backend

    @property
    def meta_backend(self) -> MetaBackend:
        return self.context.meta_backend

    @property
    def target_dir(self) -> Path | None:
        return self.context.target_dir

    @property
    def manifest(self) -> Manifest:
        if self.context.manifest is None:
            self.context.manifest = parse_manifest(get_manifest_path(self.target_dir))

        return self.context.manifest

    @property
    def templates(self) -> list[Template] | None:
        if self.context.templates is None and self.context.template_files:
            self.context.templates = [
                template
                for path in self.context.template_files
                for template in parse_templates(path)
            ]

        return self.context.templates

    @property
    def target_operations(self) -> list[Operation]:
        if self.context.target_operations is None:
            manifest = self.manifest
            file_operations = parse_operations(manifest.operation_files, self.target_dir)

            if self.templates:
                apply_templates(file_operations, self.templates)

            self.context.target_operations = [op for _, ops in file_operations for op in ops]

        return self.context.target_operations

    def run(
        self,
        wet_run: bool | None = None,
        allow_down: bool | None = None,
    ):
        """
        :param wet_run: [False] True to run the migrations, False to simulate them
        :param allow_down: [False] True to allow down migrations, False will raise if down migrations are required
        """

        wet_run = wet_run if wet_run is not None else False
        allow_down = allow_down if allow_down is not None else False
        logger = NoOpLogger() if self.context.silent else log

        for op in self.target_operations:
            set_defaults(op, self.db_backend, self.context)
            validate_model(op, self.db_backend, self.context)

        if wet_run:
            self.meta_backend.initialize()

        migration_plan = self.meta_backend.get_migration_plan(self.target_operations)

        if not allow_down and migration_plan['down']:
            raise RuntimeError('Down migrations required but not allowed. Use --down')

        def apply_operations(operations: list[Operation], up: bool):
            for op in operations:
                if up:
                    up_op = op
                else:
                    # Down migrations apply the inverse operation
                    up_op = attach_ops(op, self.context).down()

                up_ops = attach_ops(up_op, self.context)

                # Apply only if not applied already
                if not up_ops.is_up():
                    logger.info(pformat(up_op, highlight=True))

                    if wet_run:
                        up_ops.up()

                # Update the metadata
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


def sort_operations(operations: list[Operation]) -> list[Operation]:
    """ Sorts the operations into a valid application order """
    create_schemas: list[CreateSchema] = []
    create_tables: dict[QualifiedName, CreateTable] = {}
    others: list[Operation] = []

    for op in operations:
        if isinstance(op, CreateSchema):
            create_schemas.append(op)
        elif isinstance(op, CreateTable):
            create_tables[op.table.name] = op
        else:
            others.append(op)

    sorted_ops = {}

    while create_tables:
        satisfied_ops: dict[QualifiedName, CreateTable] = {}

        for op in create_tables.values():
            if all(fk.foreign_table_name in sorted_ops for fk in op.table.foreign_keys or []):
                satisfied_ops[op.table.name] = op

        if not satisfied_ops:
            raise RuntimeError('Unsatisfied or circular foreign key references found')

        sorted_ops.update(satisfied_ops)

        for table_name in satisfied_ops:
            del create_tables[table_name]

    return create_schemas + list(sorted_ops.values()) + others


class ScanRunner:
    def __init__(self, context: Context):
        self.context = context

    @property
    def db_backend(self) -> DbBackend:
        return self.context.db_backend

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

        validate_model(database, self.db_backend, self.context)
        validate_model(schema, self.db_backend, self.context)

        if table:
            validate_model(table, self.db_backend, self.context)

            table_name = QualifiedName(database=database, schema=schema, name=table)
            create_table = self.db_backend.scan_relation(table_name)

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
