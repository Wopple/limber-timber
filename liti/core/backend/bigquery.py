import json
import logging
import re

from google.cloud.bigquery import DatasetReference, PartitionRange, QueryJobConfig, RangePartitioning, \
    ScalarQueryParameter, SchemaField, \
    Table as BqTable, TableReference, TimePartitioning
from google.cloud.bigquery.schema import _DEFAULT_VALUE
from google.cloud.bigquery.table import TableListItem

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.client.bigquery import BqClient
from liti.core.function import parse_operation
from liti.core.model.v1.data_type import Array, BigNumeric, BOOL, DataType, DATE, DATE_TIME, FLOAT64, GEOGRAPHY, INT64, \
    INTERVAL, \
    JSON, \
    Numeric, \
    Range, STRING, \
    Struct, \
    TIME, TIMESTAMP
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.schema import Column, ColumnName, DatabaseName, Identifier, Partitioning, SchemaName, Table, \
    TableName

log = logging.getLogger(__name__)

REQUIRED = 'REQUIRED'
NULLABLE = 'NULLABLE'
REPEATED = 'REPEATED'


def to_dataset_ref(database: DatabaseName, schema: SchemaName) -> DatasetReference:
    return DatasetReference(database.string, schema.string)


def extract_dataset_ref(name: TableName | TableListItem) -> DatasetReference:
    if isinstance(name, TableName):
        return to_dataset_ref(name.database, name.schema_name)
    elif isinstance(name, TableListItem):
        return DatasetReference(name.project, name.dataset_id)
    else:
        raise ValueError(f'Invalid dataset ref type: {type(name)}')


def to_table_ref(name: TableName | TableListItem) -> TableReference:
    if isinstance(name, TableName):
        return TableReference(extract_dataset_ref(name), name.table_name.string)
    elif isinstance(name, TableListItem):
        return TableReference(extract_dataset_ref(name), name.table_id)
    else:
        raise ValueError(f'Invalid table ref type: {type(name)}')


def to_field_type(data_type: DataType) -> str:
    if data_type == BOOL:
        return 'BOOL'
    elif data_type == INT64:
        return 'INT64'
    elif data_type == FLOAT64:
        return 'FLOAT64'
    elif data_type == GEOGRAPHY:
        return 'GEOGRAPHY'
    elif isinstance(data_type, Numeric):
        return 'NUMERIC'
    elif isinstance(data_type, BigNumeric):
        return 'BIGNUMERIC'
    elif data_type == STRING:
        return 'STRING'
    elif data_type == JSON:
        return 'JSON'
    elif data_type == DATE:
        return 'DATE'
    elif data_type == TIME:
        return 'TIME'
    elif data_type == DATE_TIME:
        return 'DATETIME'
    elif data_type == TIMESTAMP:
        return 'TIMESTAMP'
    elif isinstance(data_type, Range):
        return 'RANGE'
    elif data_type == INTERVAL:
        return 'INTERVAL'
    elif isinstance(data_type, Array):
        return to_field_type(data_type.inner)
    elif isinstance(data_type, Struct):
        return 'RECORD'
    else:
        raise ValueError(f'bigquery.to_field_type unrecognized data_type - {data_type}')


def to_fields(data_type: DataType) -> tuple[SchemaField, ...]:
    if isinstance(data_type, Array):
        return to_fields(data_type.inner)
    if isinstance(data_type, Struct):
        return tuple(
            to_schema_field(Column(name=n, data_type=t, nullable=True))
            for n, t in data_type.fields.items()
        )
    else:
        return ()


def to_mode(column: Column) -> str:
    if isinstance(column.data_type, Array):
        return REPEATED
    elif column.nullable:
        return NULLABLE
    else:
        return REQUIRED


def to_precision(data_type: DataType) -> int | None:
    if isinstance(data_type, Array):
        return to_precision(data_type.inner)
    elif isinstance(data_type, Numeric | BigNumeric):
        return data_type.precision
    else:
        return None


def to_scale(data_type: DataType) -> int | None:
    if isinstance(data_type, Array):
        return to_scale(data_type.inner)
    elif isinstance(data_type, Numeric | BigNumeric):
        return data_type.scale
    else:
        return None


def to_range_element_type(data_type: DataType) -> str | None:
    if isinstance(data_type, Array):
        return to_range_element_type(data_type.inner)
    elif isinstance(data_type, Range):
        return data_type.kind
    else:
        return None


def to_schema_field(column: Column) -> SchemaField:
    return SchemaField(
        name=column.name.string,
        field_type=to_field_type(column.data_type),
        mode=to_mode(column),
        default_value_expression=column.default_expression,
        description=(column.options.description if column.options else None) or _DEFAULT_VALUE,
        fields=to_fields(column.data_type),
        precision=to_precision(column.data_type),
        scale=to_scale(column.data_type),
        range_element_type=to_range_element_type(column.data_type),
        rounding_mode=column.options.rounding_mode if column.options else None,
    )


def to_data_type(schema_field: SchemaField) -> DataType:
    field_type = schema_field.field_type

    if field_type == 'BOOL':
        return BOOL
    elif field_type == 'INT64':
        return INT64
    elif field_type == 'FLOAT64':
        return FLOAT64
    elif field_type == 'GEOGRAPHY':
        return GEOGRAPHY
    elif field_type.startswith('NUMERIC'):
        matches = re.match(r'NUMERIC(?:\((\d+)(?:\s*,\s*(\d+))?\))?', field_type)

        if matches.group(1) is None:
            return Numeric()
        elif matches.group(2) is None:
            return Numeric(precision=int(matches.group(1)))
        else:
            return Numeric(precision=int(matches.group(1)), scale=int(matches.group(2)))
    elif field_type.startswith('BIGNUMERIC'):
        matches = re.match(r'BIGNUMERIC(?:\((\d+)(?:\s*,\s*(\d+))?\))?', field_type)

        if matches.group(1) is None:
            return BigNumeric()
        elif matches.group(2) is None:
            return BigNumeric(precision=int(matches.group(1)))
        else:
            return BigNumeric(precision=int(matches.group(1)), scale=int(matches.group(2)))
    elif field_type == 'STRING':
        return STRING
    elif field_type == 'JSON':
        return JSON
    elif field_type == 'DATE':
        return DATE
    elif field_type == 'TIME':
        return TIME
    elif field_type == 'DATETIME':
        return DATE_TIME
    elif field_type == 'TIMESTAMP':
        return TIMESTAMP
    elif field_type.startswith('RANGE'):
        matches = re.match(r'RANGE<(DATE|DATETIME|TIMESTAMP)>', field_type)
        return Range(kind=matches.group(1))
    elif field_type == 'INTERVAL':
        return INTERVAL
    elif field_type == 'RECORD':
        return Struct(fields={f.name: to_data_type_array(f) for f in schema_field.fields})
    else:
        raise ValueError(f'bigquery.to_data_type unrecognized field_type - {schema_field}')


def to_data_type_array(schema_field: SchemaField) -> DataType:
    if schema_field.mode == REPEATED:
        return Array(inner=to_data_type(schema_field))
    else:
        return to_data_type(schema_field)


def to_column(schema_field: SchemaField) -> Column:
    return Column(
        name=schema_field.name,
        data_type=to_data_type_array(schema_field),
        nullable=schema_field.mode != REQUIRED,
    )


def data_type_to_sql(data_type: DataType) -> str:
    if data_type == BOOL:
        return 'BOOL'
    elif data_type == INT64:
        return 'INT64'
    elif data_type == FLOAT64:
        return 'FLOAT64'
    elif data_type == GEOGRAPHY:
        return 'GEOGRAPHY'
    elif isinstance(data_type, Numeric):
        return f'NUMERIC({data_type.precision}, {data_type.scale})'
    elif isinstance(data_type, BigNumeric):
        return f'BIGNUMERIC({data_type.precision}, {data_type.scale})'
    elif data_type == STRING:
        return 'STRING'
    elif data_type == JSON:
        return 'JSON'
    elif data_type == DATE:
        return 'DATE'
    elif data_type == TIME:
        return 'TIME'
    elif data_type == DATE_TIME:
        return 'DATETIME'
    elif data_type == TIMESTAMP:
        return 'TIMESTAMP'
    elif isinstance(data_type, Range):
        return f'RANGE<{data_type.kind}>'
    elif data_type == INTERVAL:
        return 'INTERVAL'
    elif isinstance(data_type, Array):
        return f'ARRAY<{data_type_to_sql(data_type.inner)}>'
    elif isinstance(data_type, Struct):
        return f'STRUCT<{", ".join(f"{n} {data_type_to_sql(t)}" for n, t in data_type.fields.items())}>'
    else:
        raise ValueError(f'bigquery.data_type_to_sql unrecognized data_type - {data_type}')


def to_table(table: BqTable) -> Table:
    if table.time_partitioning:
        partitioning = Partitioning(
            kind='TIME',
            column=ColumnName(table.time_partitioning.field),
            time_unit=table.time_partitioning.type_,
            expiration_ms=table.time_partitioning.expiration_ms,
            require_filter=table.require_partition_filter,
        )
    elif table.range_partitioning:
        range_: PartitionRange = table.range_partitioning.range_

        partitioning = Partitioning(
            kind='INT',
            column=ColumnName(table.range_partitioning.field),
            int_start=range_.start,
            int_end=range_.end,
            int_step=range_.interval,
            require_filter=table.require_partition_filter,
        )
    else:
        partitioning = None

    return Table(
        name=TableName(table.full_table_id.replace(':', '.')),
        columns=[to_column(f) for f in table.schema],
        partitioning=partitioning,
        clustering=table.clustering_fields,
    )


class BigQueryDbBackend(DbBackend):
    """ Big Query "client" that adapts terms between liti and google.cloud.bigquery """

    def __init__(self, client: BqClient):
        self.client = client

    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        dataset = to_dataset_ref(database, schema)
        table_items = self.client.list_tables(dataset)
        tables = [self.get_table(TableName(i.full_table_id.replace(':', '.'))) for i in table_items]
        return [CreateTable(table=t) for t in tables]

    def scan_table(self, name: TableName) -> CreateTable | None:
        if self.has_table(name):
            bq_table = self.get_table(name)
            return CreateTable(table=to_table(bq_table))
        else:
            return None

    def has_table(self, name: TableName) -> bool:
        return self.client.has_table(to_table_ref(name))

    def get_table(self, name: TableName) -> Table | None:
        bq_table = self.client.get_table(to_table_ref(name))
        return bq_table and Table(name=name, columns=[to_column(field) for field in bq_table.schema])

    def create_table(self, table: Table):
        bq_table = BqTable(table.name.string, [to_schema_field(c) for c in table.columns])

        bq_table.labels
        if table.partitioning:
            if table.partitioning.kind == 'TIME':
                bq_table.time_partitioning = TimePartitioning(
                    type_=table.partitioning.time_unit,
                    field=table.partitioning.column.string if table.partitioning.column else None,
                    expiration_ms=table.partitioning.expiration_ms,
                    require_partition_filter=table.partitioning.require_filter,
                )
            elif table.partitioning.kind == 'INT':
                bq_table.range_partitioning = RangePartitioning(
                    range_=PartitionRange(
                        start=table.partitioning.int_start,
                        end=table.partitioning.int_end,
                        interval=table.partitioning.int_step,
                    ),
                    field=table.partitioning.column.string,
                )
            else:
                raise ValueError(f'Unsupported partitioning type: {table.partitioning.kind}')

        bq_table.clustering_fields = [c.string for c in table.clustering] if table.clustering else None
        self.client.create_table(bq_table)

    def drop_table(self, name: TableName):
        self.client.delete_table(to_table_ref(name))

    def rename_table(self, from_name: TableName, to_name: Identifier):
        self.client.query_and_wait(f'ALTER TABLE `{from_name}` RENAME TO `{to_name}`')

    def add_column(self, table_name: TableName, column: Column):
        column_schema_parts = []

        if column.primary_key:
            if column.primary_enforced:
                # TODO? update this if Big Query ever supports enforcement
                log.warning('Not enforcing primary key since Big Query does not support enforcement')
                column_schema_parts.append(' PRIMARY KEY NOT ENFORCED')
            else:
                column_schema_parts.append(' PRIMARY KEY NOT ENFORCED')

        if column.foreign_key:
            table_name = column.foreign_key.table_name
            column_name = column.foreign_key.column_name

            if column.foreign_enforced:
                # TODO? update this if Big Query ever supports enforcement
                log.warning('Not enforcing foreign key since Big Query does not support enforcement')
                column_schema_parts.append(f'REFERENCES {table_name}({column_name}) NOT ENFORCED')
            else:
                column_schema_parts.append(f'REFERENCES {table_name}({column_name}) NOT ENFORCED')

        if column.default_expression:
            column_schema_parts.append(f'DEFAULT {column.default_expression}')

        if not column.nullable:
            # TODO: work around this limitation with: create table > drop table > rename table
            log.warning('Adding column as nullable since Big Query does not support adding non-nullable columns')

        if column.options:
            option_parts = []

            if column.options.description:
                option_parts.append(f'description = "{column.options.description}"')

            if column.options.rounding_mode:
                option_parts.append(f'rounding_mode = "{column.options.rounding_mode}"')

            if option_parts:
                column_schema_parts.append(f'OPTIONS({", ".join(option_parts)})')

        self.client.query_and_wait(
            f'''
            ALTER TABLE `{table_name}`
            ADD COLUMN `{column.name}` {data_type_to_sql(column.data_type)} {" ".join(column_schema_parts)}
            '''
        )

    def drop_column(self, table_name: TableName, column_name: ColumnName):
        self.client.query_and_wait(f'ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`')

    def rename_column(self, table_name: TableName, from_name: ColumnName, to_name: ColumnName):
        self.client.query_and_wait(f'ALTER TABLE `{table_name}` RENAME COLUMN `{from_name}` TO `{to_name}`')

    def set_clustering(self, table_name: TableName, columns: list[ColumnName] | None):
        bq_table = self.client.get_table(to_table_ref(table_name))
        bq_table.clustering_fields = [c.string for c in columns] if columns else None
        self.client.update_table(bq_table, ['clustering_fields'])


class BigQueryMetaBackend(MetaBackend):
    def __init__(self, client: BqClient, table_name: TableName):
        self.client = client
        self.table_name = table_name

    def initialize(self):
        self.client.query_and_wait(
            f'''
            CREATE SCHEMA IF NOT EXISTS `{self.table_name.database}.{self.table_name.schema_name}`;

            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                idx INT64 NOT NULL,
                op_kind STRING NOT NULL,
                op_data JSON NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
            )
            '''
        )

    def get_applied_operations(self) -> list[Operation]:
        rows = self.client.query_and_wait(f'SELECT op_kind, op_data FROM `{self.table_name}` ORDER BY idx')

        return [parse_operation(row.op_kind, json.loads(row.op_data)) for row in rows]

    def apply_operation(self, operation: Operation):
        results = self.client.query_and_wait(
            f'''
            INSERT INTO `{self.table_name}` (idx, op_kind, op_data)
            VALUES (
                (SELECT COALESCE(MAX(idx) + 1, 0) FROM `{self.table_name}`),
                @op_kind,
                @op_data
            )
            ''',
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter('op_kind', 'STRING', operation.KIND),
                    ScalarQueryParameter('op_data', 'JSON', operation.model_dump_json()),
                ]
            )
        )

        assert results.num_dml_affected_rows == 1, f'Expected exactly 1 row inserted: {results.num_dml_affected_rows}'

    def unapply_operation(self, operation: Operation):
        results = self.client.query_and_wait(
            f'''
            DELETE FROM `{self.table_name}`
            WHERE idx = (SELECT MAX(idx) FROM `{self.table_name}`)
                AND op_kind = @op_kind

                -- ensure normalized comparison, cannot compare JSON types
                AND TO_JSON_STRING(op_data) = TO_JSON_STRING(@op_data)
            ''',
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter('op_kind', 'STRING', operation.KIND),
                    ScalarQueryParameter('op_data', 'JSON', operation.model_dump_json()),
                ]
            )
        )

        assert results.num_dml_affected_rows == 1, f'Expected exactly 1 row deleted: {results.num_dml_affected_rows}'
