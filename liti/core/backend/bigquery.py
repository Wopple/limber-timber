import json
import logging
from datetime import timezone

from google.cloud.bigquery import DatasetReference, PartitionRange, QueryJobConfig, \
    ScalarQueryParameter, SchemaField, \
    Table as BqTable, TableReference
from google.cloud.bigquery.schema import _DEFAULT_VALUE
from google.cloud.bigquery.table import TableListItem

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.client.bigquery import BqClient
from liti.core.function import parse_operation
from liti.core.model.v1.datatype import Array, BigNumeric, BOOL, DataType, DATE, Date, DATE_TIME, DateTime, Float, \
    FLOAT64, \
    GEOGRAPHY, \
    Int, \
    INT64, \
    INTERVAL, \
    JSON, \
    Numeric, \
    Range, STRING, \
    Struct, \
    TIME, TIMESTAMP, Timestamp
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateTable
from liti.core.model.v1.schema import Column, ColumnName, DatabaseName, Identifier, IntervalLiteral, Partitioning, \
    RoundingModeLiteral, \
    SchemaName, Table, \
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

    if field_type in ('BOOL', 'BOOLEAN'):
        return BOOL
    elif field_type in ('INT64', 'INTEGER'):
        return INT64
    elif field_type in ('FLOAT64', 'FLOAT'):
        return FLOAT64
    elif field_type == 'GEOGRAPHY':
        return GEOGRAPHY
    elif field_type == 'NUMERIC':
        return Numeric(
            precision=schema_field.precision,
            scale=schema_field.scale,
        )
    elif field_type == 'BIGNUMERIC':
        return BigNumeric(
            precision=schema_field.precision,
            scale=schema_field.scale,
        )
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
    elif field_type == 'RANGE':
        return Range(kind=schema_field.range_element_type.element_type)
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


def interval_literal_to_sql(interval: IntervalLiteral) -> str:
    from_str: str = next(filter(lambda x: x[0] != 0, [
        (interval.year, 'YEAR'),
        (interval.month, 'MONTH'),
        (interval.day, 'DAY'),
        (interval.hour, 'HOUR'),
        (interval.minute, 'MINUTE'),
        (interval.second, 'SECOND'),
        (interval.microsecond, 'SECOND'),
    ]))[1]

    to_str: str = next(filter(lambda x: x[0] != 0, [
        (interval.microsecond, 'MICROSECOND'),
        (interval.second, 'SECOND'),
        (interval.minute, 'MINUTE'),
        (interval.hour, 'HOUR'),
        (interval.day, 'DAY'),
        (interval.month, 'MONTH'),
        (interval.year, 'YEAR'),
    ]))[1]

    if interval.year > 0:
        if to_str == 'YEAR':
            year_part = f'{interval.year}'
        else:
            year_part = f'{interval.year}-'
    else:
        year_part = ''

    if interval.month > 0:
        if to_str == 'MONTH':
            month_part = f'{interval.month}'
        else:
            month_part = f'{interval.month} '
    else:
        month_part = ''

    if interval.day > 0:
        if to_str == 'DAY':
            day_part = f'{interval.day}'
        else:
            day_part = f'{interval.day} '
    else:
        day_part = ''

    if interval.hour > 0:
        if to_str == 'HOUR':
            hour_part = f'{interval.hour}'
        else:
            hour_part = f'{interval.hour}:'
    else:
        hour_part = ''

    if interval.minute > 0:
        if to_str == 'MINUTE':
            minute_part = f'{interval.minute}'
        else:
            minute_part = f'{interval.minute}:'
    else:
        minute_part = ''

    if interval.second > 0:
        if to_str == 'SECOND':
            second_part = f'{interval.second}'
        else:
            second_part = f'{interval.second}.'
    else:
        second_part = ''

    if interval.microsecond > 0:
        microsecond_part = f'{interval.microsecond}'
    else:
        microsecond_part = ''

    year_month_part = f'{year_part}{month_part}'
    time_part = f'{hour_part}{minute_part}{second_part}{microsecond_part}'
    sign_part = '-' if interval.sign == '-' else ''

    if year_month_part:
        year_month_part = f'{sign_part}{year_month_part}'

    if day_part:
        day_part = f'{sign_part}{day_part}'

    if time_part:
        time_part = f'{sign_part}{time_part}'

    if to_str == 'MICROSECOND':
        to_str = 'SECOND'

    if from_str == to_str:
        range_part = from_str
    else:
        range_part = f'{from_str} TO {to_str}'

    return f'INTERVAL "{year_month_part}{day_part}{time_part}" {range_part}'


def column_to_sql(column: Column, mode: str | None = None) -> str:
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

    if mode == 'add' and not column.nullable:
        # TODO: work around this limitation with: create table > drop table > rename table
        log.warning('Adding column as nullable since Big Query does not support adding non-nullable columns')

    option_parts = []

    if column.description:
        option_parts.append(f'description = "{column.description}"')

    if column.rounding_mode:
        option_parts.append(f'rounding_mode = "{column.rounding_mode}"')

    if option_parts:
        column_schema_parts.append(f'OPTIONS({", ".join(option_parts)})')

    return f'`{column.name}` {data_type_to_sql(column.data_type)} {" ".join(column_schema_parts)}'


def option_dict_to_sql(option: dict[str, str]) -> str:
    join_sql = ', '.join(f'("{k}", "{v}")' for k, v in option.items())
    return f'[{join_sql}]'


def to_table(table: BqTable) -> Table:
    if table.time_partitioning:
        time_partition = table.time_partitioning
        partitioning = Partitioning(
            kind='TIME',
            column=ColumnName(time_partition.field),
            time_unit=time_partition.type_,
            expiration_days=time_partition.expiration_ms and time_partition.expiration_ms / (1000 * 60 * 60 * 24),
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

    # backend methods

    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        dataset = to_dataset_ref(database, schema)
        table_items = self.client.list_tables(dataset)
        tables = [self.get_table(TableName(i.full_table_id.replace(':', '.'))) for i in table_items]
        return [CreateTable(table=t) for t in tables]

    def scan_table(self, name: TableName) -> CreateTable | None:
        if self.has_table(name):
            return CreateTable(table=self.get_table(name))
        else:
            return None

    def has_table(self, name: TableName) -> bool:
        return self.client.has_table(to_table_ref(name))

    def get_table(self, name: TableName) -> Table | None:
        bq_table = self.client.get_table(to_table_ref(name))
        return bq_table and to_table(bq_table)

    def create_table(self, table: Table):
        column_sqls = [column_to_sql(column) for column in table.columns]

        if table.partitioning:
            partitioning = table.partitioning
            column = partitioning.column

            if partitioning.kind == 'TIME':
                if column:
                    data_type = table.column_map[column].data_type

                    if isinstance(data_type, Date):
                        partition_sql = f'PARTITION BY `{column}`\n'
                    elif isinstance(data_type, DateTime):
                        partition_sql = f'PARTITION BY DATETIME_TRUNC(`{column}`, {partitioning.time_unit})\n'
                    elif isinstance(data_type, Timestamp):
                        partition_sql = f'PARTITION BY TIMESTAMP_TRUNC(`{column}`, {partitioning.time_unit})\n'
                    else:
                        raise ValueError(f'Unsupported partitioning column data type: {data_type}')
                else:
                    partition_sql = f'PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, {partitioning.time_unit})\n'
            elif table.partitioning.kind == 'INT':
                start = partitioning.int_start
                end = partitioning.int_end
                step = partitioning.int_step
                partition_sql = f'PARTITION BY RANGE_BUCKET(`{column}`, GENERATE_ARRAY({start}, {end}, {step}))\n'
            else:
                raise ValueError(f'Unsupported partitioning type: {table.partitioning.kind}')
        else:
            partition_sql = ''

        if table.clustering:
            cluster_sql = f'CLUSTER BY {", ".join(f"`{c}`" for c in table.clustering)}\n'
        else:
            cluster_sql = ''

        options = []

        if table.friendly_name:
            options.append(f'friendly_name = "{table.friendly_name}"')

        if table.description:
            options.append(f'description = "{table.description}"')

        if table.labels:
            options.append(f'labels = [{option_dict_to_sql(table.labels)}]')

        if table.tags:
            options.append(f'tags = [{option_dict_to_sql(table.tags)}]')

        if table.expiration_timestamp:
            utc_ts = table.expiration_timestamp.astimezone(timezone.utc)
            options.append(f'expiration_timestamp = TIMESTAMP "{utc_ts.strftime("%Y-%m-%d %H:%M:%S UTC")}"')

        if table.default_rounding_mode:
            options.append(f'default_rounding_mode = "{table.default_rounding_mode}"')

        if table.max_staleness:
            options.append(f'max_staleness = {interval_literal_to_sql(table.max_staleness)}')

        if table.enable_change_history:
            options.append(f'enable_change_history = TRUE')

        if table.enable_fine_grained_mutations:
            options.append(f'enable_fine_grained_mutations = TRUE')

        if table.kms_key_name:
            options.append(f'kms_key_name = "{table.kms_key_name}"')

        if table.storage_uri:
            options.append(f'storage_uri = "{table.storage_uri}"')

        if table.file_format:
            options.append(f'file_format = {table.file_format}')

        if table.table_format:
            options.append(f'table_format = {table.table_format}')

        if options:
            options_sql = (
                f'OPTIONS(\n'
                f'    {",\n    ".join(options)}\n'
                f')\n'
            )
        else:
            options_sql = ''

        self.client.query_and_wait((
            f'CREATE TABLE `{table.name}` (\n'
            f'    {",\n    ".join(column_sqls)}\n'
            f')\n'
            f'{partition_sql}'
            f'{cluster_sql}'
            f'{options_sql}'
        ))

    def drop_table(self, name: TableName):
        self.client.delete_table(to_table_ref(name))

    def rename_table(self, from_name: TableName, to_name: Identifier):
        self.client.query_and_wait(f'ALTER TABLE `{from_name}` RENAME TO `{to_name}`')

    def set_clustering(self, table_name: TableName, columns: list[ColumnName] | None):
        bq_table = self.client.get_table(to_table_ref(table_name))
        bq_table.clustering_fields = [c.string for c in columns] if columns else None
        self.client.update_table(bq_table, ['clustering_fields'])

    def set_description(self, table_name: TableName, description: str | None):
        self.set_option(table_name, 'description', f'"{description}"' if description else 'NULL')

    def set_labels(self, table_name: TableName, labels: dict[str, str] | None):
        self.set_option(table_name, 'labels', option_dict_to_sql(labels) if labels else 'NULL')

    def set_tags(self, table_name: TableName, tags: dict[str, str] | None):
        self.set_option(table_name, 'tags', option_dict_to_sql(tags) if tags else 'NULL')

    def set_default_rounding_mode(self, table_name: TableName, rounding_mode: RoundingModeLiteral):
        self.set_option(table_name, 'default_rounding_mode', f'"{rounding_mode}"')

    def add_column(self, table_name: TableName, column: Column):
        self.client.query_and_wait((
            f'ALTER TABLE `{table_name}`\n'
            f'ADD COLUMN {column_to_sql(column, 'add')}\n'
        ))

    def drop_column(self, table_name: TableName, column_name: ColumnName):
        self.client.query_and_wait(f'ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`')

    def rename_column(self, table_name: TableName, from_name: ColumnName, to_name: ColumnName):
        self.client.query_and_wait(f'ALTER TABLE `{table_name}` RENAME COLUMN `{from_name}` TO `{to_name}`')

    def set_column_description(self, table_name: TableName, column_name: ColumnName, description: str | None):
        self.set_column_option(table_name, column_name, 'description', f'"{description}"' if description else 'NULL')

    def set_column_rounding_mode(self, table_name: TableName, column_name: ColumnName, rounding_mode: RoundingModeLiteral):
        self.set_column_option(table_name, column_name, 'rounding_mode', f'"{rounding_mode}"')

    # default methods

    def int_defaults(self, node: Int):
        node.bits = node.bits or 64

    def float_defaults(self, node: Float):
        node.bits = node.bits or 64

    def numeric_defaults(self, node: Numeric):
        node.precision = node.precision or 38
        node.scale = node.scale or 9

    def big_numeric_defaults(self, node: BigNumeric):
        node.precision = node.precision or 76
        node.scale = node.scale or 38

    def partitioning_defaults(self, node: Partitioning):
        if node.kind == 'TIME':
            node.time_unit = node.time_unit or 'DAY'
        elif node.kind == 'INT':
            node.int_step = node.int_step or 1

    def rounding_mode_defaults(self, node: RoundingModeLiteral):
        if node.string is None:
            node.string = 'ROUND_HALF_EVEN'

    def table_defaults(self, node: Table):
        if node.expiration_timestamp is not None and node.expiration_timestamp.tzinfo is None:
            node.expiration_timestamp = node.expiration_timestamp.replace(tzinfo=timezone.utc)

        if node.enable_change_history is None:
            node.enable_change_history = False

        if node.enable_fine_grained_mutations is None:
            node.enable_fine_grained_mutations = False

    # validation methods

    def validate_int(self, node: Int):
        if node.bits != 64:
            raise ValueError(f'Int.bits must be 64: {node.bits}')

    def validate_float(self, node: Float):
        if node.bits != 64:
            raise ValueError(f'Float.bits must be 64: {node.bits}')

    def validate_numeric(self, node: Numeric):
        if not (0 <= node.scale <= 9):
            raise ValueError(f'Numeric.scale must be between 0 and 9: {node.scale}')

        if not (max(1, node.scale) <= node.precision <= node.scale + 29):
            raise ValueError(
                f'Numeric.precision must be between {max(1, node.scale)} and {node.scale + 29}: {node.precision}')

    def validate_big_numeric(self, node: BigNumeric):
        if not (0 <= node.scale <= 38):
            raise ValueError(f'Scale must be between 0 and 38: {node.scale}')

        if not (max(1, node.scale) <= node.precision <= node.scale + 38):
            raise ValueError(f'Precision must be between {max(1, node.scale)} and {node.scale + 38}: {node.precision}')

    def validate_array(self, node: Array):
        if isinstance(node.inner, Array):
            raise ValueError('Nested arrays are not allowed')

    def validate_partitioning(self, node: Partitioning):
        if node.kind == 'TIME':
            required = ['kind', 'time_unit', 'require_filter']
            allowed = required + ['column', 'expiration_days']
        elif node.kind == 'INT':
            required = ['kind', 'column', 'int_start', 'int_end', 'int_step', 'require_filter']
            allowed = required
        else:
            raise ValueError(f'Invalid partitioning kind: {node.kind}')

        missing = [
            field_name
            for field_name in required
            if getattr(node, field_name) is None
        ]

        present = [
            field_name
            for field_name in Partitioning.model_fields.keys()
            if field_name not in allowed and getattr(node, field_name) is not None
        ]

        errors = [
            *[f'Missing required field for {node.kind}: {field_name}' for field_name in missing],
            *[f'Disallowed field present for {node.kind}: {field_name}' for field_name in present],
        ]

        if errors:
            raise ValueError('\n'.join(errors))

    # class methods
    def set_option(self, table_name: TableName, key: str, value: str):
        self.client.query_and_wait((
            f'ALTER TABLE `{table_name}`\n'
            f'SET OPTIONS({key} = {value})\n'
        ))

    def set_column_option(self, table_name: TableName, column_name: ColumnName, key: str, value: str):
        self.client.query_and_wait((
            f'ALTER TABLE `{table_name}`\n'
            f'ALTER COLUMN `{column_name}`\n'
            f'SET OPTIONS({key} = {value})\n'
        ))


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
            (
                f'DELETE FROM `{self.table_name}`\n'
                f'WHERE idx = (SELECT MAX(idx) FROM `{self.table_name}`)\n'
                f'    AND op_kind = @op_kind\n'
                # ensure normalized comparison, cannot compare JSON types
                f'    AND TO_JSON_STRING(op_data) = TO_JSON_STRING(@op_data)\n'
            ),
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter('op_kind', 'STRING', operation.KIND),
                    ScalarQueryParameter('op_data', 'JSON', operation.model_dump_json()),
                ]
            )
        )

        assert results.num_dml_affected_rows == 1, f'Expected exactly 1 row deleted: {results.num_dml_affected_rows}'
