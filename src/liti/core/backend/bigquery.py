import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from liti import bigquery as bq
from liti.core.backend.base import CreateRelation, DbBackend, MetaBackend
from liti.core.client.bigquery import BqClient
from liti.core.context import Context
from liti.core.error import Unsupported, UnsupportedError
from liti.core.model.v1.datatype import Array, BigNumeric, BOOL, Bytes, Datatype, DATE, Date, DATE_TIME, DateTime, \
    Float, FLOAT64, GEOGRAPHY, Int, INT64, INTERVAL, JSON, Numeric, Range, String, Struct, TIME, TIMESTAMP, Timestamp
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.operation.data.table import CreateSchema, CreateTable
from liti.core.model.v1.operation.data.view import CreateMaterializedView, CreateView
from liti.core.model.v1.parse import parse_operation
from liti.core.model.v1.schema import BigLake, Column, ColumnName, ConstraintName, DatabaseName, FieldPath, ForeignKey, \
    ForeignReference, Identifier, IntervalLiteral, MaterializedView, Partitioning, PrimaryKey, QualifiedName, Relation, \
    RoundingMode, Schema, SchemaName, StorageBilling, Table, View

log = logging.getLogger(__name__)

REQUIRED = 'REQUIRED'
NULLABLE = 'NULLABLE'
REPEATED = 'REPEATED'

ONE_MINUTE_IN_SECONDS = 60
ONE_HOUR_IN_SECONDS = 60 * ONE_MINUTE_IN_SECONDS
ONE_DAY_IN_SECONDS = 24 * ONE_HOUR_IN_SECONDS
ONE_SECOND_IN_MILLIS = 1000
ONE_DAY_IN_MILLIS = ONE_DAY_IN_SECONDS * ONE_SECOND_IN_MILLIS


def to_dataset_ref(project: DatabaseName, dataset: SchemaName) -> bq.DatasetReference:
    return bq.DatasetReference(project.string, dataset.string)


def extract_dataset_ref(name: QualifiedName | bq.TableListItem) -> bq.DatasetReference:
    if isinstance(name, QualifiedName):
        return to_dataset_ref(name.database, name.schema_name)
    elif isinstance(name, bq.TableListItem):
        return bq.DatasetReference(name.project, name.dataset_id)
    else:
        raise ValueError(f'Invalid dataset ref type: {type(name)}')


def to_table_ref(name: QualifiedName | bq.TableListItem) -> bq.TableReference:
    if isinstance(name, QualifiedName):
        return bq.TableReference(extract_dataset_ref(name), name.name.string)
    elif isinstance(name, bq.TableListItem):
        return bq.TableReference(extract_dataset_ref(name), name.table_id)
    else:
        raise ValueError(f'Invalid table ref type: {type(name)}')


def to_field_type(datatype: Datatype) -> str:
    if datatype == BOOL:
        return 'BOOL'
    elif isinstance(datatype, Int) and datatype.bits == 64:
        return 'INT64'
    elif isinstance(datatype, Float) and datatype.bits == 64:
        return 'FLOAT64'
    elif datatype == GEOGRAPHY:
        return 'GEOGRAPHY'
    elif isinstance(datatype, Numeric):
        return 'NUMERIC'
    elif isinstance(datatype, BigNumeric):
        return 'BIGNUMERIC'
    elif isinstance(datatype, String):
        return 'STRING'
    elif isinstance(datatype, Bytes):
        return 'BYTES'
    elif datatype == JSON:
        return 'JSON'
    elif datatype == DATE:
        return 'DATE'
    elif datatype == TIME:
        return 'TIME'
    elif datatype == DATE_TIME:
        return 'DATETIME'
    elif datatype == TIMESTAMP:
        return 'TIMESTAMP'
    elif isinstance(datatype, Range):
        return 'RANGE'
    elif datatype == INTERVAL:
        return 'INTERVAL'
    elif isinstance(datatype, Array):
        return to_field_type(datatype.inner)
    elif isinstance(datatype, Struct):
        return 'RECORD'
    else:
        raise ValueError(f'bigquery.to_field_type unrecognized datatype - {datatype}')


def to_mode(column: Column) -> str:
    if isinstance(column.datatype, Array):
        return REPEATED
    elif column.nullable:
        return NULLABLE
    else:
        return REQUIRED


def to_fields(datatype: Datatype) -> tuple[bq.SchemaField, ...]:
    if isinstance(datatype, Array):
        return to_fields(datatype.inner)
    if isinstance(datatype, Struct):
        return tuple(
            to_schema_field(Column(name, dt, nullable=True))
            for name, dt in datatype.fields.items()
        )
    else:
        return ()


def to_precision(datatype: Datatype) -> int | None:
    if isinstance(datatype, Array):
        return to_precision(datatype.inner)
    elif isinstance(datatype, Numeric | BigNumeric):
        return datatype.precision
    else:
        return None


def to_scale(datatype: Datatype) -> int | None:
    if isinstance(datatype, Array):
        return to_scale(datatype.inner)
    elif isinstance(datatype, Numeric | BigNumeric):
        return datatype.scale
    else:
        return None


def to_max_length(datatype: Datatype) -> int | None:
    if isinstance(datatype, Array):
        return to_max_length(datatype.inner)
    elif isinstance(datatype, String):
        return datatype.characters
    elif isinstance(datatype, Bytes):
        return datatype.bytes
    else:
        return None


def to_range_element_type(datatype: Datatype) -> str | None:
    if isinstance(datatype, Array):
        return to_range_element_type(datatype.inner)
    elif isinstance(datatype, Range):
        return datatype.kind
    else:
        return None


def to_schema_field(column: Column) -> bq.SchemaField:
    return bq.SchemaField(
        name=column.name.string,
        field_type=to_field_type(column.datatype),
        mode=to_mode(column),
        default_value_expression=column.default_expression,
        description=column.description or bq.SCHEMA_DEFAULT_VALUE,
        fields=to_fields(column.datatype),
        precision=to_precision(column.datatype),
        scale=to_scale(column.datatype),
        max_length=to_max_length(column.datatype),
        range_element_type=to_range_element_type(column.datatype),
        rounding_mode=column.rounding_mode and column.rounding_mode.string,
    )


def to_bq_foreign_key(foreign_key: ForeignKey) -> bq.ForeignKey:
    return bq.ForeignKey(
        name=foreign_key.name.string,
        referenced_table=to_table_ref(foreign_key.foreign_table_name),
        column_references=[
            bq.ColumnReference(
                referencing_column=ref.local_column_name.string,
                referenced_column=ref.foreign_column_name.string,
            )
            for ref in foreign_key.references
        ],
    )


def set_bq_partitioning(relation: Relation, bq_table: bq.Table):
    if relation.partitioning:
        bq_table.require_partition_filter = relation.partitioning.require_filter

        if relation.partitioning.kind == 'TIME':
            if relation.partitioning.expiration is not None:
                expiration_ms = int(relation.partitioning.expiration.total_seconds() * ONE_SECOND_IN_MILLIS)
            else:
                expiration_ms = None

            bq_table.time_partitioning = bq.TimePartitioning(
                type_=relation.partitioning.time_unit,
                field=relation.partitioning.column.string if relation.partitioning.column else None,
                expiration_ms=expiration_ms,
            )
        elif relation.partitioning.kind == 'INT':
            bq_table.range_partitioning = bq.RangePartitioning(
                field=relation.partitioning.column.string,
                range_=bq.PartitionRange(
                    start=relation.partitioning.int_start,
                    end=relation.partitioning.int_end,
                    interval=relation.partitioning.int_step,
                ),
            )
        else:
            raise ValueError(f'Unrecognized partitioning kind: {relation.partitioning}')


def table_to_bq_table(table: Table) -> bq.Table:
    bq_table = bq.Table(
        table_ref=to_table_ref(table.name),
        schema=[to_schema_field(column) for column in table.columns],
    )

    table_constraints = None

    if table.primary_key or table.foreign_keys:
        if table.primary_key:
            primary_key = bq.PrimaryKey([col.string for col in table.primary_key.column_names])
        else:
            primary_key = None

        if table.foreign_keys:
            foreign_keys = [to_bq_foreign_key(fk) for fk in table.foreign_keys]
        else:
            foreign_keys = None

        table_constraints = bq.TableConstraints(
            primary_key=primary_key,
            foreign_keys=foreign_keys,
        )

    if table.big_lake:
        bq_table.biglake_configuration = bq.BigLakeConfiguration(
            connection_id=table.connection_id,
            storage_uri=table.storage_uri,
            file_format=table.file_format,
            table_format=table.table_format,
        )

    bq_table.table_constraints = table_constraints
    set_bq_partitioning(table, bq_table)
    bq_table.clustering_fields = [field.string for field in table.clustering] if table.clustering else None
    bq_table.friendly_name = table.friendly_name
    bq_table.description = table.description
    bq_table.labels = table.labels
    bq_table.resource_tags = table.tags
    bq_table.expires = table.expiration_timestamp
    bq_table.max_staleness = table.max_staleness and interval_literal_to_str(table.max_staleness)
    return bq_table


def view_to_bq_table(view: View) -> bq.Table:
    bq_table = bq.Table(to_table_ref(view.name))
    bq_table.friendly_name = view.friendly_name
    bq_table.description = view.description
    bq_table.labels = view.labels
    bq_table.resource_tags = view.tags
    bq_table.expires = view.expiration_timestamp
    bq_table.view_query = view.formatted_select_sql

    # TODO: figure out what to do about bq.Table missing fields
    return bq_table


def materialized_view_to_bq_table(materialized_view: MaterializedView) -> bq.Table:
    bq_table = bq.Table(to_table_ref(materialized_view.name))
    set_bq_partitioning(materialized_view, bq_table)
    bq_table.clustering_fields = [field.string for field in materialized_view.clustering] if materialized_view.clustering else None
    bq_table.friendly_name = materialized_view.friendly_name
    bq_table.description = materialized_view.description
    bq_table.labels = materialized_view.labels
    bq_table.resource_tags = materialized_view.tags
    bq_table.expires = materialized_view.expiration_timestamp
    bq_table.mview_query = materialized_view.formatted_select_sql
    bq_table.mview_allow_non_incremental_definition = materialized_view.allow_non_incremental_definition
    bq_table.mview_enable_refresh = materialized_view.enable_refresh
    bq_table.mview_refresh_interval = materialized_view.refresh_interval

    # TODO: figure out what to do about bq.Table missing fields
    return bq_table


def to_bq_table(relation: Relation) -> bq.Table:
    if isinstance(relation, Table):
        return table_to_bq_table(relation)
    elif isinstance(relation, View):
        return view_to_bq_table(relation)
    elif isinstance(relation, MaterializedView):
        return materialized_view_to_bq_table(relation)
    else:
        raise ValueError(f'Unrecognized relation type: {relation}')


def datatype_to_sql(datatype: Datatype) -> str:
    if datatype == BOOL:
        return 'BOOL'
    elif datatype == INT64:
        return 'INT64'
    elif datatype == FLOAT64:
        return 'FLOAT64'
    elif datatype == GEOGRAPHY:
        return 'GEOGRAPHY'
    elif isinstance(datatype, Numeric):
        if datatype.precision:
            if datatype.scale:
                return f'NUMERIC({datatype.precision}, {datatype.scale})'
            else:
                return f'NUMERIC({datatype.precision})'
        else:
            return f'NUMERIC'
    elif isinstance(datatype, BigNumeric):
        if datatype.precision:
            if datatype.scale:
                return f'BIGNUMERIC({datatype.precision}, {datatype.scale})'
            else:
                return f'BIGNUMERIC({datatype.precision})'
        else:
            return f'BIGNUMERIC'
    elif isinstance(datatype, String):
        if datatype.characters is None:
            return 'STRING'
        else:
            return f'STRING({datatype.characters})'
    elif isinstance(datatype, Bytes):
        if datatype.bytes is None:
            return 'BYTES'
        else:
            return f'BYTES({datatype.bytes})'
    elif datatype == JSON:
        return 'JSON'
    elif datatype == DATE:
        return 'DATE'
    elif datatype == TIME:
        return 'TIME'
    elif datatype == DATE_TIME:
        return 'DATETIME'
    elif datatype == TIMESTAMP:
        return 'TIMESTAMP'
    elif isinstance(datatype, Range):
        return f'RANGE<{datatype.kind}>'
    elif datatype == INTERVAL:
        return 'INTERVAL'
    elif isinstance(datatype, Array):
        return f'ARRAY<{datatype_to_sql(datatype.inner)}>'
    elif isinstance(datatype, Struct):
        return f'STRUCT<{", ".join(f"{n} {datatype_to_sql(t)}" for n, t in datatype.fields.items())}>'
    else:
        raise ValueError(f'bigquery.datatype_to_sql unrecognized datatype - {datatype}')


def interval_literal_to_sql(interval: IntervalLiteral) -> str:
    return f'INTERVAL \'{interval_literal_to_str(interval)}\' YEAR TO SECOND'


def interval_literal_to_str(interval: IntervalLiteral) -> str:
    sign_part = '-' if interval.sign == '-' else ''
    year_month_part = f'{sign_part}{interval.year}-{interval.month}'
    day_part = f'{sign_part}{interval.day}'
    time_part = f'{sign_part}{interval.hour}:{interval.minute}:{interval.second}.{interval.microsecond:06d}'
    return f'{year_month_part} {day_part} {time_part}'


def column_to_sql(column: Column) -> str:
    column_schema = ''

    if column.default_expression:
        column_schema += f' DEFAULT {column.default_expression}'

    if not column.nullable:
        column_schema += ' NOT NULL'

    option_parts = []

    if column.description:
        option_parts.append(f'description = \'{column.description}\'')

    if column.rounding_mode:
        option_parts.append(f'rounding_mode = \'{column.rounding_mode}\'')

    if option_parts:
        column_schema += f' OPTIONS({", ".join(option_parts)})'

    return f'`{column.name}` {datatype_to_sql(column.datatype)}{column_schema}'


def partition_to_sql(relation: Table | MaterializedView) -> str:
    if relation.partitioning:
        partitioning = relation.partitioning
        column = partitioning.column

        if partitioning.kind == 'TIME':
            if column:
                if partitioning.column_datatype:
                    datatype = partitioning.column_datatype
                elif isinstance(relation, Table) and column in relation.column_map:
                    datatype = relation.column_map[column].datatype
                else:
                    raise ValueError(f'Unable to determine the datatype of partition column: {column}')

                if isinstance(datatype, Date):
                    return f'PARTITION BY `{column}`\n'
                elif isinstance(datatype, DateTime):
                    return f'PARTITION BY DATETIME_TRUNC(`{column}`, {partitioning.time_unit})\n'
                elif isinstance(datatype, Timestamp):
                    return f'PARTITION BY TIMESTAMP_TRUNC(`{column}`, {partitioning.time_unit})\n'
                else:
                    raise ValueError(f'Unsupported partitioning column data type: {datatype}')
            else:
                return f'PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, {partitioning.time_unit})\n'
        elif relation.partitioning.kind == 'INT':
            start = partitioning.int_start
            end = partitioning.int_end
            step = partitioning.int_step
            return f'PARTITION BY RANGE_BUCKET(`{column}`, GENERATE_ARRAY({start}, {end}, {step}))\n'
        else:
            raise ValueError(f'Unsupported partitioning type: {relation.partitioning.kind}')
    else:
        return ''


def cluster_to_sql(relation: Relation) -> str:
    if relation.clustering:
        clustering_columns = ', '.join(f'`{c}`' for c in relation.clustering)
        return f'CLUSTER BY {clustering_columns}\n'
    else:
        return ''


def view_column_to_sql(column: Column) -> str:
    if column.description:
        options_sql = f' OPTIONS(description = \'{column.description}\')'
    else:
        options_sql = ''

    return f'`{column.name}`{options_sql}'


def option_dict_to_sql(option: dict[str, str]) -> str:
    join_sql = ', '.join(f'(\'{k}\', \'{v}\')' for k, v in option.items())
    return f'[{join_sql}]'


def to_qualified_name(table: bq.Table | bq.TableReference | bq.TableListItem) -> QualifiedName:
    if isinstance(table, bq.Table | bq.TableReference):
        return QualifiedName(
            database=DatabaseName(table.project),
            schema_name=SchemaName(table.dataset_id),
            name=Identifier(table.table_id),
        )
    elif isinstance(table, bq.TableListItem):
        return QualifiedName(table.full_table_id.replace(':', '.'))
    else:
        raise ValueError(f'Invalid table type: {type(table)}')


def to_datatype(schema_field: bq.SchemaField) -> Datatype:
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
        return String(characters=schema_field.max_length)
    elif field_type == 'BYTES':
        return Bytes(bytes=schema_field.max_length)
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
        return Struct(fields={field.name: to_datatype_array(field) for field in schema_field.fields})
    else:
        raise ValueError(f'bigquery.to_datatype unrecognized field_type - {schema_field}')


def to_datatype_array(schema_field: bq.SchemaField) -> Datatype:
    if schema_field.mode == REPEATED:
        return Array(inner=to_datatype(schema_field))
    else:
        return to_datatype(schema_field)


def to_liti_rounding_mode(rounding_mode: str | bq.RoundingMode | None) -> RoundingMode | None:
    if rounding_mode is None:
        return None
    # bq.RoundingMode subclasses str so we must check it first
    elif isinstance(rounding_mode, bq.RoundingMode):
        return RoundingMode(rounding_mode.name)
    elif isinstance(rounding_mode, str):
        return RoundingMode(rounding_mode)
    else:
        raise ValueError(f'bigquery.to_liti_rounding_mode unrecognized rounding_mode - {rounding_mode}')


def to_column(schema_field: bq.SchemaField) -> Column:
    return Column(
        name=schema_field.name,
        datatype=to_datatype_array(schema_field),
        default_expression=schema_field.default_value_expression,
        nullable=schema_field.mode != REQUIRED,
        description=schema_field.description,
        rounding_mode=schema_field.rounding_mode and RoundingMode(schema_field.rounding_mode),
    )


def to_liti_foreign_key(foreign_key: bq.ForeignKey) -> ForeignKey:
    return ForeignKey(
        name=foreign_key.name,
        foreign_table_name=to_qualified_name(foreign_key.referenced_table),
        references=[
            ForeignReference(
                local_column_name=ref.referencing_column,
                foreign_column_name=ref.referenced_column,
            )
            for ref in foreign_key.column_references
        ],
        enforced=False,
    )


def to_liti_schema(dataset: bq.Dataset) -> Schema:
    if dataset.default_table_expiration_ms is not None:
        default_table_expiration = timedelta(milliseconds=dataset.default_table_expiration_ms)
    else:
        default_table_expiration = None

    if dataset.default_partition_expiration_ms is not None:
        default_partition_expiration = timedelta(milliseconds=dataset.default_partition_expiration_ms)
    else:
        default_partition_expiration = None

    if dataset.default_encryption_configuration is not None:
        default_kms_key_name = dataset.default_encryption_configuration.kms_key_name
    else:
        default_kms_key_name = None

    if dataset.max_time_travel_hours is not None:
        # the code comments say the value is an int, but a str is observed at runtime
        max_time_travel = timedelta(hours=int(dataset.max_time_travel_hours))
    else:
        max_time_travel = None

    return Schema(
        name=QualifiedName(database=dataset.project, schema_name=dataset.dataset_id),
        friendly_name=dataset.friendly_name,
        description=dataset.description,
        labels=dataset.labels or None,
        tags=dataset.resource_tags or None,
        location=dataset.location,
        default_table_expiration=default_table_expiration,
        default_partition_expiration=default_partition_expiration,
        default_rounding_mode=to_liti_rounding_mode(dataset.default_rounding_mode),
        default_kms_key_name=default_kms_key_name,
        is_case_sensitive=not dataset.is_case_insensitive,
        max_time_travel=max_time_travel,
        storage_billing=dataset.storage_billing_model,
        # TODO: figure out what to do about bq.Dataset missing fields
        # - failover_reservation
        # - is_primary_replica
        # - primary_replica
    )


def to_liti_partitioning(table: bq.Table) -> Partitioning | None:
    if table.time_partitioning is not None:
        time_partition = table.time_partitioning

        if time_partition.expiration_ms is not None:
            expiration = time_partition.expiration_ms / ONE_DAY_IN_MILLIS
        else:
            expiration = None

        return Partitioning(
            kind='TIME',
            column=time_partition.field,
            time_unit=time_partition.type_,
            expiration=expiration,
            require_filter=table.require_partition_filter or False,
        )
    elif table.range_partitioning is not None:
        range_: bq.PartitionRange = table.range_partitioning.range_

        return Partitioning(
            kind='INT',
            column=table.range_partitioning.field,
            int_start=range_.start,
            int_end=range_.end,
            int_step=range_.interval,
            require_filter=table.require_partition_filter or False,
        )
    else:
        return None

def to_liti_table(table: bq.Table) -> Table:
    primary_key = None
    foreign_keys = None
    big_lake = None

    if table.table_constraints is not None:
        if table.table_constraints.primary_key:
            primary_key = PrimaryKey(column_names=table.table_constraints.primary_key.columns)

        if table.table_constraints.foreign_keys:
            foreign_keys = [to_liti_foreign_key(fk) for fk in table.table_constraints.foreign_keys]

    if table.biglake_configuration is not None:
        big_lake = BigLake(
            connection_id=table.biglake_configuration.connection_id,
            storage_uri=table.biglake_configuration.storage_uri,
        )

    return Table(
        name=to_qualified_name(table),
        columns=[to_column(f) for f in table.schema],
        primary_key=primary_key,
        foreign_keys=foreign_keys,
        partitioning=to_liti_partitioning(table),
        clustering=table.clustering_fields or None,
        friendly_name=table.friendly_name,
        description=table.description,
        labels=table.labels or None,
        tags=table.resource_tags or None,
        expiration_timestamp=table.expires,
        big_lake=big_lake,
    )


def to_liti_view(table: bq.Table) -> View:
    select_sql = table.view_query

    return View(
        name=to_qualified_name(table),
        columns=[to_column(f) for f in table.schema],
        friendly_name=table.friendly_name,
        description=table.description,
        labels=table.labels or None,
        tags=table.resource_tags or None,
        expiration_timestamp=table.expires,
        select_sql=select_sql or None,
    )


def to_liti_materialized_view(table: bq.Table) -> MaterializedView:
    partitioning = None

    if table.time_partitioning is not None:
        time_partition = table.time_partitioning

        if time_partition.expiration_ms is not None:
            expiration = time_partition.expiration_ms / ONE_DAY_IN_MILLIS
        else:
            expiration = None

        partitioning = Partitioning(
            kind='TIME',
            column=time_partition.field,
            time_unit=time_partition.type_,
            expiration=expiration,
            require_filter=table.require_partition_filter or False,
        )
    elif table.range_partitioning is not None:
        range_: bq.PartitionRange = table.range_partitioning.range_

        partitioning = Partitioning(
            kind='INT',
            column=table.range_partitioning.field,
            int_start=range_.start,
            int_end=range_.end,
            int_step=range_.interval,
            require_filter=table.require_partition_filter or False,
        )

    select_sql = table.mview_query
    allow_non_incremental_definition = table.mview_allow_non_incremental_definition
    enable_refresh = table.mview_enable_refresh
    refresh_interval = table.mview_refresh_interval

    return MaterializedView(
        name=to_qualified_name(table),
        select_sql=select_sql or None,
        partitioning=partitioning,
        clustering=table.clustering_fields or None,
        allow_non_incremental_definition=allow_non_incremental_definition,
        enable_refresh=enable_refresh,
        refresh_interval=refresh_interval,
        friendly_name=table.friendly_name,
        description=table.description,
        labels=table.labels or None,
        tags=table.resource_tags or None,
        expiration_timestamp=table.expires,
    )


def can_coerce_int(to_dt: Any) -> bool:
    return isinstance(to_dt, Numeric | BigNumeric) or to_dt == FLOAT64


def can_coerce_numeric(from_dt: Numeric, to_dt: Any) -> bool:
    if isinstance(to_dt, Numeric):
        return (from_dt.precision, from_dt.scale) < (to_dt.precision, to_dt.scale)
    elif isinstance(to_dt, BigNumeric):
        return (from_dt.precision, from_dt.scale) <= (to_dt.precision, to_dt.scale)
    else:
        return to_dt == FLOAT64


def can_coerce_big_numeric(from_dt: BigNumeric, to_dt: Any) -> bool:
    if isinstance(to_dt, BigNumeric):
        return (from_dt.precision, from_dt.scale) < (to_dt.precision, to_dt.scale)
    else:
        return to_dt == FLOAT64


def can_coerce_string(from_dt: String, to_dt: Any) -> bool:
    if isinstance(to_dt, String):
        return from_dt.characters is not None and (
            to_dt.characters is None or from_dt.characters < to_dt.characters
        )
    else:
        return False


def can_coerce_bytes(from_dt: Bytes, to_dt: Any) -> bool:
    if isinstance(to_dt, Bytes):
        return from_dt.bytes is not None and (
            to_dt.bytes is None or from_dt.bytes < to_dt.bytes
        )
    else:
        return False


def can_coerce(from_dt: Any, to_dt: Any) -> bool:
    if from_dt == INT64:
        return can_coerce_int(to_dt)
    elif isinstance(from_dt, Numeric):
        return can_coerce_numeric(from_dt, to_dt)
    elif isinstance(from_dt, BigNumeric):
        return can_coerce_big_numeric(from_dt, to_dt)
    elif isinstance(from_dt, String):
        return can_coerce_string(from_dt, to_dt)
    elif isinstance(from_dt, Bytes):
        return can_coerce_bytes(from_dt, to_dt)
    else:
        return False


class BigQueryDbBackend(DbBackend):
    """ Big Query "client" that adapts terms between liti and google.cloud.bigquery """

    def __init__(self, client: BqClient, raise_unsupported: set[Unsupported]):
        self.client = client
        self.raise_unsupported = raise_unsupported

    # backend methods

    def scan_schema(self, database: DatabaseName, schema: SchemaName) -> list[Operation]:
        dataset = to_dataset_ref(database, schema)
        schema = self.get_schema(QualifiedName(database=database, schema_name=schema))
        relation_items = self.client.list_tables(dataset)
        relations = [self.get_relation(to_qualified_name(item)) for item in relation_items]

        if schema:
            create_schema = [CreateSchema(schema_object=schema)]
        else:
            create_schema = []

        tables = [
            CreateTable(table=t)
            for t in relations
            if isinstance(t, Table)
        ]

        views = [
            CreateView(view=v)
            for v in relations
            if isinstance(v, View)
        ]

        materialized_views = [
            CreateMaterializedView(materialized_view=m)
            for m in relations
            if isinstance(m, MaterializedView)
        ]

        return create_schema + tables + materialized_views + views

    def scan_relation(self, name: QualifiedName) -> CreateRelation | None:
        if self.has_table(name):
            relation = self.get_relation(name)

            if isinstance(relation, Table):
                return CreateTable(table=relation)
            elif isinstance(relation, View):
                return CreateView(view=relation)
            elif isinstance(relation, MaterializedView):
                return CreateMaterializedView(materialized_view=relation)
            else:
                raise ValueError(f'Unrecognized relation type: {relation}')
        else:
            return None

    def get_schema(self, name: QualifiedName) -> Schema | None:
        if name.is_schema():
            bq_dataset = self.client.get_dataset(extract_dataset_ref(name))
            return bq_dataset and to_liti_schema(bq_dataset)
        else:
            return None

    def create_schema(self, schema: Schema):
        options = []

        if schema.default_collate:
            collate_sql = f'DEFAULT COLLATE \'{schema.default_collate}\'\n'
        else:
            collate_sql = ''

        if schema.friendly_name:
            options.append(f'friendly_name = \'{schema.friendly_name}\'')

        if schema.description:
            options.append(f'description = \'{schema.description}\'')

        if schema.labels:
            options.append(f'labels = {option_dict_to_sql(schema.labels)}')

        if schema.tags:
            options.append(f'tags = {option_dict_to_sql(schema.tags)}')

        if schema.location:
            options.append(f'location = \'{schema.location}\'')

        if schema.default_table_expiration:
            table_expiration_days = schema.default_table_expiration.total_seconds() / ONE_DAY_IN_SECONDS
            options.append(f'default_table_expiration_days = {table_expiration_days}')

        if schema.default_partition_expiration:
            partition_expiration_days = schema.default_partition_expiration.total_seconds() / ONE_DAY_IN_SECONDS
            options.append(f'default_partition_expiration_days = {partition_expiration_days}')

        if schema.default_rounding_mode:
            options.append(f'default_rounding_mode = \'{schema.default_rounding_mode}\'')

        if schema.default_kms_key_name:
            options.append(f'default_kms_key_name = \'{schema.default_kms_key_name}\'')

        if schema.failover_reservation:
            options.append(f'failover_reservation = \'{schema.failover_reservation}\'')

        if schema.is_case_sensitive is False:
            options.append(f'is_case_insensitive = TRUE')

        if schema.is_primary_replica is not None:
            options.append(f'is_primary = {"TRUE" if schema.is_primary_replica else "FALSE"}')

        if schema.primary_replica:
            options.append(f'primary_replica = \'{schema.primary_replica}\'')

        if schema.max_time_travel:
            time_travel_hours = schema.max_time_travel.total_seconds() / ONE_HOUR_IN_SECONDS
            options.append(f'max_time_travel_hours = {int(time_travel_hours)}')

        if schema.storage_billing:
            options.append(f'storage_billing_model = \'{schema.storage_billing}\'')

        if options:
            joined_options = ',\n    '.join(options)

            options_sql = (
                f'OPTIONS(\n'
                f'    {joined_options}\n'
                f')\n'
            )
        else:
            options_sql = ''

        self.client.query_and_wait(
            f'CREATE SCHEMA `{schema.name}`\n'
            f'{collate_sql}'
            f'{options_sql}'
        )

    def drop_schema(self, name: QualifiedName):
        self.client.delete_dataset(extract_dataset_ref(name))

    def set_default_table_expiration(self, schema_name: QualifiedName, expiration: timedelta | None):
        self.set_schema_option(
            schema_name,
            'default_table_expiration_days',
            str(expiration.total_seconds() / ONE_DAY_IN_SECONDS) if expiration is not None else 'NULL',
        )

    def set_default_partition_expiration(self, schema_name: QualifiedName, expiration: timedelta | None):
        self.set_schema_option(
            schema_name,
            'default_partition_expiration_days',
            str(expiration.total_seconds() / ONE_DAY_IN_SECONDS) if expiration is not None else 'NULL',
        )

    def set_default_kms_key_name(self, schema_name: QualifiedName, key_name: str | None):
        self.set_schema_option(schema_name, 'default_kms_key_name', f'\'{key_name}\'' if key_name else 'NULL')

    def set_failover_reservation(self, schema_name: QualifiedName, reservation: str | None):
        self.set_schema_option(schema_name, 'failover_reservation', f'\'{reservation}\'' if reservation else 'NULL')

    def set_case_sensitive(self, schema_name: QualifiedName, case_sensitive: bool):
        self.set_schema_option(schema_name, 'is_case_insensitive', 'TRUE' if not case_sensitive else 'FALSE')

    def set_is_primary_replica(self, schema_name: QualifiedName, is_primary: bool):
        self.set_schema_option(schema_name, 'is_primary', 'TRUE' if is_primary else 'FALSE')

    def set_primary_replica(self, schema_name: QualifiedName, replica: str | None):
        self.set_schema_option(schema_name, 'primary_replica', f'\'{replica}\'' if replica else 'NULL')

    def set_max_time_travel(self, schema_name: QualifiedName, duration: timedelta | None):
        self.set_schema_option(
            schema_name,
            'max_time_travel_hours',
            str(int(duration.total_seconds() / ONE_HOUR_IN_SECONDS)) if duration is not None else 'NULL',
        )

    def set_storage_billing(self, schema_name: QualifiedName, storage_billing: StorageBilling):
        self.set_schema_option(schema_name, 'storage_billing_model', f'\'{storage_billing}\'')

    def get_table(self, name: QualifiedName) -> Table | None:
        table_ref = to_table_ref(name)

        if self.client.has_table(table_ref):
            return to_liti_table(self.client.get_table(table_ref))
        else:
            return None

    def create_table(self, table: Table):
        column_sqls = [column_to_sql(column) for column in table.columns]
        constraint_sqls = []
        options = []

        if table.default_collate:
            collate_sql = f'DEFAULT COLLATE \'{table.default_collate}\'\n'
        else:
            collate_sql = ''

        if table.primary_key:
            # TODO? update this if Big Query ever supports enforcement
            if table.primary_key.enforced:
                self.handle_unsupported(
                    Unsupported.ENFORCE_PRIMARY_KEY,
                    'Not enforcing primary key since Big Query does not support enforcement',
                )

            pk_columns_sql = ', '.join(f'`{col}`' for col in table.primary_key.column_names)
            constraint_sqls.append(f'PRIMARY KEY ({pk_columns_sql}) NOT ENFORCED')

        for foreign_key in (table.foreign_keys or []):
            # TODO? update this if Big Query ever supports enforcement
            if foreign_key.enforced:
                self.handle_unsupported(
                    Unsupported.ENFORCE_FOREIGN_KEY,
                    f'Not enforcing foreign key {foreign_key.name} since Big Query does not support enforcement',
                )

            local_column_sql = ', '.join(f'`{ref.local_column_name}`' for ref in foreign_key.references)
            foreign_column_sql = ', '.join(f'`{ref.foreign_column_name}`' for ref in foreign_key.references)

            constraint_sqls.append(
                f'CONSTRAINT `{foreign_key.name}`'
                f' FOREIGN KEY ({local_column_sql})'
                f' REFERENCES `{foreign_key.foreign_table_name}` ({foreign_column_sql})'
                f' NOT ENFORCED'
            )

        partition_sql = partition_to_sql(table)

        if table.partitioning:
            partitioning = table.partitioning

            if partitioning.kind == 'TIME' and partitioning.expiration is not None:
                options.append(f'partition_expiration_days = {partitioning.expiration.total_seconds() / ONE_DAY_IN_SECONDS}')

            if partitioning.require_filter:
                options.append(f'require_partition_filter = TRUE')

        cluster_sql = cluster_to_sql(table)

        if table.friendly_name:
            options.append(f'friendly_name = \'{table.friendly_name}\'')

        if table.description:
            options.append(f'description = \'{table.description}\'')

        if table.labels:
            options.append(f'labels = {option_dict_to_sql(table.labels)}')

        if table.tags:
            options.append(f'tags = {option_dict_to_sql(table.tags)}')

        if table.expiration_timestamp:
            utc_ts = table.expiration_timestamp.astimezone(timezone.utc)
            options.append(f'expiration_timestamp = TIMESTAMP \'{utc_ts.strftime("%Y-%m-%d %H:%M:%S UTC")}\'')

        if table.default_rounding_mode:
            options.append(f'default_rounding_mode = \'{table.default_rounding_mode}\'')

        if table.max_staleness:
            options.append(f'max_staleness = {interval_literal_to_sql(table.max_staleness)}')

        if table.enable_change_history:
            options.append(f'enable_change_history = TRUE')

        if table.enable_fine_grained_mutations:
            options.append(f'enable_fine_grained_mutations = TRUE')

        if table.kms_key_name:
            options.append(f'kms_key_name = \'{table.kms_key_name}\'')

        if table.big_lake:
            connection_sql = f'WITH CONNECTION `{table.big_lake.connection_id}`\n'
            options.append(f'storage_uri = \'{table.big_lake.storage_uri}\'')
            options.append(f'file_format = {table.big_lake.file_format}')
            options.append(f'table_format = {table.big_lake.table_format}')
        else:
            connection_sql = ''

        if options:
            joined_options = ',\n    '.join(options)

            options_sql = (
                f'OPTIONS(\n'
                f'    {joined_options}\n'
                f')\n'
            )
        else:
            options_sql = ''

        columns_and_constraints = ',\n    '.join(column_sqls + constraint_sqls)

        self.client.query_and_wait(
            f'CREATE TABLE `{table.name}` (\n'
            f'    {columns_and_constraints}\n'
            f')\n'
            f'{collate_sql}'
            f'{partition_sql}'
            f'{cluster_sql}'
            f'{connection_sql}'
            f'{options_sql}'
        )

    def drop_table(self, name: QualifiedName):
        self.client.delete_table(to_table_ref(name))

    def rename_table(self, from_name: QualifiedName, to_name: Identifier):
        self.client.query_and_wait(f'ALTER TABLE `{from_name}` RENAME TO `{to_name}`')

    def set_primary_key(self, table_name: QualifiedName, primary_key: PrimaryKey | None):
        if primary_key:
            if primary_key.enforced:
                self.handle_unsupported(
                    Unsupported.ENFORCE_PRIMARY_KEY,
                    'Not enforcing primary key since Big Query does not support enforcement',
                )

            column_sql = ', '.join(f'`{col}`' for col in primary_key.column_names)

            self.client.query_and_wait(
                f'ALTER TABLE `{table_name}`\n'
                f'ADD PRIMARY KEY ({column_sql}) NOT ENFORCED\n'
            )
        else:
            self.client.query_and_wait(
                f'ALTER TABLE `{table_name}`\n'
                f'DROP PRIMARY KEY\n'
            )

    def add_foreign_key(self, table_name: QualifiedName, foreign_key: ForeignKey):
        local_column_sql = ', '.join(f'`{ref.local_column_name}`' for ref in foreign_key.references)
        foreign_column_sql = ', '.join(f'`{ref.foreign_column_name}`' for ref in foreign_key.references)

        self.client.query_and_wait(
            f'ALTER TABLE `{table_name}`\n'
            f'ADD CONSTRAINT `{foreign_key.name}`'
            f' FOREIGN KEY ({local_column_sql})'
            f' REFERENCES `{foreign_key.foreign_table_name}` ({foreign_column_sql})'
            f' NOT ENFORCED\n'
        )

    def drop_constraint(self, table_name: QualifiedName, constraint_name: ConstraintName):
        self.client.query_and_wait(
            f'ALTER TABLE `{table_name}`\n'
            f'DROP CONSTRAINT `{constraint_name}`\n'
        )

    def set_partition_expiration(self, table_name: QualifiedName, expiration: timedelta | None):
        self.set_table_option(
            table_name,
            'partition_expiration_days',
            str(expiration.total_seconds() / ONE_DAY_IN_SECONDS) if expiration is not None else 'NULL',
        )

    def set_require_partition_filter(self, table_name: QualifiedName, require_filter: bool):
        self.set_table_option(table_name, 'require_partition_filter', 'TRUE' if require_filter else 'FALSE')

    def set_clustering(self, table_name: QualifiedName, column_names: list[ColumnName] | None):
        bq_table = self.client.get_table(to_table_ref(table_name))
        bq_table.clustering_fields = [col.string for col in column_names] if column_names else None
        self.client.update_table(bq_table, ['clustering_fields'])

    def set_friendly_name(self, entity_name: QualifiedName, friendly_name: str | None):
        self.set_entity_option(entity_name, 'friendly_name', f'\'{friendly_name}\'' if friendly_name else 'NULL')

    def set_description(self, entity_name: QualifiedName, description: str | None):
        self.set_entity_option(entity_name, 'description', f'\'{description}\'' if description else 'NULL')

    def set_labels(self, entity_name: QualifiedName, labels: dict[str, str] | None):
        self.set_entity_option(entity_name, 'labels', option_dict_to_sql(labels) if labels else 'NULL')

    def set_tags(self, entity_name: QualifiedName, tags: dict[str, str] | None):
        self.set_entity_option(entity_name, 'tags', option_dict_to_sql(tags) if tags else 'NULL')

    def set_expiration_timestamp(self, entity_name: QualifiedName, expiration_timestamp: datetime | None):
        if expiration_timestamp:
            utc_ts = expiration_timestamp.astimezone(timezone.utc)
            formatted = f'TIMESTAMP \'{utc_ts.strftime("%Y-%m-%d %H:%M:%S UTC")}\''
        else:
            formatted = 'NULL'

        self.set_entity_option(entity_name, 'expiration_timestamp', formatted)

    def set_default_rounding_mode(self, entity_name: QualifiedName, rounding_mode: RoundingMode | None):
        self.set_entity_option(entity_name, 'default_rounding_mode', f'\'{rounding_mode}\'' if rounding_mode else 'NULL')

    def set_max_staleness(self, entity_name: QualifiedName, max_staleness: IntervalLiteral | None):
        self.set_entity_option(
            entity_name,
            'max_staleness',
            interval_literal_to_sql(max_staleness) if max_staleness else 'NULL',
        )

    def set_enable_change_history(self, table_name: QualifiedName, enabled: bool):
        self.set_table_option(table_name, 'enable_change_history', 'TRUE' if enabled else 'FALSE')

    def set_enable_fine_grained_mutations(self, table_name: QualifiedName, enabled: bool):
        self.set_table_option(table_name, 'enable_fine_grained_mutations', 'TRUE' if enabled else 'FALSE')

    def set_kms_key_name(self, table_name: QualifiedName, key_name: str | None):
        self.set_table_option(table_name, 'kms_key_name', f'\'{key_name}\'' if key_name else 'NULL')

    def add_column(self, table_name: QualifiedName, column: Column):
        self.client.query_and_wait(
            f'ALTER TABLE `{table_name}`\n'
            f'ADD COLUMN {column_to_sql(column)}\n'
        )

    def drop_column(self, table_name: QualifiedName, column_name: ColumnName):
        self.client.query_and_wait(
            f'ALTER TABLE `{table_name}`\n'
            f'DROP COLUMN `{column_name}`\n'
        )

    def rename_column(self, table_name: QualifiedName, from_name: ColumnName, to_name: ColumnName):
        self.client.query_and_wait(
            f'ALTER TABLE `{table_name}`\n'
            f'RENAME COLUMN `{from_name}` TO `{to_name}`\n'
        )

    def set_column_datatype(self, table_name: QualifiedName, column_name: ColumnName, from_datatype: Datatype, to_datatype: Datatype):
        if can_coerce(from_datatype, to_datatype):
            self.client.query_and_wait(
                f'ALTER TABLE `{table_name}`\n'
                f'ALTER COLUMN `{column_name}`\n'
                f'SET DATA TYPE {datatype_to_sql(to_datatype)}\n'
            )
        else:
            self.handle_unsupported(
                Unsupported.SET_COLUMN_DATATYPE,
                f'Not updating column {column_name} from {from_datatype} to {to_datatype} since Big Query does not support it',
            )

    def add_column_field(self, table_name: QualifiedName, field_path: FieldPath, datatype: Datatype):
        table = super().add_column_field(table_name, field_path, datatype)
        self.client.update_table(to_bq_table(table), ['schema'])

    def drop_column_field(self, table_name: QualifiedName, field_path: FieldPath):
        self.handle_unsupported(
            Unsupported.DROP_COLUMN_FIELD,
            f'Not dropping field at {field_path} since Big Query does not support it',
        )

    def set_column_nullable(self, table_name: QualifiedName, column_name: ColumnName, nullable: bool):
        if nullable:
            self.client.query_and_wait(
                f'ALTER TABLE `{table_name}`\n'
                f'ALTER COLUMN `{column_name}`\n'
                f'DROP NOT NULL\n'
            )
        else:
            self.handle_unsupported(
                Unsupported.ADD_NON_NULLABLE_COLUMN,
                f'Not adding column {column_name} as non-nullable since Big Query does not support it',
            )

    def set_column_description(self, table_name: QualifiedName, column_name: ColumnName, description: str | None):
        self.set_column_option(table_name, column_name, 'description', f'\'{description}\'' if description else 'NULL')

    def set_column_rounding_mode(
        self,
        table_name: QualifiedName,
        column_name: ColumnName,
        rounding_mode: RoundingMode | None,
    ):
        self.set_column_option(
            table_name,
            column_name,
            'rounding_mode', f'\'{rounding_mode}\'' if rounding_mode else 'NULL',
        )

    def get_view(self, name: QualifiedName) -> View | None:
        table_ref = to_table_ref(name)

        if self.client.has_view(table_ref):
            return to_liti_view(self.client.get_table(table_ref))
        else:
            return None

    def create_view(self, view: View):
        column_sqls = [view_column_to_sql(column) for column in view.columns or []]
        options = []

        if view.friendly_name:
            options.append(f'friendly_name = \'{view.friendly_name}\'')

        if view.description:
            options.append(f'description = \'{view.description}\'')

        if view.labels:
            options.append(f'labels = {option_dict_to_sql(view.labels)}')

        if view.tags:
            options.append(f'tags = {option_dict_to_sql(view.tags)}')

        if view.expiration_timestamp:
            utc_ts = view.expiration_timestamp.astimezone(timezone.utc)
            options.append(f'expiration_timestamp = TIMESTAMP \'{utc_ts.strftime("%Y-%m-%d %H:%M:%S UTC")}\'')

        if view.privacy_policy:
            options.append(f'privacy_policy = \'{json.dumps(view.privacy_policy)}\'')

        if options:
            joined_options = ',\n    '.join(options)

            options_sql = (
                f'OPTIONS(\n'
                f'    {joined_options}\n'
                f')\n'
            )
        else:
            options_sql = ''

        columns_sql = ',\n    '.join(column_sqls)

        if columns_sql:
            columns_sql = (
                f' (\n'
                f'    {columns_sql}\n'
                f')'
            )

        self.client.query_and_wait(
            f'CREATE OR REPLACE VIEW `{view.name}`{columns_sql}\n'
            f'{options_sql}'
            f'AS\n'
            f'{view.formatted_select_sql}\n'
        )

    def drop_view(self, name: QualifiedName):
        self.drop_table(name)

    def get_materialized_view(self, name: QualifiedName) -> MaterializedView | None:
        table_ref = to_table_ref(name)

        if self.client.has_materialized_view(table_ref):
            return to_liti_materialized_view(self.client.get_table(table_ref))
        else:
            return None

    def create_materialized_view(self, materialized_view: MaterializedView):
        options = []
        partition_sql = partition_to_sql(materialized_view)
        cluster_sql = cluster_to_sql(materialized_view)

        if materialized_view.friendly_name:
            options.append(f'friendly_name = \'{materialized_view.friendly_name}\'')

        if materialized_view.description:
            options.append(f'description = \'{materialized_view.description}\'')

        if materialized_view.labels:
            options.append(f'labels = {option_dict_to_sql(materialized_view.labels)}')

        if materialized_view.tags:
            options.append(f'tags = {option_dict_to_sql(materialized_view.tags)}')

        if materialized_view.expiration_timestamp:
            utc_ts = materialized_view.expiration_timestamp.astimezone(timezone.utc)
            options.append(f'expiration_timestamp = TIMESTAMP \'{utc_ts.strftime("%Y-%m-%d %H:%M:%S UTC")}\'')

        if materialized_view.allow_non_incremental_definition:
            options.append(f'allow_non_incremental_definition = {"TRUE" if materialized_view.allow_non_incremental_definition else "FALSE"}')

        if materialized_view.max_staleness:
            options.append(f'max_staleness = {interval_literal_to_sql(materialized_view.max_staleness)}')

        if materialized_view.enable_refresh:
            options.append(f'enable_refresh = {"TRUE" if materialized_view.enable_refresh else "FALSE"}')

        if materialized_view.refresh_interval is not None:
            options.append(f'refresh_interval_minutes = {materialized_view.refresh_interval.total_seconds() / ONE_MINUTE_IN_SECONDS}')

        if options:
            joined_options = ',\n    '.join(options)

            options_sql = (
                f'OPTIONS(\n'
                f'    {joined_options}\n'
                f')\n'
            )
        else:
            options_sql = ''

        self.client.query_and_wait(
            f'CREATE OR REPLACE MATERIALIZED VIEW `{materialized_view.name}`\n'
            f'{partition_sql}'
            f'{cluster_sql}'
            f'{options_sql}'
            f'AS\n'
            f'{materialized_view.formatted_select_sql}\n'
        )

    def drop_materialized_view(self, name: QualifiedName):
        self.drop_table(name)

    def execute_sql(self, sql: str):
        self.client.query_and_wait(sql)

    def execute_bool_value_query(self, sql: str) -> bool:
        row_iter = self.client.query_and_wait(sql)

        if row_iter.total_rows != 1:
            raise ValueError(f'Expected a bool value query, got {row_iter.total_rows} rows')

        row = next(iter(row_iter))

        if len(row) != 1:
            raise ValueError(f'Expected a bool value query, got a row with {len(row)} columns')

        value = row[0]

        if not isinstance(value, bool):
            raise ValueError(f'Expected a bool value query, got a value of type {type(value)}')

        return value

    # default methods

    def int_defaults(self, node: Int, context: Context):
        node.bits = node.bits or 64

    def float_defaults(self, node: Float, context: Context):
        node.bits = node.bits or 64

    def numeric_defaults(self, node: Numeric, context: Context):
        node.precision = node.precision or 38
        node.scale = node.scale or 9

    def big_numeric_defaults(self, node: BigNumeric, context: Context):
        node.precision = node.precision or 76
        node.scale = node.scale or 38

    def partitioning_defaults(self, node: Partitioning, context: Context):
        if node.kind == 'TIME':
            node.time_unit = node.time_unit or 'DAY'
        elif node.kind == 'INT':
            node.int_step = node.int_step or 1

    def table_defaults(self, node: Table, context: Context):
        if node.expiration_timestamp is not None and node.expiration_timestamp.tzinfo is None:
            node.expiration_timestamp = node.expiration_timestamp.astimezone(timezone.utc)

        if node.enable_change_history is None:
            node.enable_change_history = False

        if node.enable_fine_grained_mutations is None:
            node.enable_fine_grained_mutations = False

    def view_defaults(self, node: View, context: Context):
        super().view_defaults(node, context)

        if node.expiration_timestamp is not None and node.expiration_timestamp.tzinfo is None:
            node.expiration_timestamp = node.expiration_timestamp.astimezone(timezone.utc)

    def materialized_view_defaults(self, node: MaterializedView, context: Context):
        super().materialized_view_defaults(node, context)

        if node.expiration_timestamp is not None and node.expiration_timestamp.tzinfo is None:
            node.expiration_timestamp = node.expiration_timestamp.astimezone(timezone.utc)

        if node.allow_non_incremental_definition is None:
            node.allow_non_incremental_definition = False

        if node.enable_refresh is None:
            node.enable_refresh = True

        if node.refresh_interval is None:
            node.refresh_interval = timedelta(minutes=30)

    # validation methods

    def validate_schema(self, node: Schema, context: Context):
        VALID_HOURS = {48, 72, 96, 120, 144, 168}

        if node.name is None:
            raise ValueError('Schema requires a name')

        if node.name.database is None or len(node.name.database.string) == 0:
            raise ValueError('Schema requires a database')

        if node.name.schema_name is None or len(node.name.schema_name.string) == 0:
            raise ValueError('Schema requires a schema')

        if node.name.name is not None:
            raise ValueError('Schema must not have a name, only database and schema')

        if node.max_time_travel is not None:
            float_hours = node.max_time_travel.total_seconds() / ONE_HOUR_IN_SECONDS
            int_hours = int(float_hours)

            if float_hours != int_hours or int_hours not in VALID_HOURS:
                raise ValueError(f'max_time_travel must be one of {VALID_HOURS} hours: {node.max_time_travel}')

    def validate_int(self, node: Int, context: Context):
        if node.bits != 64:
            raise ValueError(f'Int.bits must be 64: {node.bits}')

    def validate_float(self, node: Float, context: Context):
        if node.bits != 64:
            raise ValueError(f'Float.bits must be 64: {node.bits}')

    def validate_numeric(self, node: Numeric, context: Context):
        if not (0 <= node.scale <= 9):
            raise ValueError(f'Numeric.scale must be between 0 and 9: {node.scale}')

        if not (max(1, node.scale) <= node.precision <= node.scale + 29):
            raise ValueError(
                f'Numeric.precision must be between {max(1, node.scale)} and {node.scale + 29}: {node.precision}')

    def validate_big_numeric(self, node: BigNumeric, context: Context):
        if not (0 <= node.scale <= 38):
            raise ValueError(f'Scale must be between 0 and 38: {node.scale}')

        if not (max(1, node.scale) <= node.precision <= node.scale + 38):
            raise ValueError(f'Precision must be between {max(1, node.scale)} and {node.scale + 38}: {node.precision}')

    def validate_string(self, node: String, context: Context):
        if node.characters is not None and node.characters <= 0:
            raise ValueError(f'String.characters must be positive: {node.characters}')

    def validate_bytes(self, node: Bytes, context: Context):
        if node.bytes is not None and node.bytes <= 0:
            raise ValueError(f'Bytes.bytes must be positive: {node.bytes}')

    def validate_array(self, node: Array, context: Context):
        if isinstance(node.inner, Array):
            raise ValueError('Nested arrays are not allowed')

    def validate_partitioning(self, node: Partitioning, context: Context):
        if node.kind == 'TIME':
            required = ['kind', 'time_unit', 'require_filter']
            allowed = required + ['column', 'column_datatype', 'expiration']
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

    def handle_unsupported(self, unsupported: Unsupported, message: str):
        if unsupported in self.raise_unsupported:
            raise UnsupportedError(message)
        else:
            log.warning(message)

    def set_entity_option(self, entity_name: QualifiedName, key: str, value: str):
        entity = self.get_entity(entity_name)

        if isinstance(entity, Schema):
            self.set_schema_option(entity_name, key, value)
        elif isinstance(entity, Table):
            self.set_table_option(entity_name, key, value)
        elif isinstance(entity, View):
            self.set_view_option(entity_name, key, value)
        elif isinstance(entity, MaterializedView):
            self.set_materialized_view_option(entity_name, key, value)
        else:
            raise ValueError(f'Unrecognized entity type: {entity}')

    def set_schema_option(self, schema_name: QualifiedName, key: str, value: str):
        self.client.query_and_wait(
            f'ALTER SCHEMA `{schema_name}`\n'
            f'SET OPTIONS({key} = {value})\n'
        )

    def set_table_option(self, table_name: QualifiedName, key: str, value: str):
        self.client.query_and_wait(
            f'ALTER TABLE `{table_name}`\n'
            f'SET OPTIONS({key} = {value})\n'
        )

    def set_column_option(self, table_name: QualifiedName, column_name: ColumnName, key: str, value: str):
        self.client.query_and_wait(
            f'ALTER TABLE `{table_name}`\n'
            f'ALTER COLUMN `{column_name}`\n'
            f'SET OPTIONS({key} = {value})\n'
        )

    def set_view_option(self, view_name: QualifiedName, key: str, value: str):
        self.client.query_and_wait(
            f'ALTER VIEW `{view_name}`\n'
            f'SET OPTIONS({key} = {value})\n'
        )

    def set_materialized_view_option(self, materialized_view_name: QualifiedName, key: str, value: str):
        self.client.query_and_wait(
            f'ALTER MATERIALIZED VIEW `{materialized_view_name}`\n'
            f'SET OPTIONS({key} = {value})\n'
        )


class BigQueryMetaBackend(MetaBackend):
    def __init__(self, client: BqClient, table_name: QualifiedName):
        self.client = client
        self.table_name = table_name

    def initialize(self):
        self.client.query_and_wait(
            f'CREATE SCHEMA IF NOT EXISTS `{self.table_name.database}.{self.table_name.schema_name}`;\n'
            f'\n'
            f'CREATE TABLE IF NOT EXISTS `{self.table_name}` (\n'
            f'    idx INT64 NOT NULL,\n'
            f'    op_kind STRING NOT NULL,\n'
            f'    op_data JSON NOT NULL,\n'
            f'    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL\n'
            f')\n'
        )

    def get_applied_operations(self) -> list[Operation]:
        if self.client.has_table(to_table_ref(self.table_name)):
            rows = self.client.query_and_wait(f'SELECT op_kind, op_data FROM `{self.table_name}` ORDER BY idx')
            return [parse_operation(row.op_kind, json.loads(row.op_data)) for row in rows]
        else:
            return []

    def apply_operation(self, operation: Operation):
        results = self.client.query_and_wait(
            f'INSERT INTO `{self.table_name}` (idx, op_kind, op_data)\n'
            f'VALUES (\n'
            f'    (SELECT COALESCE(MAX(idx) + 1, 0) FROM `{self.table_name}`),\n'
            f'    @op_kind,\n'
            f'    @op_data\n'
            f')\n',
            job_config=bq.QueryJobConfig(
                query_parameters=[
                    bq.ScalarQueryParameter('op_kind', 'STRING', operation.KIND),
                    bq.ScalarQueryParameter('op_data', 'JSON', operation.model_dump_json(exclude_none=True)),
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
            job_config=bq.QueryJobConfig(
                query_parameters=[
                    bq.ScalarQueryParameter('op_kind', 'STRING', operation.KIND),
                    bq.ScalarQueryParameter('op_data', 'JSON', operation.model_dump_json(exclude_none=True)),
                ]
            )
        )

        assert results.num_dml_affected_rows == 1, f'Expected exactly 1 row deleted: {results.num_dml_affected_rows}'
