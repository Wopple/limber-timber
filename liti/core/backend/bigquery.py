from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter, SchemaField, Table as BqTable

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.client.bigquery import BqClient
from liti.core.function import parse_operation
from liti.core.model.v1.data_type import Array, BOOL, DataType, FLOAT64, INT64, STRING, Struct
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import Column, Table, TableName

REQUIRED = 'REQUIRED'
NULLABLE = 'NULLABLE'
REPEATED = 'REPEATED'


def to_field_type(column: Column) -> str:
    data_type = column.data_type

    if isinstance(data_type, Struct):
        return 'RECORD'
    elif data_type == BOOL:
        return 'BOOLEAN'
    elif data_type == INT64:
        return 'INT64'
    elif data_type == FLOAT64:
        return 'FLOAT64'
    elif data_type == STRING:
        return 'STRING'
    else:
        raise ValueError(f'bigquery.field_type unrecognized data_type - {column}')


def to_fields(column: Column) -> tuple[SchemaField, ...]:
    data_type = column.data_type

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


def to_schema_field(column: Column) -> SchemaField:
    return SchemaField(
        name=column.name,
        field_type=to_field_type(column),
        mode=to_mode(column),
        fields=to_fields(column),
    )


def to_data_type(schema_field: SchemaField) -> DataType:
    field_type = schema_field.field_type

    if field_type == 'RECORD':
        return Struct(fields={f.name: to_data_type_array(f) for f in schema_field.fields})
    elif field_type == 'BOOLEAN':
        return BOOL
    elif field_type == 'INT64':
        return INT64
    elif field_type == 'FLOAT64':
        return FLOAT64
    elif field_type == 'STRING':
        return STRING
    else:
        raise ValueError(f'bigquery.data_type unrecognized field_type - {schema_field}')


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


class BigQueryDbBackend(DbBackend):
    def __init__(self, client: BqClient):
        self.client = client

    def get_table(self, name: TableName) -> Table:
        bq_table = self.client.get_table(name.str)
        columns = [to_column(f) for f in bq_table.schema]
        return Table(name=name, columns=columns)

    def create_table(self, table: Table):
        bq_table = BqTable(table.name.str, [to_schema_field(c) for c in table.columns])
        self.client.create_table(bq_table)

    def drop_table(self, name: TableName):
        self.client.delete_table(name.str)


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
                applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
            )
            '''
        )

    def get_applied_operations(self) -> list[Operation]:
        rows = self.client.query_and_wait(f'SELECT op_kind, op_data FROM `{self.table_name}` ORDER BY idx')

        return [parse_operation(row.op_kind, row.op_data) for row in rows]

    def apply_operation(self, operation: Operation):
        self.client.query_and_wait(
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

    def unapply_operation(self, operation: Operation) -> bool:
        results = self.client.query_and_wait(
            f'''
            DELETE FROM `{self.table_name}`
             WHERE idx = (SELECT MAX(idx) FROM `{self.table_name}`)
               AND op_kind = @op_kind

               -- ensure normalized comparison, cannot compare JSON types
               AND TO_JSON_STRING(op_data) = TO_JSON_STRING(PARSE_JSON(@op_data))
            ''',
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter('op_kind', 'STRING', operation.KIND),
                    ScalarQueryParameter('op_data', 'JSON', operation.model_dump_json()),
                ]
            )
        )

        return results.num_dml_affected_rows == 1
