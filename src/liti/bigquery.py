# A collection of re-imports to allow for convenient namespacing of bigquery types like so:
# from liti import bigquery as bq

# noinspection PyUnresolvedReferences
from google.cloud.bigquery import (
    Client,
    ConnectionProperty,
    Dataset,
    DatasetReference,
    PartitionRange,
    QueryJob,
    QueryJobConfig,
    RangePartitioning,
    ScalarQueryParameter,
    SchemaField,
    Table,
    TableReference,
)

# noinspection PyUnresolvedReferences
from google.cloud.bigquery.enums import RoundingMode
# noinspection PyUnresolvedReferences
from google.cloud.bigquery.schema import _DEFAULT_VALUE as SCHEMA_DEFAULT_VALUE

# noinspection PyUnresolvedReferences
from google.cloud.bigquery.table import (
    BigLakeConfiguration,
    ColumnReference,
    ForeignKey,
    PrimaryKey,
    RowIterator,
    TableConstraints,
    TableListItem,
    TimePartitioning,
)
