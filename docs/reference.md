# Limber Timber Datatypes

Below are all the datatypes that make up operation data structures.

## Operation Types

The `KIND` class constant is the string value to use in the operation file to specify that operation.

::: liti.core.model.v1.operation.data.table.CreateSchema
    options:
      members:
        - KIND
        - schema_object

::: liti.core.model.v1.operation.data.table.DropSchema
    options:
      members:
        - KIND
        - schema_name

::: liti.core.model.v1.operation.data.table.SetDefaultTableExpiration
    options:
      members:
        - KIND
        - schema_name
        - expiration

::: liti.core.model.v1.operation.data.table.SetDefaultPartitionExpiration
    options:
      members:
        - KIND
        - schema_name
        - expiration

::: liti.core.model.v1.operation.data.table.SetDefaultKmsKeyName
    options:
      members:
        - KIND
        - schema_name
        - key_name

::: liti.core.model.v1.operation.data.table.SetFailoverReservation
    options:
      members:
        - KIND
        - schema_name
        - reservation

::: liti.core.model.v1.operation.data.table.SetCaseSensitive
    options:
      members:
        - KIND
        - schema_name
        - case_sensitive

::: liti.core.model.v1.operation.data.table.SetIsPrimaryReplica
    options:
      members:
        - KIND
        - schema_name
        - is_primary

::: liti.core.model.v1.operation.data.table.SetPrimaryReplica
    options:
      members:
        - KIND
        - schema_name
        - replica

::: liti.core.model.v1.operation.data.table.SetMaxTimeTravel
    options:
      members:
        - KIND
        - schema_name
        - duration

::: liti.core.model.v1.operation.data.table.SetStorageBilling
    options:
      members:
        - KIND
        - schema_name
        - storage_billing

::: liti.core.model.v1.operation.data.table.CreateTable
    options:
      members:
        - KIND
        - table

::: liti.core.model.v1.operation.data.table.DropTable
    options:
      members:
        - KIND
        - table_name

::: liti.core.model.v1.operation.data.table.RenameTable
    options:
      members:
        - KIND
        - from_name
        - to_name

::: liti.core.model.v1.operation.data.table.SetPrimaryKey
    options:
      members:
        - KIND
        - table_name
        - primary_key

::: liti.core.model.v1.operation.data.table.AddForeignKey
    options:
      members:
        - KIND
        - table_name
        - foreign_key

::: liti.core.model.v1.operation.data.table.DropConstraint
    options:
      members:
        - KIND
        - table_name
        - constraint_name

::: liti.core.model.v1.operation.data.table.SetPartitionExpiration
    options:
      members:
        - KIND
        - table_name
        - expiration

::: liti.core.model.v1.operation.data.table.SetRequirePartitionFilter
    options:
      members:
        - KIND
        - table_name
        - require_filter

::: liti.core.model.v1.operation.data.table.SetClustering
    options:
      members:
        - KIND
        - table_name
        - column_names

::: liti.core.model.v1.operation.data.table.SetFriendlyName
    options:
      members:
        - KIND
        - entity_name
        - friendly_name

::: liti.core.model.v1.operation.data.table.SetDescription
    options:
      members:
        - KIND
        - entity_name
        - description

::: liti.core.model.v1.operation.data.table.SetLabels
    options:
      members:
        - KIND
        - entity_name
        - labels

::: liti.core.model.v1.operation.data.table.SetTags
    options:
      members:
        - KIND
        - entity_name
        - tags

::: liti.core.model.v1.operation.data.table.SetExpirationTimestamp
    options:
      members:
        - KIND
        - entity_name
        - expiration_timestamp

::: liti.core.model.v1.operation.data.table.SetDefaultRoundingMode
    options:
      members:
        - KIND
        - entity_name
        - rounding_mode

::: liti.core.model.v1.operation.data.table.SetMaxStaleness
    options:
      members:
        - KIND
        - entity_name
        - max_staleness

::: liti.core.model.v1.operation.data.table.SetEnableChangeHistory
    options:
      members:
        - KIND
        - table_name
        - enabled

::: liti.core.model.v1.operation.data.table.SetEnableFineGrainedMutations
    options:
      members:
        - KIND
        - table_name
        - enabled

::: liti.core.model.v1.operation.data.table.SetKmsKeyName
    options:
      members:
        - KIND
        - table_name
        - key_name

::: liti.core.model.v1.operation.data.view.CreateView
    options:
      members:
        - KIND
        - view

::: liti.core.model.v1.operation.data.view.DropView
    options:
      members:
        - KIND
        - view_name

::: liti.core.model.v1.operation.data.view.CreateMaterializedView
    options:
      members:
        - KIND
        - materialized_view

::: liti.core.model.v1.operation.data.view.DropMaterializedView
    options:
      members:
        - KIND
        - materialized_view_name

::: liti.core.model.v1.operation.data.column.AddColumn
    options:
      members:
        - KIND
        - table_name
        - column

::: liti.core.model.v1.operation.data.column.DropColumn
    options:
      members:
        - KIND
        - table_name
        - column_name

::: liti.core.model.v1.operation.data.column.RenameColumn
    options:
      members:
        - KIND
        - table_name
        - from_name
        - to_name

::: liti.core.model.v1.operation.data.column.SetColumnDatatype
    options:
      members:
        - KIND
        - table_name
        - column_name
        - datatype

::: liti.core.model.v1.operation.data.column.AddColumnField
    options:
      members:
        - KIND
        - table_name
        - field_path
        - datatype

::: liti.core.model.v1.operation.data.column.DropColumnField
    options:
      members:
        - KIND
        - table_name
        - field_path

::: liti.core.model.v1.operation.data.column.SetColumnNullable
    options:
      members:
        - KIND
        - table_name
        - field_path
        - table_name

::: liti.core.model.v1.operation.data.column.SetColumnDescription
    options:
      members:
        - KIND
        - column_name
        - nullable
        - table_name

::: liti.core.model.v1.operation.data.column.SetColumnRoundingMode
    options:
      members:
        - KIND
        - column_name
        - description
        - table_name

## Schema Types

::: liti.core.model.v1.schema.IntervalLiteral
    options:
      members:
        - year
        - month
        - day
        - hour
        - minute
        - second
        - microsecond
        - sign

::: liti.core.model.v1.schema.RoundingMode
    options:
      members:
        - string

::: liti.core.model.v1.schema.ValidatedString
    options:
      members:
        - string

::: liti.core.model.v1.schema.DatabaseName
    options:
      members: []

::: liti.core.model.v1.schema.Identifier
    options:
      members: []

::: liti.core.model.v1.schema.FieldPath
    options:
      members: []

::: liti.core.model.v1.schema.SchemaName
    options:
      members: []

::: liti.core.model.v1.schema.ColumnName
    options:
      members: []

::: liti.core.model.v1.schema.QualifiedName
    options:
      members:
        - database
        - schema_name
        - name

::: liti.core.model.v1.schema.PrimaryKey

::: liti.core.model.v1.schema.ForeignReference

::: liti.core.model.v1.schema.ForeignKey
    options:
      members:
        - name
        - foreign_table_name
        - references
        - enforced

::: liti.core.model.v1.schema.Column
    options:
      members:
        - name
        - datatype
        - default_expression
        - nullable
        - description
        - rounding_mode

::: liti.core.model.v1.schema.Partitioning
    options:
      members:
        - kind
        - column
        - column_datatype
        - time_unit
        - int_start
        - int_end
        - int_step
        - expiration
        - require_filter

::: liti.core.model.v1.schema.BigLake

::: liti.core.model.v1.schema.Entity

::: liti.core.model.v1.schema.Schema

::: liti.core.model.v1.schema.Relation

::: liti.core.model.v1.schema.Table
    options:
      members:
        - columns
        - default_collate
        - primary_key
        - foreign_keys
        - partitioning
        - clustering
        - default_rounding_mode
        - max_staleness
        - enable_change_history
        - enable_fine_grained_mutations
        - kms_key_name
        - big_lake

::: liti.core.model.v1.schema.ViewLike
    options:
      members:
        - select_sql
        - select_file
        - entity_names

::: liti.core.model.v1.schema.View
    options:
      members:
        - columns
        - privacy_policy

::: liti.core.model.v1.schema.MaterializedView
    options:
      members:
        - partitioning
        - clustering
        - allow_non_incremental_definition
        - enable_refresh
        - refresh_interval

## Datatypes

::: liti.core.model.v1.datatype.Datatype
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Bool
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Int
    options:
      members:
        - type
        - bits


::: liti.core.model.v1.datatype.Float
    options:
      members:
        - type
        - bits

::: liti.core.model.v1.datatype.Geography
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Numeric
    options:
      members:
        - type
        - precision
        - scale

::: liti.core.model.v1.datatype.BigNumeric
    options:
      members:
        - type
        - precision
        - scale

::: liti.core.model.v1.datatype.String
    options:
      members:
        - type
        - characters

::: liti.core.model.v1.datatype.Bytes
    options:
      members:
        - type
        - bytes

::: liti.core.model.v1.datatype.Json
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Date
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Time
    options:
      members:
        - type

::: liti.core.model.v1.datatype.DateTime
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Timestamp
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Range
    options:
      members:
        - type
        - kind

::: liti.core.model.v1.datatype.Interval
    options:
      members:
        - type

::: liti.core.model.v1.datatype.Array
    options:
      members:
        - type
        - inner

::: liti.core.model.v1.datatype.Struct
    options:
      members:
        - type
        - fields
