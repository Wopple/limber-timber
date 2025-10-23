Below are a collection of short step-by-step guides to perform tasks with Limber Timber.

# Hello World

This is a minimal example of creating a Big Query table using the CLI.

- the runner creates a Big Query dataset for storing the metadata named `my_project.my_migrations`
- the runner creates a metadata table named `my_project.my_migrations.hello_world`
- runs a dry run to show the migrations that will be applied
- the migrations create a Big Query dataset named `my_project.hello_world`
- the migrations create a table named `my_project.hello_world.my_table`

---

1) Create a target directory.

```shell
mkdir migrations
```

2) Create the manifest.

```yaml
# ./migrations/manifest.yaml
version: 1
operation_files:
- ops/hello_world.yaml
```

3) Create the operation file.

```yaml
# ./migrations/ops/hello_world.yaml
version: 1
operations:
- kind: create_schema
  data:
    schema_object:
      name:
        project: my_project
        schema_name: hello_world
- kind: create_table
  data:
    table:
      name: my_project.hello_world.my_table
      columns:
      - name: my_column
        datatype: BOOL
```

4) Run a dry run.

```shell
liti migrate \
    -t migrations \
    --db bigquery \
    --meta bigquery \
    --meta-table-name my_project.my_migrations.hello_world
```

5) Run a wet run.

```shell
liti migrate -w \
    -t migrations \
    --db bigquery \
    --meta bigquery \
    --meta-table-name my_project.my_migrations.hello_world
```

Dry runs are the default and wet runs require an explicit flag as a safety precaution.

# Roll Back

Imagine you are iterating on your database design while in development. You want to work with this development cycle:

- apply migrations
- test things out
- if it is no good, roll back migrations
- repeat

Here's what that flow looks like:

1) Update the migrations to add a column.

```yaml
# ./migrations/manifest.yaml
version: 1
operation_files:
- ops/create_schema.yaml
- ops/create_table.yaml
- ops/add_column.yaml # new line
```

```yaml
# ./migrations/ops/add_column.yaml
version: 1
operations:
- kind: add_column
  data:
    table_name: my_project.my_app.my_table
    column:
      name: my_new_column
      datatype: INT64
```

2) Apply the updated migrations.

```shell
liti migrate -w \
    -t migrations \
    --db bigquery \
    --meta bigquery \
    --meta-table-name my_project.my_migrations.my_app
```

Now assume you have done some testing and decided against using that new column.

3) Revert the updated migration files.

```yaml
# ./migrations/manifest.yaml
version: 1
operation_files:
- ops/create_schema.yaml
- ops/create_table.yaml
```

```yaml
# ./migrations/ops/add_column.yaml
# [file deleted]
```

4) Run a wet run.

```shell
liti migrate -w \
    -t migrations \
    --db bigquery \
    --meta bigquery \
    --meta-table-name my_project.my_migrations.my_app
```

Oops, that failed.

5) Run a wet run with down migrations enabled.

```shell
liti migrate -wd \
    -t migrations \
    --db bigquery \
    --meta bigquery \
    --meta-table-name my_project.my_migrations.my_app
```

Down migrations are disabled by default and require an explicit flag as a safety precaution.

# Adopt a Database

Imagine you are learning about Limber Timber and are liking what you see. However, you have an existing migration system
with a lot of incremental migrations. Adopting Limber Timber and replacing your existing migrations is a major burden
which tips the balance against being worth the effort.

This scenario is made easier with Limber Timber thanks to:

- schema scanning
- running migrations in memory

Here's how to have Limber Timber adopt an existing database.

1) Scan the schema you want to adopt.

```shell
liti scan \
    --db bigquery \
    --scan-database my_project \
    --scan-schema my_app \
    > migrations/ops/adopt.yaml
```

This will create an operation file that generates the same schema.

However, scanning is not perfect:

- views may be created before their dependencies if they depend on other views
- while foreign keys are respected, if you have cyclical foreign keys it will fail
- the generated file can be verbose
- it will scan a migrations table if you have any in the schema
- scanning will not create templates

Therefore...

2) Make manual adjustments to the generated operations.

3) Create your manifest.

```yaml
# ./migrations/manifest.yaml
version: 1
operation_files:
- ops/adopt.yaml
```

4) Run a wet run partially in memory.

Running a wet run will write to both the database schema and the meta table. Either can be configured to run in memory
to essentially "do nothing." So we are going to run the schema migrations in memory and write to the meta table for real
so the only persistent change is the creation of the meta table.

```shell
liti migrate -w \
    -t migrations \
    --db memory \
    --meta bigquery \
    --meta-table-name my_project.my_migrations.my_app
```

Now when you run Limber Timber it will assume those migrations have already run. If you run the migrations in a fresh
environment it will perform the schema migrations you expect.

Adoption complete!

# Multiple Environments

You have a set of migrations to manage a single schema and the resources within. You want to apply these migrations in
different environments for different purposes like production and development. This can be done with templates.

1) Create a production template.

```yaml
# ./migrations/tpl/production.yaml

# set the production database value for all QualifiedNames
- root_type: QualifiedName
  path: database
  value: high_slots

# set the production schema value for all QualifiedNames
- root_type: QualifiedName
  path: schema_name
  value: my_app_production

# set the partition expiration to 1 year for all time partitioned tables
- root_type: Partitioning
  path: expiration
  value: P1Y
  local_match:
    kind: TIME
```

> Note: Limber Timber uses Pydantic for modeling, and Pydantic uses
> [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) for timedeltas.

2) Create a development template.

```yaml
# ./migrations/tpl/development.yaml

# set the development database value for all QualifiedNames
- root_type: QualifiedName
  path: database
  value: low_slots

# set the development schema value for all QualifiedNames
- root_type: QualifiedName
  path: schema_name
  value: my_app_development

# set the partition expiration to 7 days for all time partitioned tables
- root_type: Partitioning
  path: expiration
  value: P7D
  local_match:
    kind: TIME
```

3) Write your other migrations.

```yaml
# ./migrations/ops/init.yaml
version: 1
operations:
- kind: create_schema
  data:
    schema_object:
      name:
        schema_name: placeholder # can omit the database
- kind: create_table
  data:
    table:
      name: my_table # can omit the database and schema
      columns:
      - name: my_column
        datatype: BOOL
      - name: col_date
        datatype: DATE
      partitioning:
        kind: TIME
        column: col_date
        time_unit: DAY
        # can omit the expiration
- kind: add_column
  data:
    table_name: my_table # can omit the database and schema
    column:
    - name: my_new_column
      datatype: INT64
```

4) Select the environment when running the migrations.

```shell
liti migrate \
    -t migrations \
    --db bigquery \
    --meta bigquery \
    --meta-table-name high_slots.my_migrations.my_app_production \
    --tpl migrations/tpl/production.yaml
```

> Tip: You can use multiple templates. This means you could have more dimensions beyond environment represented by
> different sets of templates and select 1 template for each dimension. This is done with multiple `--tpl` options.

# Unsupported Operations

Limber Timber adopts the philosophy of supporting narrow use cases well over supporting broad use cases poorly. This
means there will be missing features. While common use cases are prioritized for development, there will always be
projects that require some unsupported features. If you find yourself in this situation, you have two options: add
support for the features, or use the `ExecuteSql` operator.

Conceptually, the `ExecuteSql` operator is very simple, it executes arbitrary SQL. However, there are caveats:

- little support for templating
- down migrations must be implemented by you
- checking for application must be implemented by you

> Note: Some schema changes cannot be performed with SQL queries like updating the clustering columns in Big Query.
> While that use case is supported with the `SetClustering` operation, other such API-only features would not be
> supported by `ExecuteSql`.

1) Write the operation.

```yaml
# ./migrations/ops/create_add_function.yaml
version: 1
operations:
- kind: execute_sql
  data:
    up: "sql/create_add_function.sql"
    down: "sql/drop_add_function.sql"

    # false values here ensure the operation is always run, SQL files run a
    # boolean value query where TRUE means the operation is already applied
    is_up: false
    is_down: "sql/is_add_function_dropped.sql"

    # entity_names is able to participate in templating
    entity_names:
      schema:
        database: my_project
        schema_name: my_functions
```

2) Write the referenced SQL files.

```sql
-- ./migrations/sql/create_add_function.yaml

-- OR REPLACE is needed since this file is always being applied
CREATE OR REPLACE FUNCTION `{schema}.add`(a INT64, b INT64, default_value INT64) RETURNS INT64 AS (
    COALESCE(a + b, default_value)
)
```

```sql
-- ./migrations/sql/drop_add_function.yaml
DROP FUNCTION `{schema}.add`
```

```sql
-- ./migrations/sql/is_add_function_dropped.yaml
SELECT COUNT(*) = 0
FROM `{schema}.INFORMATION_SCHEMA.ROUTINES`
WHERE
    routine_name = 'add'
    AND routine_type = 'FUNCTION'
```
