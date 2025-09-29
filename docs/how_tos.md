Below are a collection of short step-by-step guides to perform tasks with Limber Timber.

# Hello World

This is a minimal example of creating a table.

- the runner creates a Big Query dataset for storing the metadata named `my_project.my_migrations`
- the runner creates a metadata table named `my_project.my_migrations.hello_world`
- runs a dry run to show the migrations that will be applied
- the migrations create a Big Query dataset named `my_project.my_dataset`
- the migrations create a table named `my_project.my_dataset.my_table`

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
- sql/hello_world.yaml
```

3) Create the operation file.

```yaml
# ./migrations/sql/hello_world.yaml
version: 1
operations:
- kind: create_schema
  data:
    schema_object:
      name:
        project: my_project
        schema_name: my_dataset
- kind: create_table
  data:
    table:
      name: my_project.my_dataset.my_table
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
