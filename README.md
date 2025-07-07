Limber Timber
=============

***Database Migrations Made Easy***

## Overview

This project is not ready for production use, consider it v0.0.0.

I am writing the migration system I always wanted but does not exist (yet).

## Notable Feature Goals

- Migrations specified in data, not SQL
- Down migrations automatically inferred from up migrations
  - Yes, down migrations for drop table and drop column are automatically inferred
- Separation of database and metadata
- In-memory support
- Database adoption
- No checksums
- Manifest instead of numbered or timestamped migration filenames
- JSON schema for migration files
- Error recovery for backends that do not support DDL transactions

## Not Goals

- ORM
- Parsing SQL
- Specifying all kinds of migrations in pure data (e.g. DML migrations will use SQL)
- Preserving lost data

## Rationale

- It is cumbersome to iterate on migrations without robust down migrations
- Automatically inferred down migrations reduces developer burden
- Writing migrations in data is cleaner and not specific to a database
- Never parsing SQL reduces the complexity of the codebase
- A lightweight open source library makes it each to add missing features
- JSON schema allows IDEs to be configured for migration file validation and auto-completion
- Separation of database and metadata allows for more flexible metadata storage options
- In-memory database and metadata allow for application unit testing
- No checksums allows for modifying migration files without breaking the migrations
- Manifest files cause git merge conflicts when parallel development has collisions

## Roadmap

These are listed in rough priority order if you are interested in contributing.

- ✅ CLI
- ➡️ Publish to PyPI
- ➡️ Github Actions
  - ➡️ Unit Tests
  - ➡️ Release
- ✅ In-memory Database
- ✅ In-memory Metadata
- ➡️ Big Query Database
  - ➡️ Create Table
    - ➡️ Columns
      - ✅ BOOL
      - ✅ INT64
      - ✅ FLOAT64
      - ✅ STRING
      - ✅ DATE
      - ✅ DATETIME
      - ✅ TIME
      - ✅ TIMESTAMP
      - ➡️ GEOGRAPHY
      - ✅ INTERVAL
      - ✅ JSON
      - ✅ NUMERIC
      - ✅ BIGNUMERIC
      - ✅ RANGE
      - ✅ ARRAY
      - ✅ STRUCT
      - ✅ PRIMARY KEY
      - ✅ FOREIGN KEY
    - ✅ Partitioning
    - ✅ Clustering
  - ✅ Drop Table
  - ✅ Rename Table
  - ✅ Set Table Partition Expiration
  - ✅ Set Table Clustering
  - ✅ Add Column
  - ✅ Drop Column
  - ✅ Rename Column
  - ➡️ Alter Column
  - ➡️ Create View
  - ➡️ Create Materialized View
  - ➡️ Create Snapshot Table
  - ➡️ Create Table Clone
- ✅ Big Query Metadata
- ➡️ Database Adoption
- ➡️ JSON Schema
- ➡️ Database Specific Validation
- ➡️ Arbitrary DML SQL Migrations
- ➡️ File System Metadata
- ➡️ SQLite Database
- ➡️ SQLite Metadata
- ➡️ Postgres Database
- ➡️ Postgres Metadata
- ➡️ MySQL Database
- ➡️ MySQL Metadata

## Usage

1. Create your target manifest

> Note: All migration files can use any of these extensions:
> - `.json`
> - `.yaml`
> - `.yml`

Create a target directory with a manifest file named `manifest.yaml`.

```yaml
# target_dir/manifest.yaml
version: 1
operation_files:
- path/to/create_user_table.yaml
- path/to/enrich_user_name.json
```

2. Create your target migration operations

> Tip: Using a subdirectory for the operations files makes it easy to configure your IDE to apply the correct JSON schema.

Create the files listed in your manifest.

```yaml
# target_dir/path/to/create_user_table.yaml
version: 1
operations:
- kind: create_table
  data:
    table:
      name:
        database: your_project
        schema_name: your_dataset
        table_name: users
      columns:
      - name: id
        data_type: INT64
      - name: name
        data_type: STRING
```

```yaml
# target_dir/path/to/enrich_user_name.yaml
version: 1
operations:
- kind: rename_column
  data:
    table_name:
      database: your_project
      schema_name: your_dataset
      table_name: users
    from_name: name
    to_name: firstname
- kind: add_column
  data:
    table_name:
      database: your_project
      schema_name: your_dataset
      table_name: users
    column:
      name: lastname
      data_type: STRING
```

3. Check what migrations will run

```shell
poetry run liti migrate \
    -t target_dir \
    --db bigquery \
    --meta bigquery \
    --meta-table-name your_project.your_dataset._migrations
```

4. Run the migrations

```shell
poetry run liti migrate -w \
    -t target_dir \
    --db bigquery \
    --meta bigquery \
    --meta-table-name your_project.your_dataset._migrations
```
