# Limber Timber

***It's data based!***

---

![status](https://img.shields.io/pypi/status/limber-timber)
[![PyPI version](https://img.shields.io/pypi/v/limber-timber)](https://pypi.org/project/limber-timber/)
![Python](https://img.shields.io/pypi/pyversions/limber-timber)
[![Tests](https://github.com/Wopple/limber-timber/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/Wopple/limber-timber/actions/workflows/unit-tests.yml)
![Last Commit](https://img.shields.io/github/last-commit/Wopple/limber-timber)
[![License](https://img.shields.io/github/license/Wopple/limber-timber)](LICENSE)

```shell
pip install limber-timber
```

# Overview

I am writing the migration system I always wanted but does not exist (yet).

# Docs

https://Wopple.github.io/limber-timber

# Roadmap

These are listed in rough priority order.

- ✅ CLI
- ✅ Publish to PyPI
- ✅ Templating
- ➡️ Documentation
- ➡️ Unit Tests
  - ➡️ Templating
- ➡️ JSON Schema
    - To validate and auto complete migration files in IDEs
- ✅ In-memory Database
- ✅ In-memory Metadata
- ➡️ Big Query Database
  - ➡️ Create Snapshot Table
  - ➡️ Create Table Clone
- ✅ Big Query Metadata
- ✅ Database Adoption
- ✅ Raise Unsupported Operations
- ✅ Scan Topologically with Foreign Keys
- ✅ Database Specific Validation
- ➡️ Github Actions
    - ➡️ Release
- ➡️ Expand Grouped Operations
  - To handle complex operations that do not have atomic support in the backend
- ➡️ Grouped Operation Application
  - To reduce round trips with the backend and reduce migration time
- ✅ Minimize Scan Output
- ✅ Arbitrary DML SQL Migrations
- ➡️ Optional Backend Installation
  - To minimize dependency bloat
- ➡️ File System Metadata
- ➡️ SQLite Database
- ➡️ SQLite Metadata
- ➡️ Postgres Database
- ➡️ Postgres Metadata
- ➡️ MySQL Database
- ➡️ MySQL Metadata

# Usage

### Create Migrations

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
- path/to/enrich_user_name.yaml
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
        datatype: INT64
      - name: name
        datatype: STRING
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
      datatype: STRING
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

### Scan Database

You can also scan a schema / table which will print out the operations file that generates that schema / table.

```shell
# scan a schema
poetry run liti scan \
    --db bigquery \
    --scan-database your_project \
    --scan-schema your_dataset
```

```shell
# scan a table
poetry run liti scan \
    --db bigquery \
    --scan-database your_project \
    --scan-schema your_dataset \
    --scan-table your_table
```

# Learn

Being completely new to this project, you will have no idea where to start. Here. This is where you start. This is a
crash course on what Limber Timber is and how its put together.

### The Big Picture

Limber Timber uses the `Operation` to describe changes to a database. These operations are pure data. They can be
serialized to JSON or YAML, and can be deserialized from the same. Developers write JSON or YAML files to describe the
migrations for their application.

The `Operation` can be enhanced to become an `OperationOps`. This type brings behavior to the data. It allows you to:
- check if the operation has been applied to the database
  - useful for recovery from a failure between applying an operation and writing it to the metadata
- apply the operation, i.e. the "up" migration
- produce the inverse `Operation` that will perform the "down" migration

Down migrations are inferred from the up migrations, so developers only ever have to write the up migrations.

### Migration Files

Migration files start with a manifest file. The manifest points to the operation files in the order they should be
applied. Each operation file contains a list of operations in the order they should be applied. In this way,
```
# file1
[op1, op2]

# file2
[op3]
```
is exactly the same as:
```
# file1
[op1]

# file2
[op2, op3]
```

The migrational unit is the `Operation`, not the file. Grouping operations into files can help for organization, but
having a single file with all operations or many files each with one operation are both valid. There are no checksums
and no need to specially name your files. You can also organize your migrations with sub-directories, just specify the
paths in the manifest.

One major benefit to this system is if parallel developers add operations, one will merge first, and then the other will
get a merge conflict. This is much better than having migrations applied out of order (or breaking) after the fact. You
learn right away about the conflict, and the developer is prompted to resolve it. This benefit assumes all developers
are using the same style for adding new migrations: either adding a new file to the manifest, or adding a new operation
to the most recent file.

### Python Modules

`liti.core.model`

This module stores all the data models. The models are versioned, though currently there is only the one version. The
hierarchy is roughly:

> `operation.ops` > `operation.data` > `schema` > `datatype`

`liti.core.model.v1.operation.data`

These are the pure data operations. They are (de)serialized between the operation files and metadata.

`liti.core.model.v1.operation.ops`

These are the wrappers that enhance operations with behavior. There is a 1:1 relationship.

`liti.core.model.v1.datatype`

These are descriptions of column types.

`liti.core.model.v1.schema`

These are descriptions of tables and related constructs.

`liti.core.backend`

Both the database and the metadata can support different backends. You can even use different backends together. The
backends deal in both the `liti` model and backend specific types adapting between the two.

`liti.core.client`

These are clients used by the backends. They solely deal in backend specific types with no dependencies on the `liti`
model.

`liti.core.base`

This module has base classes for applying default values and validating the data. They are implemented using the
Observer / Observable pattern so different backends can define their own behavior. Also implements the templating
engine.

`liti.core.runner`

This module is for the runners associated with the various ways `liti` can be run. Main code will instantiate a runner
and run it.

# Contribution

If you want to contribute, the roadmap is a good place to start. I will only accept contributions if:

1. I agree with the design decisions
2. The code style matches the existing code

It is highly recommended but not necessary to:

1. Include unit tests

If you have any questions, you can reach out to me on [discord](https://discord.gg/b4jGYACJJy).

### Design Principles

- The default behavior is safe and automated
- The behavior can be configured to be fast and efficient
- High flexibility to support future and unknown use-cases
- Prefer supporting narrow use cases well rather than broad use cases poorly
- Apply heavy importance to the Single Responsibility Principle
- Put complex logic in easily tested functions

### Code Style

- 4-space indentation
- Prefer single quotes
  - exceptions
    - `pyproject.toml`
    - docstrings
    - nested f-strings
- Use newlines to visually separate blocks and conceptual groups of code
- Include explicit `else` blocks
  - exceptions
    - assertive if-statements
- Naming
  - balance brevity and clarity: say exactly what is needed
  - do not restate what is already clear from the context
- Comments
  - dos
    - clarify confusing code
    - explain the 'why'
    - first try to explain with the code instead of a comment
  - do nots
    - make assumptions about the reader
    - state that which is explained by the surrounding code
    - cover up for poor code
    - just because
- Multiline strings use concatenated single line strings
  - exceptions
    - docstrings
- No `from my.module import *`
  - instead: `from my import module as md`
