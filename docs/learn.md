# Concepts

## Data

Everything in Limber Timber is written using data. This includes:

- The manifest
- Operations
- Templates

Both JSON and YAML are supported. They are designated by file extension.

## Operation

The core of Limber Timber is the operation. An operation is the unit of migration, whereas in other migrations tools it
is a file. Operations include `create_table`, `add_column`, and so on. Each operation has its own set of fields which
parametrize the operation. The metadata table stores the operations that have been applied.

Example operation:

```yaml
# Creates a table named `user_resource_access` in the `auth` schema of the
# `recordly` database. It associates users with the resources they have access to.
kind: create_table
data:
  table:
    name:
      database: recordly
      schema: auth
      name: user_resource_access
    columns:
    - name: user_id
      datatype:
        type: INT
        bits: 64
    - name: resource_id
      # shorthand for an int with 64 bits, equivalent to user_id's datatype
      datatype: INT64
```

## Operation File

These are data files containing a list of operations. Operations are applied in the order they are listed.

Example operation file:

```yaml
# note the -'s which make this file a list instead of an object
- kind: create_table
  data:
    table:
      # shorthand for fully qualifying a table name
      name: recordly.auth.user_resource_access
      columns:
      - name: user_id
        datatype: INT64
      - name: resource_id
        datatype: INT64
- kind: add_column
  data:
    table_name: recordly.auth.user_resource_access
    column:
      name: access_level
      datatype: STRING
      # default is NOT NULL since it is easier to relax requirements
      nullable: true
```

## Manifest

A manifest lists all your operation files. Operation files are applied in the order they are listed. Manifests must be
named one of:

- `manifest.json`
- `manifest.yaml`
- `manifest.yml`

Example manifest file:

```yaml
# This is the version of the model in case it becomes useful in the future.
version: 1
operation_files:
# model/ and auth/ are directory names for organization,
# they have no special meaning
- model/create_users.yaml
- model/create_resources.yaml
- auth/create_user_resource_access.yaml
```

## Target Directory

The target directory contains your manifest file. Operation filenames in the manifest are relative from the target
directory. The target directory can be specified programmatically or from the CLI.

Example contents within a target directory:
```
manifest.yaml
auth/
  create_user_resource_access.yaml
model/
  create_resources.yaml
  create_users.yaml
```

Note the sort order of the operation filenames is irrelevant.
