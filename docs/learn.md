# Concepts

## Data

Everything in Limber Timber is written using data. This includes:

- The manifest
- Operations
- Templates

Both JSON and YAML are supported. They are designated by file extension.

## Operation

The core of Limber Timber is the operation. An operation is the unit of migration, whereas in other migration tools it
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

## Template

The Limber Timber templates are written as data. They do not work like most templates where you have unfilled sections
and fill in the blanks. Instead, they search for values to replace based on a specification, and update the values to
the template's replacement value.

Let's look at the fields of a template in detail.

> Note: templates still have improvements to be made for complex use cases. The basic usages will likely have a stable
> interface, but more complex usages have a good chance of receiving backwards incompatible changes.

> Tip: you can use templates not just to replace values, but also to set unspecified values. For example, you can set
> the database and schema everywhere with templates, and then only write the name component for all of your entities.

Example template:

```yaml
operation_kinds:
- create_table
root_type: QualifiedName
path: schema
value: staging_schema
full_match:
  table:
    partitioning:
      column:
        name: some_date
local_match:
  database: auth_db
```

#### operation_kinds

This is a list of the kinds of operations this template should consider. If none are provided, all operations are
considered. In the example, this template will only replace values inside `create_table` operations.

This field is optional.

#### root_type

The root type is a Limber Timber model class name. It affects where the `path` searches from. The template will search
for all instances of that class in the operation, and replace values within those instances. If no root type is
provided, the operation is used as the root. In the example, this template will use each `QualifiedName` as a root to
replace the value.

This field is optional.

#### path

This is a path from each selected root to the value that needs to be replaced. A path is `"."` delimited strings.
Starting with each root, the first string in the path is a field name to look for the field to replace the value. The
template will search nested structures in this way to find that location. When the path encounters a `tuple`, `list`, or
`set`, all values within that collection will be explored. So a template that encounters a `list` will replace a field
nested within each list item. When a path encounters a `dict`, the next string in the path is instead used to look up a
key within that dict. Currently only `dict[str, Any]` is supported in this way. In the example, the value being replaced
is the `schema` field within each `QualifiedName`

> Note: `schema` is a sub-class of `ValidatedString` which stores its string value in a field named `string`. However,
> we do not use `schema.string` for the path as a convenience due to special handling. This applies to all
> `ValidatedString`s in paths and matches.

This field is required.

#### value

This is the value to replace at each root + path location. In the example, this template is updating the schema
component of the table names to `staging_schema`.

This field is required.

#### full_match

Matches (both full and local) act as filters on which values to replace. They are nested object data structures where
the keys are strings. They check for matches using a similar process to paths by traversing the fields of nested Limber
Timber model types. Only values that have a positive match will be replaced. Matches can match any child value from its
respective root including parents and siblings of the value being replaced. Full matches use the operation as the root.
In the example, only `create_table` operations that create tables partitioned on a column named `"some_date"` will have
a value replaced.

This field is optional.

#### local_match

Local matches work very similarly to full matches, except they match from each selected root as per `root_type`. In the
example, only tables in the `"auth_db"` database will have a value replaced.

This field is optional.

## Template File

These are data files containing a list of templates. Templates are applied in the order they are listed. Multiple
template files can be specified in the CLI, and they are applied in the order they are listed there as well. When
multiple templates replace the same value, the last one applied wins. In the CLI, template files are relative paths
from the current directory.

Example template file:

```yaml
# note the -'s which make this file a list instead of an object

# within all `QualifiedName`s, replace the database value with "replacement_database"
- root_type: QualifiedName
  path: database
  value: replacement_database
# within all `QualifiedName`s, replace the schema value with "replacement_schema"
- root_type: QualifiedName
  path: schema
  value: replacement_schema
```
