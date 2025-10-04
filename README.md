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
- ➡️ Grouped Operation Application
    - To reduce round trips with the backend and reduce migration time
- ➡️ Expand Grouped Operations
  - To handle complex operations that do not have atomic support in the backend
- ✅ Minimize Scan Output
- ✅ Arbitrary DML SQL Migrations
- ➡️ File System Metadata
- ➡️ SQLite Database
- ➡️ SQLite Metadata
- ➡️ Postgres Database
- ➡️ Postgres Metadata
- ➡️ MySQL Database
- ➡️ MySQL Metadata
- ➡️ Optional Backend Installation
    - To minimize dependency bloat

# Contribution

If you want to contribute, the roadmap is a good place to start. I will only accept contributions if:

1. I agree with the design decisions
2. The code style matches the existing code

It is highly recommended but not necessary to:

3. Include unit tests

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
    - state that which is explained by the nearby code
    - cover up for poor code
    - just because
- Multiline strings use concatenated single line strings
  - exceptions
    - docstrings
- No `from my.module import *`
  - instead: `from my import module as md`

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
