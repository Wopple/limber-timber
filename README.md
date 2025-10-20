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

I am writing the migration system I always wanted but did not exist, until now. Read the docs to learn more!

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
- ✅ JSON Schema
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

If you want to contribute, read the docs and check out the roadmap. I will only accept contributions if:

1. I agree with the design decisions
2. The code style matches the existing code
3. Unit tests are included (if needed)

If you have any questions, you can reach out to me on [discord](https://discord.gg/b4jGYACJJy).

### Design Principles

- The default behavior is safe and automated
- The behavior can be configured to be fast and efficient
- High flexibility to support future and unknown use-cases
- Prefer supporting narrow use cases well rather than broad use cases poorly
- Apply heavy importance to the Single Responsibility Principle
- Put complex logic in easily testable functions

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
