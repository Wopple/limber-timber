![status](https://img.shields.io/pypi/status/limber-timber)
[![PyPI version](https://img.shields.io/pypi/v/limber-timber)](https://pypi.org/project/limber-timber/)
![Python](https://img.shields.io/pypi/pyversions/limber-timber)
[![Tests](https://github.com/Wopple/limber-timber/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/Wopple/limber-timber/actions/workflows/unit-tests.yml)
![Last Commit](https://img.shields.io/github/last-commit/Wopple/limber-timber)
[![License](https://img.shields.io/github/license/Wopple/limber-timber)](LICENSE)

# Introduction

Limber Timber is a database migration tool written in python with flexibility in mind. It is the database migration tool
I always wanted but never existed (until now).

Currently only Big Query is supported as that is what I'm using at work. The codebase is written to support different
backends, and I plan to implement them eventually. Pull requests are welcome.

### Migrations Written in Data

Data is trivial to parse and thus easy to manipulate. You describe the migrations in data, and Limber Timber does the
rest.

### Inferred Down Migrations

Other migration tools that support down migrations put the burden on the developer to implement them. Limber Timber
automatically infers the down migrations from the up migrations removing that burden from you.

### Separation of Database and Metadata

Limber Timber is not prescriptive about where you store your migrations table. You can even store it in a completely
different type of database! This gives you the flexibility you need to meet the task at hand.

### In-memory Support

There are many reasons you may want to run migrations in-memory.

- Unit tests
- Dry runs
- Only produce the migrations table
- Something else!

### Manifest

A manifest file lists the migrations to apply and the order to apply them in.

- No special naming conventions like timestamps in your migration files
- Produces git merge conflicts instead of breaking silently
- Allows for better organization with directories

### Error Recovery

Some backends (looking at you Big Query) do not support transactions for DDL queries. Limber Timber does its best to
fail in recoverable ways despite this limitation. In most cases, no manual intervention will be needed in the case of a
failure.

### Templates

In Limber Timber, templates are first class. This means you do not need a preprocessing step in your deployments to fill
out parameters. You write your templates in data and provide those template files to the migration runner.

### Scanning

I want people to use Limber Timber, so I need to make it easy for you to start using it. Scanning is a feature that
queries a schema or table and produces the migrations that would generate the same. Then you can run the migrations
in-memory to generate the metadata and voilà, Limber Timber is ready to go.

### JSON Schema

(Coming Soon)

JSON Schema allows for integrated validation and code completion.
