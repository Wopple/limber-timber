import logging
from argparse import ArgumentParser, BooleanOptionalAction, Namespace
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from liti import bigquery as bq
from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.backend.bigquery import BigQueryDbBackend, BigQueryMetaBackend
from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.client.bigquery import BqClient
from liti.core.context import Context
from liti.core.model.v1.schema import DatabaseName, Identifier, QualifiedName, SchemaName
from liti.core.runner import MigrateRunner, ScanRunner


class Clients(BaseModel):
    """ Used to ensure we have a single instance of any client that is needed

    This becomes useful for example when it comes to having transactions that span the DB and metadata.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    big_query: BqClient | None = None


def parse_all_arguments() -> Namespace:
    parser = ArgumentParser(prog='liti')
    parser.add_argument('command', help='action to perform')
    parser.add_argument('-t', '--target', help='directory with migration files')
    parser.add_argument('--tpl', action='append', metavar=('template',), help='[repeatable] filename containing operation templates')
    parser.add_argument('-w', '--wet', action=BooleanOptionalAction, default=False, help='should perform migration side effects')
    parser.add_argument('-d', '--down', action=BooleanOptionalAction, default=False, help='should allow performing down migrations')
    parser.add_argument('-v', '--verbose', action=BooleanOptionalAction, default=False, help='should log in a wet run')
    parser.add_argument('--db', default='memory', help='type of database backend (e.g. memory, bigquery) (default: memory)')
    parser.add_argument('--meta', default='memory', help='type of metadata backend (e.g. memory, bigquery) (default: memory)')
    parser.add_argument('--meta-table-name', help='fully qualified table name for a metadata table')
    parser.add_argument('--scan-database', help='database to scan')
    parser.add_argument('--scan-schema', help='schema to scan')
    parser.add_argument('--scan-table', help='table to scan')
    parser.add_argument('--gcp-project', help='project to use for GCP backends')
    return parser.parse_args()


def parse_migrate_arguments() -> Namespace:
    parser = ArgumentParser(prog='liti')
    parser.add_argument('command', help='action to perform')
    parser.add_argument('-t', '--target', required=True, help='directory with migration files')
    parser.add_argument('--tpl', action='append', metavar=('template',), help='[repeatable] filename containing operation templates')
    parser.add_argument('-w', '--wet', action=BooleanOptionalAction, default=False, help='should perform migration side effects')
    parser.add_argument('-d', '--down', action=BooleanOptionalAction, default=False, help='should allow performing down migrations')
    parser.add_argument('-v', '--verbose', action=BooleanOptionalAction, default=False, help='should log in a wet run')
    parser.add_argument('--db', default='memory', help='type of database backend (e.g. memory, bigquery) (default: memory)')
    parser.add_argument('--meta', default='memory', help='type of metadata backend (e.g. memory, bigquery) (default: memory)')
    parser.add_argument('--meta-table-name', help='fully qualified table name for a metadata table')
    parser.add_argument('--gcp-project', help='project to use for GCP backends')
    return parser.parse_args()


def parse_scan_arguments() -> Namespace:
    parser = ArgumentParser(prog='liti')
    parser.add_argument('command', help='action to perform')
    parser.add_argument('--db', required=True, help='type of database backend (e.g. bigquery)')
    parser.add_argument('--scan-database', required=True, help='database to scan')
    parser.add_argument('--scan-schema', required=True, help='schema to scan')
    parser.add_argument('--scan-table', help='table to scan, scan whole schema if not provided')
    parser.add_argument('--gcp-project', help='project to use for GCP backends')
    return parser.parse_args()


def build_clients(args: Namespace) -> Clients:
    client_ids = []

    if 'db' in args:
        client_ids.append(args.db)

    if 'meta' in args:
        client_ids.append(args.meta)

    if 'bigquery' in client_ids:
        if 'gcp_project' in args:
            gcp_project = args.gcp_project
        elif 'scan_database' in args:
            gcp_project = args.scan_database
        else:
            raise ValueError('Unable to determine the GCP project to use for the client')

        big_query_client = BqClient(bq.Client(project=gcp_project))
    else:
        big_query_client = None

    return Clients(
        big_query=big_query_client,
    )


def build_db_backend(args: Namespace, clients: Clients) -> DbBackend:
    if args.db == 'memory':
        return MemoryDbBackend()
    elif args.db == 'bigquery':
        # TODO: allow flags to raise unsupported operations
        return BigQueryDbBackend(clients.big_query, raise_unsupported=set())
    else:
        raise ValueError(f'Invalid database backend: {args.db}')


def build_meta_backend(args: Namespace, clients: Clients) -> MetaBackend:
    if args.db == 'memory':
        return MemoryMetaBackend()
    elif args.db == 'bigquery':
        return BigQueryMetaBackend(clients.big_query, QualifiedName(args.meta_table_name))
    else:
        raise ValueError(f'Invalid metadata backend: {args.db}')


def migrate():
    args = parse_migrate_arguments()
    silent = args.wet and not args.verbose

    if silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.INFO)

    clients = build_clients(args)
    db_backend = build_db_backend(args, clients)
    meta_backend = build_meta_backend(args, clients)

    runner = MigrateRunner(context=Context(
        db_backend=db_backend,
        meta_backend=meta_backend,
        target_dir=args.target and Path(args.target),
        silent=silent,
        template_files=args.tpl and [Path(template) for template in args.tpl],
    ))

    runner.run(
        wet_run=args.wet,
        allow_down=args.down,
    )


def scan():
    args = parse_scan_arguments()
    clients = build_clients(args)
    db_backend = build_db_backend(args, clients)

    runner = ScanRunner(context=Context(
        db_backend=db_backend,
    ))

    runner.run(
        database=DatabaseName(args.scan_database),
        schema=SchemaName(args.scan_schema),
        table=Identifier(args.scan_table) if args.scan_table else None,
    )


def main():
    args = parse_all_arguments()

    if args.command == 'migrate':
        migrate()
    elif args.command == 'scan':
        scan()
    else:
        raise ValueError(f'Invalid command: {args.command}')
