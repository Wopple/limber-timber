from argparse import ArgumentParser, BooleanOptionalAction, Namespace

from google.cloud import bigquery

from liti.core.backend.bigquery import BigQueryDbBackend, BigQueryMetaBackend
from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
from liti.core.client.bigquery import BqClient
from liti.core.model.v1.schema import TableName
from liti.core.runner import MigrateRunner


def parse_arguments() -> Namespace:
    parser = ArgumentParser(prog='Limber Timber')
    parser.add_argument('command', help='action to perform')
    parser.add_argument('-t', '--target', required=True, help='directory with migration files')
    parser.add_argument('-w', '--wet', action=BooleanOptionalAction, default=False, help='[False] should perform migration side effects')
    parser.add_argument('-d', '--down', action=BooleanOptionalAction, default=False, help='[False] should allow performing down migrations')
    parser.add_argument('--db', default='memory', help='[memory] type of database backend (e.g. memory, bigquery)')
    parser.add_argument('--meta', default='memory', help='[memory] type of metadata backend (e.g. memory, bigquery)')
    parser.add_argument('--meta-table-name', help='fully qualified table name for a metadata table')
    parser.add_argument('--gcp-project', required=False, help='project to use for GCP backends')
    return parser.parse_args()


def main():
    args = parse_arguments()

    if 'bigquery' in (args.db, args.meta):
        big_query_client = BqClient(bigquery.Client(project=args.gcp_project))
    else:
        big_query_client = None

    match args.db:
        case 'memory':
            db_backend = MemoryDbBackend()
        case 'bigquery':
            db_backend = BigQueryDbBackend(big_query_client)
        case _:
            raise ValueError(f'Invalid database backend: {args.db}')

    match args.meta:
        case 'memory':
            meta_backed = MemoryMetaBackend()
        case 'bigquery':
            meta_backed = BigQueryMetaBackend(big_query_client, TableName(args.meta_table_name))
        case _:
            raise ValueError(f'Invalid database backend: {args.db}')

    match args.command:
        case 'migrate':
            runner = MigrateRunner(
                db_backend=db_backend,
                meta_backend=meta_backed,
                target=args.target,
            )

            runner.run(
                wet_run=args.wet,
                allow_down=args.down,
            )
        case _:
            raise ValueError(f'Invalid command: {args.command}')
