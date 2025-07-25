import logging
from typing import Iterable

from liti import bigquery as bq

log = logging.getLogger(__name__)


class BqClient:
    """ Big Query client that lives in terms of google.cloud.bigquery

    Can be used as a context manager to run queries within a transaction.
    """

    def __init__(self, client: bq.Client):
        self.client = client
        self.session_id: str | None = None

    def __enter__(self):
        if self.session_id is not None:
            raise RuntimeError('Big Query does not support nested transactions')

        job = self.client.query('BEGIN TRANSACTION', job_config=bq.QueryJobConfig(create_session=True))
        job.result()
        self.session_id = job.session_info.session_id

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            job = self.query('COMMIT TRANSACTION', job_config=bq.QueryJobConfig(session_id=self.session_id))
        else:
            job = self.query('ROLLBACK TRANSACTION', job_config=bq.QueryJobConfig(session_id=self.session_id))

        job.result()

    def setup_config(self, job_config: bq.QueryJobConfig | None) -> bq.QueryJobConfig:
        if self.session_id is not None:
            job_config = job_config or bq.QueryJobConfig()
            job_config.connection_properties = [bq.ConnectionProperty('session_id', self.session_id)]

        return job_config

    def query(self, sql: str, job_config: bq.QueryJobConfig | None = None) -> bq.QueryJob:
        log.info(f'query:\n{sql.strip()}')
        job_config = self.setup_config(job_config)
        return self.client.query(sql, job_config=job_config)

    def query_and_wait(self, sql: str, job_config: bq.QueryJobConfig | None = None) -> bq.RowIterator:
        log.info(f'query_and_wait:\n{sql.strip()}')
        job_config = self.setup_config(job_config)
        return self.client.query_and_wait(sql, job_config=job_config)

    def has_table(self, table_ref: bq.TableReference) -> bool:
        for table_item in self.client.list_tables(f'{table_ref.project}.{table_ref.dataset_id}'):
            if table_item.table_id == table_ref.table_id:
                return True

        return False

    def get_table(self, table_ref: bq.TableReference) -> bq.Table:
        return self.client.get_table(table_ref)

    def list_tables(self, dataset_ref: bq.DatasetReference) -> Iterable[bq.TableListItem]:
        return self.client.list_tables(dataset_ref)

    def create_table(self, bq_table: bq.Table):
        self.client.create_table(bq_table)

    def delete_table(self, table_ref: bq.TableReference):
        self.client.delete_table(table_ref)

    def update_table(self, table: bq.Table, fields: list[str]) -> bq.Table:
        return self.client.update_table(table, fields)
