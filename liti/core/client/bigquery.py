from google.cloud.bigquery import Client, ConnectionProperty, QueryJob, QueryJobConfig
from google.cloud.bigquery.table import RowIterator, Table


class BqClient:
    """ Can be used as a context manager to run queries within a transaction """

    def __init__(self, client: Client):
        self.client = client
        self.session_id: str | None = None

    def __enter__(self):
        if self.session_id is not None:
            raise RuntimeError('Big Query does not support nested transactions')

        job = self.client.query('BEGIN TRANSACTION', job_config=QueryJobConfig(create_session=True))
        job.result()
        self.session_id = job.session_info.session_id

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            job = self.query('COMMIT TRANSACTION', job_config=QueryJobConfig(session_id=self.session_id))
        else:
            job = self.query('ROLLBACK TRANSACTION', job_config=QueryJobConfig(session_id=self.session_id))

        job.result()

    def setup_config(self, job_config: QueryJobConfig | None) -> QueryJobConfig:
        if self.session_id is not None:
            job_config = job_config or QueryJobConfig()
            job_config.connection_properties = [ConnectionProperty('session_id', self.session_id)]

        return job_config

    def query(self, sql: str, job_config: QueryJobConfig | None = None) -> QueryJob:
        job_config = self.setup_config(job_config)
        return self.client.query(sql, job_config=job_config)

    def query_and_wait(self, sql: str, job_config: QueryJobConfig | None = None) -> RowIterator:
        job_config = self.setup_config(job_config)
        return self.client.query_and_wait(sql, job_config=job_config)

    def get_table(self, table_name: str) -> Table:
        return self.client.get_table(table_name)

    def create_table(self, bq_table: Table):
        self.client.create_table(bq_table)

    def delete_table(self, table_name: str):
        self.client.delete_table(table_name)
