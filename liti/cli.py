from google.cloud import bigquery
from google.cloud.bigquery.enums import JobCreationMode

from liti.core.backend.bigquery import BigQueryDbBackend


def main():
    client = bigquery.Client(
        default_job_creation_mode=JobCreationMode.JOB_CREATION_OPTIONAL,
    )

    db_backend = BigQueryDbBackend(client)

    print(1)
