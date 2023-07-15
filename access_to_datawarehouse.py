from google.cloud import bigquery
import os

CREDENTIALS_PATH = './credentials/gcp_credentials.json'

class bigqueryAccessObj():
    ''' Creating a client bigquery instance.'''

    @staticmethod
    def startingClient(path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        return bigquery.Client()

    def __init__(self, bigquery_client=startingClient(CREDENTIALS_PATH)):
        self.bigquery_client = bigquery_client

    def _creatingtable(self, table_id, schema):
        table = bigquery.Table(table_id, schema=schema)
        table = self.bigquery_client.create_table(table)
        print(f'Created table {table.project}.{table.dataset_id}.{table.table_id}')