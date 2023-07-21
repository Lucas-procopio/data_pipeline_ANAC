from google.cloud import bigquery
import os
from time import sleep

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

    def __loaddataontable(self, dataframe, table_id, schema):
        job_config = bigquery.LoadJobConfig(schema=schema)
        job = self.bigquery_client.load_table_from_dataframe(dataframe, table_id, job_config) # making api request
        job.result() # getting job results

        table = self.bigquery_client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} to {table_id}")

    def updatetable(self, dataframe, table_id, schema):
        try:
            print(f'Starting loading data at {table_id}.')
            self.__loaddataontable()
            print(f'Data was loaded successfully at {table_id}.')
        except:
            try:
                print(f"Table {table_id} didn't exist. It'll create first.")
                print(f'Starting creating {table_id}.')
                self._creatingtable(table_id, dataframe)
                print(f'Table {table_id} was created.')
                sleep(5)
                print(f'Starting loading data at {table_id} again.')
                self.__loaddataontable(dataframe, table_id, schema)
                print(f'Data was loaded successfully at {table_id}.')
            except:
                print('This something wrong with parameters passed.')
