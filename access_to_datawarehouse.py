from google.cloud import bigquery
import os

CREDENTIALS_PATH = './credentials/gcp_credentials.json'

class bigqueryAcessObj():
    ''' Creating a client bigquery instance.'''

    @staticmethod
    def startingClient(path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        return bigquery.client()

    def __init__(self, bigquery_client=startingClient(CREDENTIALS_PATH)):
        self.bigquery_client = bigquery_client
