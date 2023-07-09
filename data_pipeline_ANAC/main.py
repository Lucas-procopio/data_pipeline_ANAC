from google.cloud import storage
import os

class storageAccessObj():
    ''' It's was created for instance a client storage, defining a bucket and get list of blobs. Then, filter blobs and files'''

    @staticmethod
    def _startingClient(path:str):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path #
        return storage.Client()

    def __init__(self, bucket:str, storage_client=_startingClient('./data_pipeline_ANAC/credentials/gcp_credentials.json')):
        self.bucket = bucket
        self.storage_client = storage_client

    def __bucketObject(self):
        # Getting object called bucket
        return self.storage_client.get_bucket(self.bucket)

    def bucketListObject(self, origem:str, filename:str, format:str):
        # Getting blobs using some filters like parameters
        list_blobs = self.__bucketObject()
        return [blob.name for blob in list_blobs.list_blobs() if origem.lower() in blob.name 
                and filename.lower() in blob.name and (format.upper() in blob.name or format.lower() in blob.name)]
    
if __name__ == '__main__':
    
    objectA = storageAccessObj('datalake_f')
    objectB = storageAccessObj._startingClient('./data_pipeline_ANAC/credentials/gcp_credentials.json')
    print(objectA.bucketListObject('anac', 'tarifas_transporte_aereo', '.CSV'))