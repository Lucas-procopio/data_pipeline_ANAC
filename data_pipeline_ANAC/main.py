from google.cloud import storage
import os

class storageAccessObj():
    ''' It's was created for instance a client storage, defining a bucket and get list of blobs. Then, filter blobs and files'''

    CREDENTIALS_PATH = './credentials/gcp_credentials.json'

    @staticmethod
    def _startingClient(path:str):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path #
        return storage.Client()

    def __init__(self, bucket:str, storage_client=_startingClient(CREDENTIALS_PATH)):
        self.bucket = bucket
        self.storage_client = storage_client

    def _bucketObject(self):
        # Getting object called bucket
        return self.storage_client.get_bucket(self.bucket)

    def bucketListObject(self, fromDatabase:str, filename:str, formatFile:str):
        # Getting blobs using some filters like parameters
        list_blobs = self._bucketObject()
        return [blob.name for blob in list_blobs.list_blobs() if fromDatabase.lower() in blob.name 
                and filename.lower() in blob.name and (formatFile.upper() in blob.name or formatFile.lower() in blob.name)]

    def downloadFileStorage(self, storage_path, local_path):
        bucket = self._bucketObject()
        blob = bucket.blob(storage_path)
        return blob.download_to_filename(f'{local_path}')

    def downloadListFiles(self, fromDatabase:str, filename:str, formatFile:str):
        filesDownloadList = self.bucketListObject(fromDatabase, filename, formatFile)
        for filePath in filesDownloadList:
            paths = filePath.rsplit('/', 1)
            ifExist = os.path.exists(paths[0])
            if ifExist is False:
                os.makedirs(paths[0])
            self.downloadFileStorage(filePath, os.path.join('./',filePath))
        

if __name__ == '__main__':
    
    objectA = storageAccessObj('datalake_f')
    objectA.downloadListFiles('anac', 'tarifas_transporte_aereo', '.csv')
    