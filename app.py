from access_to_datalake import storageAccessObj
from access_to_datawarehouse import bigqueryAccessObj
from etlRaw import localTransform
import shutil

SCHEMA = []

if __name__ == '__main__':
    extractingDatalake = storageAccessObj()
    # getting of objects in datalake
    objectsBucketList = extractingDatalake.bucketListObject('anac', 'tarifas_transporte_aereo', '2002', '.csv')
    print('\n\n', 'Objects List on Datalake:', '\n\n', objectsBucketList, '\n\n')
    # Download data in a temporary folder
    localpath = extractingDatalake.downloadListFiles('anac', 'tarifas_transporte_aereo', '2002', 'csv')
    
    # Creating local transform object
    localtranforming = localTransform(localpath, 'anac', 'tarifas_transporte_aereo', '2002', 'csv')
    print('Objects in a temporary local path:', '\n\n', localtranforming.gettingFullLocalPath(), '\n\n')

    # Creating a clean's dataset
    dataset = localtranforming.createDataset()
    print(dataset)
    # Dropping unusable data
    shutil.rmtree(localpath[:localpath.find('/', 2)])

    # Accessing DatawaHouse
    conectingDatawarehouse = bigqueryAccessObj()
    
    # Updating dataset on datawareHouse
    conectingDatawarehouse.updatetable('table_id', SCHEMA)