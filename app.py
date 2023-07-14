from access_to_datalake import storageAccessObj
from etlRaw import localTransform
import os

if __name__ == '__main__':
    extractingDatalake = storageAccessObj()
    # getting of objects in datalake
    objectsBucketList = extractingDatalake.bucketListObject('anac', 'tarifas_transporte_aereo', '2002', '.csv')
    print(objectsBucketList)
    # Download data in a temporary folder
    extractingDatalake.downloadListFiles('anac', 'tarifas_transporte_aereo', '.csv', '2002')
    
    localpath = localTransform('./anac/tarifas_transporte_aereo/2002', 'anac', 'tarifas_transporte_aereo', '2002', 'csv')
    print(localpath.gettingFullLocalPath())