from access_to_datalake import storageAccessObj
from access_to_datawarehouse import bigqueryAccessObj
from etlRaw import localTransform
import shutil
from google.cloud import bigquery

SCHEMA = [bigquery.SchemaField('ano', bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField('mes', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('empresa', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('origem', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('destino', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('tarifa', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('assentos', bigquery.enums.SqlTypeNames.STRING)
            ]


table_id = ''

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
    dataset.to_excel('dataset.xlsx')
    # Updating dataset on datawareHouse
    #conectingDatawarehouse.updatetable(dataset, table_id, SCHEMA)