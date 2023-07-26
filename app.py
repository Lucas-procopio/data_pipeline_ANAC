from access_to_datalake import storageAccessObj
from access_to_datawarehouse import bigqueryAccessObj
from etlRaw import localTransform
import shutil
from google.cloud import bigquery
import argparse
import os

def orchestration(table_id, datalake, credentials_path, schema):
    # Acessing datalake
    extractingDatalake = storageAccessObj(datalake, storageAccessObj._startingClient(credentials_path))
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
    conectingDatawarehouse = bigqueryAccessObj(bigqueryAccessObj._startingClient(credentials_path))

    # Updating dataset on datawareHouse
    conectingDatawarehouse.updatetable(dataset, table_id, schema)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='pipeline_anac',
        description='This a pipeline defition that made a api request at google storage (datalake). Then, doing ETL of data. Finally, insert a data after ETL on a table in bigquery (datawarehouse)'
    )

    parser.add_argument('credentials_path', type=str, help='path with credentals to access gcp')
    parser.add_argument('datalake', type=str, help='datalakes name in a bucket')
    parser.add_argument('table_id', type=str, help='table_ids name at bigquery')
    args = parser.parse_args()

    SCHEMA = [bigquery.SchemaField('ano', bigquery.enums.SqlTypeNames.INT64), 
            bigquery.SchemaField('mes', bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField('empresa', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('origem', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('destino', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('tarifa', bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField('assentos', bigquery.enums.SqlTypeNames.INT64)
            ]

    orchestration(args.table_id, args.datalake, args.credentials_path, SCHEMA)