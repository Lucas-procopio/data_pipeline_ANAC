from access_to_datalake import storageAccessObj

if __name__ == '__main__':
    requestObject = storageAccessObj('datalake_f')
    requestObject.downloadListFiles('ANAC', 'tarifas_transporte_aereo', '.csv', '2002')