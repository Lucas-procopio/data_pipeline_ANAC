import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

serviceAccount = r'WAY_TO_GCP_CREDENTIALS'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serviceAccount

pipeline_options = {
    'runner': 'DataflowRunner', # Setting a runnerDataflowRunner
    'project': 'PROJECT_ID', # Project_id
    'job_name': 'testing', # job's name
    'staging_location': 'gs://STORAGE/temp', # Where would be staging area of pipeline
    'temp_location': 'gs://STORAGE/temp', # where would be temp location 
    'region': 'us-central1', # datacenter of dataflow
    'template_location': 'gs://STORAGE/template/batch_job_voos', # Where would be save a template that was created in proccess
    'save_main_session': True # parâmetro para salvar tudo que ele declarou na main session (permite que os workers identifiquem e achem) 
    }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]
        
def criar_dict_nivel1(record):
    dict_ = {}
    dict_['airport'] = record[0]
    dict_['lista'] = record[1]
    return (dict_)

def desanimar_dict(record):
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '_' + k, v) for k,v in desanimar_dict(value).items() ]
        else:
            return [ (key, value) ]
    items = [ item for k, v in record.items() for item in expand(k, v)]
    return dict(items)

def criar_dict_nivel10(record):
    dict_ = {}
    dict_['airport'] = record['airport']
    dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]
    dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]
    return (dict_)

table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela = 'project_id:dataset_id.table_id'

tempo_Atrasos = (
    p1
    | "Import Late's Data" >> beam.io.ReadFromText(r'gs://storage/entrada/voos_sample.csv', skip_header_lines = 1)
    | "Dividing string for comma" >> beam.Map(lambda record: record.split(','))
    | "Getting lates flight data" >> beam.ParDo(filtro())
    | "Making for late" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Sum for key" >> beam.CombinePerKey(sum)
)

qtd_Atrasos = (
    p1
    | "Import Data" >> beam.io.ReadFromText(r'gs://storage/entrada/voos_sample.csv', skip_header_lines= 1)
    | "Dividing string for amount" >> beam.Map(lambda record:record.split(','))
    | "Getting flight for amount" >> beam.ParDo(filtro())
    | "Making for amount" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Counting for key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos': qtd_Atrasos, 'Tempo_Atrasos': tempo_Atrasos}
    | "Group By" >> beam.CoGroupByKey()
    | "ETL nivel 1" >> beam.Map(lambda record: criar_dict_nivel1(record))
    | "ETL dict" >>  beam.Map(lambda record: desanimar_dict(record))
    | "ETL nivel 10" >> beam.Map(lambda record: criar_dict_nivel10(record))
    | "creating table" >> beam.io.WriteToBigQuery(
                    tabela,
                    schema=table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Append data if table exist
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, # Create table if doesn't exist
                    custom_gcs_temp_location='gs://storage/temp')
)
p1.run()
