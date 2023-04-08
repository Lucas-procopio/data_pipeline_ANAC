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
    'template_location': 'gs://STORAGE/temp/template/batch_job_voos' # Where would be save a template that was created in proccess
    }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

class filtro(beam.DoFn):

    def process(self, record):
        if int(record[8]) > 0:
            return [record]
        
tempo_Atrasos = (
    p1
    | "Import Late's Data" >> beam.io.ReadFromText(r'gs://storage/entrada/voos_sample.csv', skip_header_lines = 1)
    | "Dividing string for comma" >> beam.Map(lambda record: record.split(','))
    | "Getting lates flight data" >> beam.ParDo(filtro())
    | "Making for late" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Sum for key" >> beam.CombinePerKey(sum)
#   | "Show results" >> beam.Map(print)
)

qtd_Atrasos = (
    p1
    | "Import Data" >> beam.io.ReadFromText(r'gs://storage/entrada/voos_sample.csv', skip_header_lines= 1)
    | "Dividing string for amount" >> beam.Map(lambda record:record.split(','))
    | "Getting flight for amount" >> beam.ParDo(filtro())
    | "Making for amount" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Counting for key" >> beam.combiners.Count.PerKey()
#   | "Show results amount" >> beam.Map(print)
)

tabela_atrasos = (
    {'amount_lates': qtd_Atrasos, 'time_lates': tempo_Atrasos}
    | "Group By" >> beam.CoGroupByKey()
#    | beam.Map(print)
    | "Pushing data to Cloud Storage" >> beam.io.WriteToText(r'gs://storage/saida/voos_sample.csv')
)

p1.run()