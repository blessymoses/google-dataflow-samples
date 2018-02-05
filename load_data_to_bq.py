import apache_beam as beam
import re

PROJECT='dataflowqs'
BUCKET='simplebucket1_dftest'

class convertToDictFn(beam.DoFn):
   def process(self, element):
      value_dict = dict()
      fields = element.split(",")
      value_dict['name'] = fields[0]
      value_dict['gender'] = fields[1]
      value_dict['occurences'] = fields[2]
      yield value_dict

class convertToCsvFn(beam.DoFn):
   def process(self, element):
      yield "|".join(element.itervalues())

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=loadtobigquery',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)
   inputfile = 'gs://simplebucket1_dftest/landing/bqdata.csv'

   lines = p | 'Read Text File' >> beam.io.ReadFromText(inputfile)

   #convert data to dict
   newlines = lines | 'Convert to Dict' >> beam.ParDo(convertToDictFn())
   #write data to bigquery
   #newlines | 'Write To BigQuery' >> beam.io.WriteToBigQuery('namesdata','userdata',PROJECT,schema=None,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   newlines | 'Write To BigQuery' >> beam.io.WriteToBigQuery('namesdata','userdata',PROJECT,
      'name:STRING,gender:STRING,occurences:INTEGER',
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   p.run()

if __name__ == '__main__':
   run()
