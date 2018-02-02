import apache_beam as beam
import re

PROJECT='dataflowqs'
BUCKET='simplebucket1_dftest'

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=simplejob1',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)
   inputfile = 'gs://simplebucket1_dftest/landing/simpletext.txt'
   output_prefix = 'gs://simplebucket1_dftest/output/opf'

   lines = p | 'Read Text File' >> beam.io.ReadFromText(inputfile)

   #lines | 'Write Text File' >> beam.io.WriteToText(output_prefix,num_shards=1)

   newlines = lines | 'Process File' >> beam.Map(lambda line: (line + 'appString'))

   newlines | 'Write Processed File' >> beam.io.WriteToText(output_prefix,num_shards=1)

   p.run()

if __name__ == '__main__':
   run()
