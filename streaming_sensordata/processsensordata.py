import os 
import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
import argparse 
from datetime import datetime, timedelta
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows   
import pandas as pd
import json 

class InterpolateSensors(beam.DoFn):
    def  process(self, sensorValues):
        (timestamp, values) = sensorValues
        df = pd.DataFrame(values)
        df.columns = ["Sensor","Value"]
        json_str =  json.loads(df.groupby(["Sensor"]).mean().T.iloc[0].to_json())
        json_str['timestamp'] = timestamp
        return [json_str]
        
def roundTime(dt, roundTo=1):
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds+roundTo/2)
    return str(dt + timedelta(0,rounding-seconds,-dt.microsecond))

def isMissing(jsonData):
    return len(jsonData.values()) == 6

def run(args, pipeline_args):
    schema = schema = 'Timestamp:TIMESTAMP, PRESSURE_1:FLOAT, PRESSURE_2:FLOAT, PRESSURE_3:FLOAT, PRESSURE_4:FLOAT, PRESSURE_5:FLOAT'
    with beam.Pipeline(options = PipelineOptions(pipeline_args, streaming = True, save_main_session = True)) as p:
        data = ( p 
                | 'Read Data' >> ReadFromPubSub(subscription=args.subscription_name)
                | 'Decode data' >> beam.Map(lambda x: x.decode('utf-8'))
                | 'Convert to List' >> beam.Map(lambda x: x.split(','))
                | 'to tuple' >> beam.Map(lambda x: (roundTime(datetime.strptime(x[0],'%Y-%m-%d %H:%M:%S.%f'), roundTo = 1.0), [x[1], float(x[2])]))
        )
        bq = ( data
              | 'Windowing' >> beam.WindowInto(FixedWindows(15))
              | 'Group by' >> beam.GroupByKey()
              | 'Interpolate' >> beam.ParDo(InterpolateSensors())
              | "Filter Missing" >> beam.Filter(isMissing)
            #   | 'd' >> beam.Map(print)
              | 'Write To BigQuery' >> WriteToBigQuery(args.bq_table, schema =schema)
        ) 
        
        


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--subscription_name', 
        help = 'SUBSCRIPTION NAME'
    )
    parser.add_argument(
        '--bq_table',
        help = 'BIGQUERY TABLE',
        default = 'tonal-land-386509.SensorReadings.RawReadings'
    )
    args, pipeline_args = parser.parse_known_args()
    print('ok')
    run(args, pipeline_args)
