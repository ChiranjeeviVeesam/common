import argparse 
from google.cloud import pubsub_v1
from datetime import datetime
from scipy import stats
import time 
import os 

def run(project_id, topic_name, INTERVAL=200):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    sensorNames = ['Pressure_1','Pressure_2','Pressure_3','Pressure_4','Pressure_5']
    sensorCenterLines = [1992,2080,2390,1732,1911]
    standardDeviation = [442,388,354,403,366]
    c = 0

    while(True):

        for pos in range(0, 5):
            sensor = sensorNames[pos]
            reading =  stats.truncnorm.rvs(-1,1,loc = sensorCenterLines[pos], scale = standardDeviation[pos])
            timeStamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            message = timeStamp + ',' + sensor + ','+ str(reading)
            publisher.publish(topic_path, data = message.encode('utf-8'))
            c=c+1
            time.sleep(INTERVAL/1000)
            if c == 100:
                print('published 100 messages')
                c=0
 
 

if __name__ == '__main__':

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_id',
        help='GCP project ID'
    )
    parser.add_argument(
        '--topic_name',
        help='GCP topic name'
    )
    
    args = parser.parse_args()
    try:
        run(args.project_id,
            args.topic_name)
    except Exception as e:
        print(e)
