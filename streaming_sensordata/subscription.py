import os 
from google.cloud import pubsub_v1
import argparse

def callback(message):
    print(f"Received {message}.")
    message.ack()
    
def run(PROJECT_ID, SUBSCRIPTION_NAME):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback = callback)
    print(f"Listening for messages on {subscription_path}..\n")
    try:
        streaming_pull_future.result()
    except:
        streaming_pull_future.cancel()
        
if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', help='PROJECT ID')
    parser.add_argument('--subscription_name', help='SUBSCRIPTION')
    args = parser.parse_args()
    run(args.project_id, args.subscription_name)
