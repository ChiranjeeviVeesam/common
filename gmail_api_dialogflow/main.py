from extract_manager import ExtractManager 
from load_manager import LoadManager
import base64
import json 
import os 

def main(event, context):
  try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='editorkey.json'
    os.environ["GOOGLE_CLOUD_PROJECT"]='peak-emitter-377300'
    data_str = base64.b64decode(event['data']).decode('utf-8')
    data_dict = json.loads(data_str)
    latest_history_id = data_dict['historyId']

    extract_manager_client = ExtractManager()
    load_manager_client = LoadManager()  

    previous_history_id = extract_manager_client.get_history_id()    
    load_manager_client.save_history_id(latest_history_id)
    
    if previous_history_id:
        total_data =  extract_manager_client.get_gmail_data(previous_history_id)
        load_manager_client.publish_data(total_data)       
                            
    else:
        print('no history')
    
  except Exception as e:
    print('failed at main function', e)

    
    
